{
    mysql-replication-relay
    Copyright (C) 2023 Freedelity

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
}
unit ClassReplicationRelay;

{$IFDEF FPC}
  {$MODE Delphi}
{$ENDIF}

{$UNDEF REPDEBUG}
interface 
 
uses
  ReplicationRelayStructures,Declarations, ClassModule, ClassMysqlReplicationClient, MySQL, ClassMySQL, Dateutils, Classes, SysUtils, ClassDebug, IdTCPClient, IdIOHandler, IdHashSHA1, IdGlobal, StrUtils,ClassServer,ClassServerHandler,Contnrs,HFNV1A,IdHTTP,IdCustomHTTPServer,IdHTTPServer,IdSocketHandle,IdContext, ClassPCQueue;
type

  TFilter = object
    FilterType: Byte;
    FilterDiscardType: Byte;
    FilterQueueLimit: Integer;    
    SchemaName: String;    
    TableName: String;    
  end;

  TRelayClient = record 
    Id: DWord;
    Name: String;
    Token: DWord; 
    Client: TServerHandler;
    ConnectionStatus: Integer;
    Queue: TPCQueue;    
    NumFilter: Integer;
    MaxFilter: Integer;    
    Filters: Array of TFilter;
    
    UnQueuedLog: Array of TQueueItem;
    UnQueuedLogTick: Array of Int64;
    UnQueuedLogCount: Integer;
    UnQueuedIdx: Integer;
  
    // Some Stats
    Started: TDateTime;
    LastSeen: TDateTime;
    Served: Integer;
    Discarded: Integer;
    MaxQueue: Integer;    
  end;
  
  TTableStat = object
    Name: String;
    NumInsert: Integer;
    NumUpdate: Integer;
    NumDelete: Integer;
    
    HistInsert: Array[0..86400] of Integer;
    HistUpdate: Array[0..86400] of Integer;
    HistDelete: Array[0..86400] of Integer;
  end;
  
  TReplicationRelay = class(TThread)
  protected
    FReplicator: TMysqlReplicationClient;
    FServer: TServer;
    FHTTPServer: TIdHTTPServer;    
    
    FCriticalSection: TRTLCriticalSection;
    
    FNumClient: Integer;
    FMaxClient: Integer;
    
    FNumServed: Integer;
    FNumEvent: Integer;
    FNumEventType: Array of Integer;
    
    FTableStats: TFPHashList;
    
    FAdminHash: QWord;
    FAuthHash: QWord;
    FSport: Integer;
    FHport: Integer;
    FHAddress: String;
    
    FHour,FMin,FSec: Integer;
    
    FYear,FMonth,FDay: Word;
    
    FLogSchema: String;
    FLogPath: String;
    FLogExcludes: TFPHashList;
    
    FStarted: TDateTime;

    
  public
    FClients: Array of TRelayClient; 
    
    constructor Create(CreateSuspended: Boolean);
    destructor Destroy; override;
    
    procedure Execute; override; 

    function Init(Host,Login,Password,DB: String;AdminSecret: String;AuthSecret: String;SPort: Integer; HAddress: String; HPort: Integer; HLogPath: String; HLogSchema: String; HLogExcludes: String): Integer;
    function Run(): Integer;
    
    procedure EnqueueEvent(AEventType: Byte; AEvent: TBinLogEvent);    
    procedure LogEvent(AEventType: Byte; AEvent: TBinLogEvent);    
    function PollEvent(AClientID: DWord): TQueueItem;
    function PollEventV2(AClientID: DWord): TQueueItem;
    
    function RegisterClient(AHandler: TServerHandler;AClientName: String;AAuth: QWord; var AToken: DWord): Integer;
    function AddFilter(AClientID: DWord; AFilterType: Byte; ASchemaName,ATableName: String; AFilterDiscardType: Byte; AQueueLimit: Integer): Integer;
    
    procedure HTTPRequest(AContext: TIdContext; ARequestInfo: TIdHTTPRequestInfo; AResponseInfo: TIdHTTPResponseInfo);        
  published
  end;   
implementation


{******************************************************************************}
procedure TReplicationRelay.HTTPRequest(AContext: TIdContext; ARequestInfo: TIdHTTPRequestInfo; AResponseInfo: TIdHTTPResponseInfo);
var 
  i,j,k,h,hidx: Integer;
  lCountI,lCountU,lCountD: Integer;  
  Buf: String;
  lPtr: ^TTableStat;
  lHour,lMin,lSec,lMil: Word;
  lNow: TDateTime;
  lDaySec: Integer;
  tick: Int64;

  procedure TableStatItemProc(Data: Pointer; Arg: Pointer);
  begin
    try
      AddToLog(llWarning,'TableStatItem');
      if(Data=nil) then AddToLog(llWarning,'Data is Nil')      
      else AddToLog(llWarning,'Data is not Nil');
      if(Arg=nil) then AddToLog(llWarning,'Arg is Nil')
      else AddToLog(llWarning,'Arg is not Nil');      
      
      lPtr:=Data;
      AddToLog(llWarning,(lPtr^).Name);
      AddToLog(llWarning,'<tr><td>'+(lPtr^).Name+'</td><td>'+IntToStr((lPtr^).NumInsert)+'</td><td>'+IntToStr((lPtr^).NumUpdate)+'</td><<td>'+IntToStr((lPtr^).NumDelete)+'</td></tr>');
      Buf:=Buf+'<tr><td>'+(lPtr^).Name+'</td><td>'+IntToStr((lPtr^).NumInsert)+'</td><td>'+IntToStr((lPtr^).NumUpdate)+'</td><<td>'+IntToStr((lPtr^).NumDelete)+'</td></tr>';    
    except
      on E:Exception do 
      begin
        AddToLog(llWarning,'Exception : '+E.Message);
      end;
    end;  
  end;

begin

  AddToLog(llWarning,'HTTP Request');
  //EnterCriticalSection(FCriticalSection);

  lNow:=Now();
  DecodeTime(lNow,lHour,lMin,lSec,lMil);
  lDaySec:=lHour*3600+lMin*60+lSec;
  
  Buf:='<html><head><title>Replicate Relay Status</title>';
  //Buf:=Buf+'<link rel="stylesheet" href="https://fonts.googleapis.com/css?family=VT323">';
  Buf:=Buf+'<link rel="stylesheet" href="https://fonts.googleapis.com/css?family=Share+Tech+Mono">';
  Buf:=Buf+'<style> html { font-family: "Share Tech Mono","VT323"; font-size: 20px; background: #000000; color: #a4f644;} h2 { font-size: 24px; font-weight: bold; } table { border: 2x solid #a4f644; font-size: 20px;} th { font-weight: bold; text-align: center; border: 1px solid #a4f644; padding: 4px;} td { border: 1px solid #a4f644; text-align: right; padding: 4px;} </style>';
  Buf:=Buf+'</head><body>';
  Buf:=Buf+'<h2>SERVER</h2>';

  Buf:=Buf+'<table cellspacing=0>';
  
  Buf:=Buf+'<tr><th>Info</th><th>Value</th></tr>';
  Buf:=Buf+'<tr><td>Started</td><td>'+DateTimeToStr(FStarted)+'</td></tr>';

  Buf:=Buf+'<tr><td>Protocol Version</td><td>'+IntToHex(REPRELAY_VERSION,8)+'</td></tr>';
  Buf:=Buf+'<tr><td>Number of Clients</td><td>'+IntToStr(FNumClient-1)+'</td></tr>';
  Buf:=Buf+'<tr><td>Current Binlog File</td><td>'+FReplicator.FBinLogFile+'</td></tr>';
  Buf:=Buf+'<tr><td>Current Binlog Position</td><td>'+IntToStr(FReplicator.FBinLogOffset)+'</td></tr>';
  Buf:=Buf+'<tr><td>Events Received</td><td>'+IntToStr(FNumEvent)+'</td></tr>';
  for i:=0 to 255 do
  begin
    if(i=$02) then Buf:=Buf+'<tr><td>QUERY_EVENT</td><td>'+IntToStr(FNumEventType[i])+'</td></tr>'
    else if(i=$04) then Buf:=Buf+'<tr><td>ROTATE_EVENT</td><td>'+IntToStr(FNumEventType[i])+'</td></tr>'
    else if(i=$0F) then Buf:=Buf+'<tr><td>FORMAT_DESCRIPTION_EVENT</td><td>'+IntToStr(FNumEventType[i])+'</td></tr>'
    else if(i=$10) then Buf:=Buf+'<tr><td>XID_EVENT</td><td>'+IntToStr(FNumEventType[i])+'</td></tr>'
    else if(i=$13) then Buf:=Buf+'<tr><td>TABLE_MAP_EVENT</td><td>'+IntToStr(FNumEventType[i])+'</td></tr>'
    else if(i=$17) then Buf:=Buf+'<tr><td>WRITE_ROW_EVENT_V1</td><td>'+IntToStr(FNumEventType[i])+'</td></tr>'
    else if(i=$18) then Buf:=Buf+'<tr><td>UPDATE_ROW_EVENT_V1</td><td>'+IntToStr(FNumEventType[i])+'</td></tr>'
    else if(i=$19) then Buf:=Buf+'<tr><td>DELETE_ROW_EVENT_V1</td><td>'+IntToStr(FNumEventType[i])+'</td></tr>'
    else if(FNumEventType[i]>0) then Buf:=Buf+'<tr><td>Event #'+IntToHex(i,2)+'</td><td>'+IntToStr(FNumEventType[i])+'</td></tr>';
  end;
  Buf:=Buf+'<tr><td>Events Served</td><td>'+IntToStr(FNumServed)+'</td></tr>';

  Buf:=Buf+'</table>';


  Buf:=Buf+'<br>';
  if(FNumClient>1) then
  begin
     Buf:=Buf+'<h2>CLIENTS</h2><br>';
     Buf:=Buf+'<table cellspacing=0>';
     Buf:=Buf+'<tr><th>ID</th><th>Name</th><th>Status</th><th>Started</th><th>Last Seen</th><th>Inactive</th><th>Filter</th><th>Queue</th><th>Max Queue</th><th>Served</th><th>Discarded</th><th>History</th></tr>';
        
      for i:=1 to FNumClient-1 do
      begin
        Buf:=Buf+'<tr>';
        Buf:=Buf+'<td>'+IntToStr(i)+'</td>';
        Buf:=Buf+'<td>'+FClients[i].Name+'</td>';
        
        if(FClients[i].ConnectionStatus=1) then Buf:=Buf+'<td>CONNECTED</td>'
        else if (SecondsBetween(FClients[i].LastSeen,Now)<2) then Buf:=Buf+'<td>POLLING</td>'
        else Buf:=Buf+'<td>DISCONNECTED</td>';
 
        Buf:=Buf+'<td>'+DateTimeToStr(FClients[i].Started)+'</td>';
        Buf:=Buf+'<td>'+DateTimeToStr(FClients[i].LastSeen)+'</td>';
        Buf:=Buf+'<td>'+IntToStr(SecondsBetween(FClients[i].LastSeen,Now))+'</td>';
        
        Buf:=Buf+'<td style="text-align: left;">';
        if(FClients[i].NumFilter>0) then
        begin
          for j:=0 to FClients[i].NumFilter-1 do
          begin
            
            Buf:=Buf+FClients[i].Filters[j].SchemaName+'.'+FClients[i].Filters[j].TableName+' (';
            if((FClients[i].Filters[j].FilterType AND REPRELAYREQ_INSERT)>0) then Buf:=Buf+'I';
            if((FClients[i].Filters[j].FilterType AND REPRELAYREQ_UPDATE)>0) then Buf:=Buf+'U';
            if((FClients[i].Filters[j].FilterType AND REPRELAYREQ_DELETE)>0) then Buf:=Buf+'D';
            Buf:=Buf+')<br>';
            
          end;
        end;
        Buf:=Buf+'</td>';
        
        Buf:=Buf+'<td>'+IntToStr(FClients[i].Queue.Count)+'</td>';
        Buf:=Buf+'<td>'+IntToStr(FClients[i].MaxQueue)+'</td>';
        Buf:=Buf+'<td>'+IntToStr(FClients[i].Served)+'</td>';
        Buf:=Buf+'<td>'+IntToStr(FClients[i].Discarded)+'</td>';
        
        // 20230612
        tick:=GetTickCount64;
        Buf:=Buf+'<td style="text-align: left !important;">';
        for h:=0 to FClients[i].UnQueuedLogCount-1 do
        begin
          hidx:=(10+FClients[i].UnQueuedIdx-1-h) MOD 10;
          Buf:=Buf+'Served '+IntToSTr(-h-1);
          if(h<9) then Buf:=Buf+' ';
          Buf:=Buf+' : '+FClients[i].UnQueuedLog[hidx].SchemaName+'.'+FClients[i].UnQueuedLog[hidx].TableName+' ('+IntToStr(FClients[i].UnQueuedLog[hidx].EventType)+') : - '+IntToStr(tick-FClients[i].UnQueuedLogTick[hidx])+' ms<br>';
          tick:=FClients[i].UnQueuedLogTick[hidx];
        end;
        Buf:=Buf+'</td>';
        // --------
                
        Buf:=Buf+'</tr>';
      end;
      Buf:=Buf+'</table>';
      
  end;
  
  Buf:=Buf+'<br>';  
  
  Buf:=Buf+'<h2>TABLE USAGE</h2>';

  Buf:=Buf+'<table cellspacing=0>';
  Buf:=Buf+'<tr><th>Table</th><th>Tot Insert</th><th>Tot Update</th><th>Tot Delete</th><th>Day Insert</th><th>Day Update</th><th>Day Delete</th><th>Hour Insert</th><th>Hour Update</th><th>Hour Delete</th></tr>';

  for i:=0 to FTableStats.Count-1 do
  begin
    Buf:=Buf+'<tr><td style="text-align: left;">'+TTableStat(FTableStats.Items[i]^).Name+'</td><td>'+IntToStr(TTableStat(FTableStats.Items[i]^).NumInsert)+'</td><td>'+IntToStr(TTableStat(FTableStats.Items[i]^).NumUpdate)+'</td><td>'+IntToStr(TTableStat(FTableStats.Items[i]^).NumDelete)+'</td>';
    
    lCountI:=0;
    lCountU:=0;
    lCountD:=0;
    
    if(lDaySec>0) then
      for j:=0 to lDaySec do
      begin
        lCountI:=lCountI+TTableStat(FTableStats.Items[i]^).HistInsert[j];
        lCountU:=lCountU+TTableStat(FTableStats.Items[i]^).HistUpdate[j];
        lCountD:=lCountD+TTableStat(FTableStats.Items[i]^).HistDelete[j];
      end;
    
    Buf:=Buf+'<td>'+IntToStr(lCountI)+'</td><td>'+IntToStr(lCountU)+'</td><td>'+IntToStr(lCountD)+'</td>';

    lCountI:=0;
    lCountU:=0;
    lCountD:=0;
    
    //AddToLog(llWarning,'lDaySec = '+IntToStr(lDaySec));
    //AddToLog(llWarning,'from = '+IntToStr((lDaySec+0+86400-3600) MOD 86400));
    //AddToLog(llWarning,'to = '+IntToStr((lDaySec+3599+86400-3600) MOD 86400));
    
    for j:=0 to 3600-1 do
    begin
      k:=(lDaySec+j+86400-3600) MOD 86400;
      lCountI:=lCountI+TTableStat(FTableStats.Items[i]^).HistInsert[k];
      lCountU:=lCountU+TTableStat(FTableStats.Items[i]^).HistUpdate[k];
      lCountD:=lCountD+TTableStat(FTableStats.Items[i]^).HistDelete[k];
    end;
    
    Buf:=Buf+'<td>'+IntToStr(lCountI)+'</td><td>'+IntToStr(lCountU)+'</td><td>'+IntToStr(lCountD)+'</td>';
        
    Buf:=Buf+'</tr>';
  end;


  Buf:=Buf+'</table>';  
 
  Buf:=Buf+'<br>';  
  
  Buf:=Buf+'<h2>SCHEMA</h2>';

  for i:=0 to Length(FReplicator.FBinLogTables)-1 do
  begin
  Buf:=Buf+'<h3>TABLE '+FReplicator.FBinLogTables[i].SchemaName+'.'+FReplicator.FBinLogTables[i].TableName+'</h3>';
  Buf:=Buf+'<table cellspacing=0>';
  Buf:=Buf+'<tr><th>SchemaUID</th><th>TableUID</th><th>ColUID</th><th>Column</th><th>Type</th><th>FieldType</th><th>Meta</th></tr>';
  
  for j:=0 to Length(FReplicator.FBinLogTables[i].Columns)-1 do
  begin
    with(FReplicator.FBinLogTables[i]) do
    begin
      Buf:=Buf+'<tr><td>'+IntToHex(SchemaNameID,2)+'</td><td>'+IntToHex(TableNameID,2)+'</td><td>'+IntToHex(Columns[j].ColumnNameID,4)+'</td><td>'+Columns[j].ColumnName+'</td><td>'+Columns[j].ColumnType+'</td><td>'+IntToHex(Columns[j].ColumnFieldType,4)+'</td><td>';
      for k:=0 to Length(Columns[j].ColumnFieldMeta)-1 do
      begin
        Buf:=Buf+IntToHex(Ord(Columns[j].ColumnFieldMeta[k]),2);
      end;
      Buf:=Buf+'</td></tr>';
    end;
  end;
  Buf:=Buf+'</table><br>';

  end;

  
  Buf:=Buf+'</body></html>';
  
  AResponseInfo.ContentText := Buf;
  
  AResponseInfo.WriteContent;
  
  //LeaveCriticalSection(FCriticalSection);

end;
{******************************************************************************}
constructor TReplicationRelay.Create(CreateSuspended: Boolean);
var 
  i,j: Integer;
begin
  FNumClient:=1;  // Index 0 is unused
  FMaxClient:=16;
  FNumServed:=0;
  FNumEvent:=0;
  SetLength(FNumEventType,256);
  InitCriticalSection(FCriticalSection);
  for i:=0 to 255 do FNumEventType[i]:=0;
  FTableStats:=TFPHashList.Create;
  FStarted:=Now();
  FAdminHash:=FNV1AQ('allyourbasearebelongtous');
  SetLength(FClients,FMaxClient);
  
  // 20230612
  for i:=0 to FMaxClient-1 do 
  begin
   SetLength(FClients[i].UnQueuedLog,10);
    for j:=0 to 9 do
      FClients[i].UnQueuedLog[j]:=TQueueItem.Create;
      
    SetLength(FClients[i].UnQueuedLogTick,10);
    FClients[i].UnQueuedIdx:=0;  
    FClients[i].UnQueuedLogCount:=0;
  end;
  // --------

   
  
  AddToLog(llWarning,'TReplicationRelay Created');  
  FHTTPServer:=TIdHTTPServer.Create;
  FHTTPServer.OnCommandGet:=HTTPRequest;
  FHTTPServer.Active := false;
  
  FLogExcludes:=TFPHashList.Create;
  
  inherited Create(CreateSuspended);
end;
{******************************************************************************}
destructor TReplicationRelay.Destroy;
begin
  FreeAndNil(FLogExcludes);
	inherited;
end;
{******************************************************************************}
function TReplicationRelay.Init(Host,Login,Password,DB: String;AdminSecret: String;AuthSecret: String;SPort: Integer; HAddress: String; HPort: Integer; HLogPath: String; HLogSchema: String; HLogExcludes: String): Integer;
var
  resu, i: Integer;
  Binding : TIdSocketHandle;
  lStrs: TArray<String>;
  lPositive: UintPtr;
begin
  Result:=0;
  AddToLog(llWarning,'TReplicationRelay Init');  
  FAdminHash:=FNV1AQ(AdminSecret);
  AddToLog(llWarning,'Admin Secret Hash = '+IntToHex(FAdminHash,16));
  FAuthHash:=FNV1AQ(AuthSecret);
  AddToLog(llWarning,'Auth Secret Hash = '+IntToHex(FAuthHash,16));
  FSport:=SPort;
  AddToLog(llWarning,'Listening port : '+IntToStr(FSport));
 
  FHport:=HPort;
  FHAddress:=HAddress;
 
  FLogPath:=HLogPath;
  if(FLogPath<>'') then ForceDirectories(FLogPath);
  AddToLog(llWarning,'Log Path : '+FLogPath);
  
  FLogSchema:=HLogSchema;
  AddToLog(llWarning,'Log Schema : '+FLogSchema);

  lStrs:=HLogExcludes.Split(',');  
  lPositive:=1;
  if(Length(lStrs)>0) then 
  begin
    AddToLog(llWarning,'Got Excludes for log');
    for i:=0 to Length(lStrs)-1 do
    begin
      AddToLog(llWarning,'Exclude : ['+lStrs[i]+']');
      FLogExcludes.Add(lStrs[i],Pointer(lPositive));
    end;
  end;
  AddToLog(llWarning,'Total Excludes: '+IntToSTr(FLogExcludes.Count));
  
  
  FReplicator:=TMysqlReplicationClient.Create;
  
  resu:=FReplicator.Connect(Host,Login,Password,DB);
  if(resu<>1) then
  begin
    AddToLog(llError,'TReplicationRelay : Error connecting to Master MySQL Server!');
    Exit;
  end;

  resu:=FReplicator.Init(GetProcessID()); // Use pid as client ID
  if(resu<>1) then
  begin
    AddToLog(llError,'TReplicationRelay : Error in Replication Client Initialization!');
    Exit;
  end;  
  Result:=1;
  
  Binding := FHTTPServer.Bindings.Add;
  Binding.IP := FHAddress;
  Binding.Port := FHport;
  AddToLog(llWarning,'Server bound to IP ' + Binding.IP + ' on port ' + IntToStr(Binding.Port));  
  FHTTPServer.Active := true;
end;
{******************************************************************************}
function TReplicationRelay.RegisterClient(AHandler: TServerHandler;AClientName: String;AAuth: QWord; var AToken: DWord): Integer;
var
  i,ClientID: Integer;
begin  
  Result:=0;

  if((AAuth<>FAuthHash) OR (AClientName='')) then
  begin
    AddToLog(llWarning,'Invalid Auth or Client Name '+IntToHex(AAuth,16)+'<>'+IntToHex(FAuthHash,16)+' / '+AClientName);
    AToken:=0;
    Result:=0;
    Exit;
  end;
  
  ClientID:=0;
  
  if(FNumClient>0) then
    for i:=0 to FNumClient-1 do
    begin
      if(FClients[i].Name=AClientName) then 
      begin
        ClientID:=i;
        break;
      end;
    end;
  
  if(ClientID>0) then
  begin
    // If the client is already connected, disconnect it
    if((FClients[ClientID].Client<>nil)) then 
    begin
      {$IFDEF REPDEBUG}
      AddToLog(llWarning,'Disconnecting previous connection (ClientID = '+IntToStr(ClientID)+')');
      AddToLog(llWarning,'Disconnecting previous connection (FCID = '+IntToStr(FClients[ClientID].Client.FCID)+')');
      {$ENDIF}
      
      FClients[ClientID].Client.Killswitch:=true;
      
      // FClients[ClientID].Client.Connection.IOHandler.DiscardAll; Hangs?
       //AddToLog(llWarning,'IO Handler DiscardAll Done');
      //FClients[ClientID].Client.Connection.Disconnect;
      //AddToLog(llWarning,'Disconnect Done');
      ///FClients[ClientID].Client.Destroy;
      //AddToLog(llWarning,'Destroyed');
      FClients[ClientID].Client:=nil;
      //AddToLog(llWarning,'Disconnected client');
    end
    else
    begin
      AddToLog(llWarning,'No previous connection found');
    end;

    FClients[ClientID].Client:=AHandler;
    {$IFDEF REPDEBUG}
    AddToLog(llWarning,'NEW AHandler.FCID = '+IntToStr(AHandler.FCID));
    {$ENDIF}
    AToken:=FClients[ClientID].Token;
    
    {$IFDEF REPDEBUG}    
    AddToLog(llWarning,'RegisterClient => Existing Client '+AClientName+', ID = '+IntToStr(ClientID)+', Token = '+IntToHex(AToken,8)+', Queue size = '+IntToStr(FClients[ClientID].Queue.Count));    
    {$ENDIF}
    
    FClients[ClientID].ConnectionStatus:=1;
    Result:=ClientID;
    
    Exit;
  end
  else
  begin    
    if(FNumClient=FMaxClient) then
    begin
      FMaxClient:=FMaxClient*2;
      SetLength(FClients,FMaxClient);
    end;    
    Result:=FNumClient;
    FClients[FNumClient].Id:=FNumClient;
    FClients[FNumClient].Name:=AClientName;
    FClients[FNumClient].Token:=((DateTimeToUnix(Now()) AND $FFFFFF) SHL 8) + (FNumClient AND $FF);
    FClients[FNumClient].Queue:=TPCQueue.Create;
    FClients[FNumClient].NumFilter:=0;
    FClients[FNumClient].MaxFilter:=16;
    FClients[FNumClient].ConnectionStatus:=1;
    
    FClients[FNumClient].Served:=0;
    FClients[FNumClient].Discarded:=0;
    FClients[FNumClient].MaxQueue:=0;
    FClients[FNumClient].Started:=Now();
    FClients[FNumClient].LastSeen:=Now();   
    
    FClients[FNumClient].Client:=AHandler;
    //AddToLog(llWarning,'NEW AHandler.FCID = '+IntToStr(AHandler.FCID));
    SetLength(FClients[FNumClient].Filters,FClients[FNumClient].MaxFilter);
    
    AToken:=FClients[FNumClient].Token;
    Result:=FNumClient;
    
    //AddToLog(llWarning,'RegisterClient => New Client #'+IntToStr(FNumClient)+', Name = '+AClientName+', Token '+IntToHex(AToken,8));
    
    // 20230612
    SetLength(FClients[FNumClient].UnQueuedLog,10);
    for i:=0 to 9 do
      FClients[FNumClient].UnQueuedLog[i]:=TQueueItem.Create;
      
    SetLength(FClients[FNumClient].UnQueuedLogTick,10);
    FClients[FNumClient].UnQueuedIdx:=0;
    FClients[FNumClient].UnQueuedLogCount:=0;
    // -------- 
    
    Inc(FNumClient);
    Exit;
  end;  
  
end;
{******************************************************************************}
function TReplicationRelay.AddFilter(AClientID: DWord; AFilterType: Byte; ASchemaName,ATableName: String;AFilterDiscardType: Byte; AQueueLimit: Integer): Integer;
var 
  i: Integer; 
begin
  if(AClientId<1) OR (AClientID>=FNumClient) then
  begin
    Result:=0;
    Exit;
  end;
  
  if(FClients[AClientID].NumFilter>0) then
    for i:=0 to FClients[AClientID].NumFilter-1 do
      if((FClients[AClientID].Filters[i].FilterType=AFilterType) AND (FClients[AClientID].Filters[i].SchemaName=ASchemaName) AND (FClients[AClientID].Filters[i].TableName=ATableName)) then
      begin
        {$IFDEF REPDEBUG}   
        AddToLog(llWarning,'WARNING : Client tried to add existing filter, only updating discard info');
        {$ENDIF}
        FClients[AClientID].Filters[i].FilterDiscardType:=AFilterDiscardType;
        FClients[AClientID].Filters[i].FilterQueueLimit:=AQueueLimit;  
        Result:=1;
        Exit;
      end;
  
  if(FClients[AClientID].NumFilter=FClients[AClientID].MaxFilter) then
  begin
    FClients[AClientID].MaxFilter:=FClients[AClientID].MaxFilter*2;
    SetLength(FClients[AClientID].Filters,FClients[AClientID].MaxFilter);
  end;
  FClients[AClientID].Filters[FClients[AClientID].NumFilter].FilterDiscardType:=AFilterDiscardType;
  FClients[AClientID].Filters[FClients[AClientID].NumFilter].FilterQueueLimit:=AQueueLimit;  
  FClients[AClientID].Filters[FClients[AClientID].NumFilter].FilterType:=AFilterType;
  FClients[AClientID].Filters[FClients[AClientID].NumFilter].SchemaName:=ASchemaName;
  FClients[AClientID].Filters[FClients[AClientID].NumFilter].TableName:=ATableName;
  
  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'Added Filter #'+IntToStr(FClients[AClientID].NumFilter)+' to client #'+IntToSTr(AClientID)+' : '+IntToStr(AFilterType)+'/'+ASchemaName+'.'+ATableName);
  {$ENDIF}
  
  Inc(FClients[AClientID].NumFilter);
  Result:=1;
end;
{******************************************************************************}
procedure TReplicationRelay.LogEvent(AEventType: Byte; AEvent: TBinLogEvent);
var
  i,j: Integer;
  lFileName: String;
  lHandler: TextFile;
  lStrHead: UTF8String;
  //lStr: BigString;
  lStr: UTF8String;
begin
  
  try
    EnterCriticalSection(FCriticalSection);
    //lStr:=lBigString.Create;
    if(AEvent.Schema<>FLogSchema) then Exit;
    if(FLogExcludes.Find(AEvent.Table)<>Nil) then Exit;
    
    lFileName:=FLogPath+'relaylogs-'+FormatFloat('0000',FYear)+FormatFloat('00',FMonth)+FormatFloat('00',FDay)+'.txt';
    
    assignFile(lHandler,lFileName);
    try
    
      if(FileExists(lFileName)) then Append(lHandler)
      else Rewrite(lHandler);
    except
      on e: Exception do
      begin
        writeln('Error Opening log file : ', e.Message);
        Exit;
      end;    
    end;    
      
    
    lStrHead:=FormatFloat('00',FHour)+':'+FormatFloat('00',FMin)+':'+FormatFloat('00',FSec)+';'+AEvent.Schema+';'+AEvent.Table+';';
    if(AEventType=REPRELAYREQ_INSERT) then lStrHead:=lStrHead+'I'
    else if(AEventType=REPRELAYREQ_UPDATE) then lStrHead:=lStrHead+'U'
    else if(AEventType=REPRELAYREQ_DELETE) then lStrHead:=lStrHead+'D';

    //AddToLog(llWarning,'Head = '+lStrHead);

    for i:=0 to AEvent.NumRows-1 do
    begin
      //AddToLog(llWarning,'Row '+IntToStr(i)+' NumColumns = '+IntToStr(AEvent.NumColumns));
      lStr:=lStrHead;
      for j:=0 to AEvent.NumColumns-1 do
      begin
        if(AEvent.ColumnTypes[j]=FIELD_TYPE_TINY_BLOB) OR (AEvent.ColumnTypes[j]=FIELD_TYPE_MEDIUM_BLOB) OR (AEvent.ColumnTypes[j]=FIELD_TYPE_LONG_BLOB) OR (AEvent.ColumnTypes[j]=FIELD_TYPE_BLOB) then continue;
        
        if(AEventType=REPRELAYREQ_INSERT) then 
        begin
          lStr:=lStr+';'+AEvent.Rows[i].FieldsOut[j];
          
          if(AEvent.ColumnTypes[j]=FIELD_TYPE_DECIMAL) OR (AEvent.ColumnTypes[j]=FIELD_TYPE_TINY) OR
          (AEvent.ColumnTypes[j]=FIELD_TYPE_SHORT) OR (AEvent.ColumnTypes[j]=FIELD_TYPE_LONG) OR
          (AEvent.ColumnTypes[j]=FIELD_TYPE_FLOAT) OR (AEvent.ColumnTypes[j]=FIELD_TYPE_DOUBLE) OR
          (AEvent.ColumnTypes[j]=FIELD_TYPE_LONGLONG) OR (AEvent.ColumnTypes[j]=FIELD_TYPE_INT24) 
          then lStr:=lStr+';'+AEvent.Rows[i].ValuesOut[j]       
          else if(AEvent.Rows[i].ValuesOut[j]='NULL') then lStr:=lStr+';'
          else if(Length(AEvent.Rows[i].ValuesOut[j])=19) AND (AEvent.Rows[i].ValuesOut[j]='0000-00-00 00:00:00') then lStr:=lStr+';0'
          else lStr:=lStr+';'+'"'+StringReplace(AEvent.Rows[i].ValuesOut[j],'"','\"',[rfReplaceAll])+'"';          
        end
        else if(AEventType=REPRELAYREQ_UPDATE) then 
        begin
          lStr:=lStr+';'+AEvent.Rows[i].FieldsIn[j];
          
          if(AEvent.ColumnTypes[j]=FIELD_TYPE_DECIMAL) OR (AEvent.ColumnTypes[j]=FIELD_TYPE_TINY) OR
          (AEvent.ColumnTypes[j]=FIELD_TYPE_SHORT) OR (AEvent.ColumnTypes[j]=FIELD_TYPE_LONG) OR
          (AEvent.ColumnTypes[j]=FIELD_TYPE_FLOAT) OR (AEvent.ColumnTypes[j]=FIELD_TYPE_DOUBLE) OR
          (AEvent.ColumnTypes[j]=FIELD_TYPE_LONGLONG) OR (AEvent.ColumnTypes[j]=FIELD_TYPE_INT24) 
          then lStr:=lStr+';'+AEvent.Rows[i].ValuesIn[j]+';'+AEvent.Rows[i].ValuesOut[j]   
          else 
          begin
            if(AEvent.Rows[i].ValuesIn[j]='NULL') then lStr:=lStr+';'
            else if(Length(AEvent.Rows[i].ValuesIn[j])=19) AND (AEvent.Rows[i].ValuesIn[j]='0000-00-00 00:00:00') then lStr:=lStr+';0'
            else lStr:=lStr+';'+'"'+StringReplace(AEvent.Rows[i].ValuesIn[j],'"','\"',[rfReplaceAll])+'"';   
            
            if(AEvent.Rows[i].ValuesOut[j]='NULL') then lStr:=lStr+';'
            else if(Length(AEvent.Rows[i].ValuesOut[j])=19) AND (AEvent.Rows[i].ValuesOut[j]='0000-00-00 00:00:00') then lStr:=lStr+';0'
            else lStr:=lStr+';'+'"'+StringReplace(AEvent.Rows[i].ValuesOut[j],'"','\"',[rfReplaceAll])+'"';   
          end;
          
        end
        else if(AEventType=REPRELAYREQ_DELETE) then 
        begin
          lStr:=lStr+';'+AEvent.Rows[i].FieldsIn[j];
          
          if(AEvent.ColumnTypes[j]=FIELD_TYPE_DECIMAL) OR (AEvent.ColumnTypes[j]=FIELD_TYPE_TINY) OR
          (AEvent.ColumnTypes[j]=FIELD_TYPE_SHORT) OR (AEvent.ColumnTypes[j]=FIELD_TYPE_LONG) OR
          (AEvent.ColumnTypes[j]=FIELD_TYPE_FLOAT) OR (AEvent.ColumnTypes[j]=FIELD_TYPE_DOUBLE) OR
          (AEvent.ColumnTypes[j]=FIELD_TYPE_LONGLONG) OR (AEvent.ColumnTypes[j]=FIELD_TYPE_INT24) 
          then lStr:=lStr+';'+AEvent.Rows[i].ValuesIn[j]          
          else if(AEvent.Rows[i].ValuesIn[j]='NULL') then lStr:=lStr+';'
          else if(Length(AEvent.Rows[i].ValuesIn[j])=19) AND (AEvent.Rows[i].ValuesIn[j]='0000-00-00 00:00:00') then lStr:=lStr+';0'          
          else lStr:=lStr+';'+'"'+StringReplace(AEvent.Rows[i].ValuesIn[j],'"','\"',[rfReplaceAll])+'"';                  
          
        end;                
      end;
      WriteLn(lHandler,lStr);
    end;
    closeFile(lHandler);

  finally
    LeaveCriticalSection(FCriticalSection);
    //FreeAndNil(lStr);
  end;
end;

{******************************************************************************}
procedure TReplicationRelay.EnqueueEvent(AEventType: Byte; AEvent: TBinLogEvent);
var
  i,j,k,l: Integer;
  lItem,lOldItem: TQueueItem;
begin
  EnterCriticalSection(FCriticalSection);

  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'EnqueueEvent '+IntToStr(AEVentType));
  {$ENDIF}
  if(FMaxClient=0) then Exit;
  for i:=0 to FNumClient-1 do
  begin
    {$IFDEF REPDEBUG}
    AddToLog(llWarning,'Client #'+IntToStr(i)+' has '+IntToStr(FClients[i].NumFilter)+' filters');
    {$ENDIF}
    if(FClients[i].NumFilter=0) then continue;
    for j:=0 to FClients[i].NumFilter-1 do
    begin
      {$IFDEF REPDEBUG}
      AddToLog(llWarning,'Filter #'+IntToStr(j)+' : '+IntToStr(FClients[i].Filters[j].FilterType)+' on '+FClients[i].Filters[j].SchemaName+'.'+FClients[i].Filters[j].TableName);
      AddToLog(llWarning,'Client has filter for '+AEvent.Schema+'.'+AEvent.Table+' ?');
      {$ENDIF}
//      if not ((FClients[i].Filters[j].FilterType AND AEventType)<>0) then
//        AddToLog(llWarning, 'filter DTC');
//      if (FClients[i].Filters[j].SchemaName<>AEvent.Schema) then
//        AddToLog(llWarning, 'schema DTC '+FClients[i].Filters[j].SchemaName+'<>'+AEvent.Schema+' / '+IntToStr(Length(FClients[i].Filters[j].SchemaName))+'<>?'+IntToStr(Length(AEvent.Schema)));
//      if (FClients[i].Filters[j].TableName<>AEvent.Table) then
//        AddToLog(llWarning, 'table DTC '+FClients[i].Filters[j].TableName+'<>'+AEvent.Table+' / '+IntToStr(Length(FClients[i].Filters[j].TableName))+'<>?'+IntToStr(Length(AEvent.Table)));

      if ((FClients[i].Filters[j].FilterType AND AEventType)<>0) AND (FClients[i].Filters[j].SchemaName=AEvent.Schema) AND (FClients[i].Filters[j].TableName=AEvent.Table) then
      begin
        {$IFDEF REPDEBUG}
        AddToLog(llWarning,'Client has filter for '+AEvent.Schema+'.'+AEvent.Table);
        {$ENDIF}
        for k:=0 to AEvent.NumRows-1 do
        begin
          lItem:=TQueueItem.Create;
          lItem.EventType:=AEventType;
          lItem.InstanceID:=GInstanceID;
          lItem.SchemaNameID:=0; // TODO
          lItem.SchemaName:=AEvent.Schema;
          lItem.TableNameID:=0; // TODO
          lItem.TableName:=AEvent.Table;
          SetLength(lItem.Cols,AEvent.NumColumns);
          for l:=0 to AEvent.NumColumns-1 do
          begin
            if((AEventType AND REPRELAYREQ_INSERT)<>0) then
            begin
              lItem.Cols[l].NameID:=0; // TODO
              lItem.Cols[l].Name:=AEvent.Rows[k].FieldsOut[l];
              lItem.Cols[l].Before:='';
              lItem.Cols[l].After:=AEvent.Rows[k].ValuesOut[l];
            end
            else if((AEventType AND REPRELAYREQ_DELETE)<>0) then
            begin
              lItem.Cols[l].NameID:=0; // TODO
              lItem.Cols[l].Name:=AEvent.Rows[k].FieldsIn[l];
              lItem.Cols[l].After:='';
              lItem.Cols[l].Before:=AEvent.Rows[k].ValuesIn[l];
            end
            else if((AEventType AND REPRELAYREQ_UPDATE)<>0) then
            begin
              if(AEvent.Rows[k].FieldsOut[l]<>AEvent.Rows[k].FieldsIn[l]) then
              begin
                AddToLog(llWarning,'ALERT : FIELD IN/OUT NAME MISMATCH!!! '+AEvent.Rows[k].FieldsIn[l]+' <> '+AEvent.Rows[k].FieldsOut[l]);
                Halt;
              end;
              lItem.Cols[l].NameID:=0; // TODO
              lItem.Cols[l].Name:=AEvent.Rows[k].FieldsOut[l];
              lItem.Cols[l].Before:=AEvent.Rows[k].ValuesIn[l];
              lItem.Cols[l].After:=AEvent.Rows[k].ValuesOut[l];
            end            
          end;                         
          
          lItem.EventPosition:=DateTimeToUnix(Now) SHL 32; // Unique Event ID
          lItem.EventPosition:=lItem.EventPosition+FReplicator.FBinLogOffset;
          
          if((FClients[i].Queue.Count>=FClients[i].Filters[j].FilterQueueLimit) AND (FClients[i].Filters[j].FilterDiscardType<>REPRELAY_DISCARD_NODISCARD)) then 
          begin
            {$IFDEF REPDEBUG}
            AddToLog(llWarning,'Queue Limit Reached');
            {$ENDIF}
            Inc(FClients[i].Discarded);
            if((FClients[i].Filters[j].FilterDiscardType=REPRELAY_DISCARD_UNQUEUEOLDEST)) then 
            begin
              {$IFDEF REPDEBUG}
              AddToLog(llWarning,'Unqueue Oldest');
              {$ENDIF}
              lOldItem:=FClients[i].Queue.Pop();
              FClients[i].Queue.Push(lItem); 
              FreeAndNil(lOldItem); // Leak fixed here
            end
            else 
            begin
              {$IFDEF REPDEBUG}
              AddToLog(llWarning,'Ignore New');
              {$ENDIF}         
              FreeAndNil(lItem); // Leak fixed here
            end;
          end
          else FClients[i].Queue.Push(lItem); 
          if(FClients[i].Queue.Count()>FClients[i].MaxQueue) then FClients[i].MaxQueue:=FClients[i].Queue.Count();

          {$IFDEF REPDEBUG}
          AddToLog(llWarning,'Enqueued Event to Client '+IntToStr(i)+' Total in Queue '+IntToStr(FClients[i].Queue.Count));
          {$ENDIF}
        end;
      end;
    end;
  end;
  LeaveCriticalSection(FCriticalSection);
end;
{******************************************************************************}
function TReplicationRelay.PollEvent(AClientID: DWord): TQueueItem;
var
  k: Integer;  
  lItem: TQueueItem;
begin
  if((AClientID<1) OR (AClientID>FNumClient)) then
  begin
    Result:=nil;
    Exit;
  end;
  EnterCriticalSection(FCriticalSection);
  
  FClients[AClientID].LastSeen:=Now();       
  if(FClients[AClientID].Queue.Count>0) then 
  begin
    Inc(FClients[AClientID].Served);
    Inc(FNumServed);  
  end;
  lItem:=FClients[AClientID].Queue.Pop();
  if(lItem<>nil) then 
  begin
    lItem.QueueSize:=FClients[AClientID].Queue.Count;
    
    // 20230612
    k:=FClients[AClientID].UnQueuedIdx;
    
    FClients[AClientID].UnQueuedLog[k].EventType:=lItem.EventType;
    FClients[AClientID].UnQueuedLog[k].SchemaName:=lItem.SchemaName;
    FClients[AClientID].UnQueuedLog[k].TableName:=lItem.TableName;
    FClients[AClientID].UnQueuedLogTick[k]:=GetTickCount64;
    
    FClients[AClientID].UnQueuedIdx:=(FClients[AClientID].UnQueuedIdx+1) MOD 10;
    if(FClients[AClientID].UnQueuedLogCount<10) then Inc(FClients[AClientID].UnQueuedLogCount);
    // --------
    
  end;
  Result:=lItem;
  LeaveCriticalSection(FCriticalSection);
end;
{******************************************************************************}
function TReplicationRelay.PollEventV2(AClientID: DWord): TQueueItem;
begin
  Result:=PollEvent(AClientID);
end;
{******************************************************************************}
function TReplicationRelay.Run(): Integer;
begin
  // Obsolete
  Result:=0;
end;
{******************************************************************************}
procedure TReplicationRelay.Execute();
var 
  i,j: Integer;
  Event: TBinLogEvent;
  lPtr: ^TTableStat;
  lHour,lMin,lSec,lMil: Word;
  lDaySec: Integer;
  lLastDaySec: Integer;
  lNow: TDateTime;
begin 
  Event.Init();
  
  AddToLog(llWarning,'TReplicationRelay : Run, starting server...');
  
  FServer:=TServer.Create;
  FServer.Start(FSPort);
  FServer.Relay:=self;
  
  lLastDaySec:=0;
  
  AddToLog(llWarning,'TReplicationRelay : Run, polling events...');
  while (true) do
  begin
  
    lNow:=Now();
    DecodeDate(lNow,FYear,FMonth,FDay);
    DecodeTime(lNow,lHour,lMin,lSec,lMil);
    FHour:=lHour;
    FMin:=lMin;
    FSec:=lSec;
    lDaySec:=lHour*3600+lMin*60+lSec;
    if(lDaySec<>lLastDaySec) then
    begin      
      //AddToLog(llWarning,'tick '+IntToStr(lDaySec)+' <> '+IntToStr(lLastDaySec));
      for j:=lLastDaySec+1 to lDaySec do
      begin
        //AddToLog(llWarning,'clear '+IntToStr(j));
        for i:=0 to FTableStats.Count-1 do
        begin
          TTableStat(FTableStats.Items[i]^).HistInsert[j]:=0;
          TTableStat(FTableStats.Items[i]^).HistUpdate[j]:=0;
          TTableStat(FTableStats.Items[i]^).HistDelete[j]:=0;
        end;
      end;
      lLastDaySec:=lDaySec;

    end;
     
    try
      //WriteLn('Poll');
      {$IFDEF REPDEBUG}
      AddToLog(llWarning,'Poll...');
      {$ENDIF}
      FReplicator.GetEvent(Event); /// Ignore the result, in case of error, EventType will be 666 and we will reconnect below
      
      Inc(FNumEvent);
      if((Event.EventType>0) AND (Event.EventType<=255)) then Inc(FNumEventType[Event.EventType]);        
      
      case Event.EventType of
        666:
          begin
            FReplicator.Reconnect();
          end;
        $02:
          begin
            {$IFDEF REPDEBUG}
            if(Event.Data[1]<>'#') then
              AddToLog(llWarning,'QUERY : '+Event.Data);
            {$ENDIF}
              //WriteLn('QUERY : '+Event.Data);
          end;
        $17:
          begin
            {$IFDEF REPDEBUG}
            AddToLog(llWarning,'*INSERT* : Table '+Event.Schema+'.'+Event.Table);
            {$ENDIF}
            if(FLogSchema<>'') then LogEvent(REPRELAYREQ_INSERT,Event);
            EnqueueEvent(REPRELAYREQ_INSERT,Event);
            
            lPtr:=FTableStats.Find(Event.Schema+'.'+Event.Table);
            if(lPtr=nil) then 
            begin
              New(lPtr);
              (lPtr^).Name:=Event.Schema+'.'+Event.Table;
                            
              for j:=0 to 86400-1 do
              begin
                (lPtr^).HistInsert[j]:=0;
                (lPtr^).HistUpdate[j]:=0;
                (lPtr^).HistDelete[j]:=0;
              end;
              (lPtr^).HistInsert[lDaySec]:=1;
              
              (lPtr^).NumInsert:=1;
              (lPtr^).NumUpdate:=0;
              (lPtr^).NumDelete:=0;                 
              FTableStats.Add((lPtr^).Name,lPtr);
              //AddToLog(llWarning,'* '+(lPtr^).Name);
            end
            else 
            begin
              Inc((lPtr^).NumInsert);
              Inc((lPtr^).HistInsert[lDaySec]);
            end;
            //AddToLog(llWarning,'# '+(lPtr^).Name);              
          end;
        $18:
          begin
            {$IFDEF REPDEBUG}
            AddToLog(llWarning,'*UPDATE* : Table '+Event.Schema+'.'+Event.Table);
            {$ENDIF}
            if(FLogSchema<>'') then LogEvent(REPRELAYREQ_UPDATE,Event);
            EnqueueEvent(REPRELAYREQ_UPDATE,Event);
            
            lPtr:=FTableStats.Find(Event.Schema+'.'+Event.Table);
            if(lPtr=nil) then 
            begin
              New(lPtr);
              (lPtr^).Name:=Event.Schema+'.'+Event.Table;
                             
              for j:=0 to 86400-1 do
              begin
                (lPtr^).HistInsert[j]:=0;
                (lPtr^).HistUpdate[j]:=0;
                (lPtr^).HistDelete[j]:=0;
              end;                
              (lPtr^).HistUpdate[lDaySec]:=1;
              
              (lPtr^).NumInsert:=0;
              (lPtr^).NumUpdate:=1;
              (lPtr^).NumDelete:=0;                 
              FTableStats.Add(TTableStat(lPtr^).Name,lPtr);
              //AddToLog(llWarning,'* '+(lPtr^).Name);
            end
            else 
            begin
              Inc((lPtr^).NumUpdate);
              Inc((lPtr^).HistUpdate[lDaySec]);
            end;
            //AddToLog(llWarning,'# '+(lPtr^).Name);
          end;
        $19:
          begin
            {$IFDEF REPDEBUG}
            AddToLog(llWarning,'*DELETE* : Table '+Event.Schema+'.'+Event.Table);            
            {$ENDIF}
            //AddToLog(llWarning,'*DELETE* : Table '+Event.Schema+'.'+Event.Table+', lDaySec = '+IntToStr(lDaySec));            
            if(FLogSchema<>'') then LogEvent(REPRELAYREQ_DELETE,Event);
            EnqueueEvent(REPRELAYREQ_DELETE,Event);

            lPtr:=FTableStats.Find(Event.Schema+'.'+Event.Table);
            if(lPtr=nil) then 
            begin
              New(lPtr);
              (lPtr^).Name:=Event.Schema+'.'+Event.Table;

              for j:=0 to 86400-1 do
              begin
                (lPtr^).HistInsert[j]:=0;
                (lPtr^).HistUpdate[j]:=0;
                (lPtr^).HistDelete[j]:=0;
              end;                
              (lPtr^).HistDelete[lDaySec]:=Event.NumRows;                
              
              (lPtr^).NumInsert:=0;
              (lPtr^).NumUpdate:=0;
              (lPtr^).NumDelete:=Event.NumRows;                 
              FTableStats.Add((lPtr^).Name,lPtr);
              //AddToLog(llWarning,'* '+(lPtr^).Name);
            end
            else 
            begin
              Inc((lPtr^).NumDelete,Event.NumRows);
              Inc((lPtr^).HistDelete[lDaySec],Event.NumRows);
            end;
            //AddToLog(llWarning, '+(lPtr^).Name);
          end
      end;

    Except
      on E:Exception do 
      begin
        AddToLog(llWarning,'Exception : '+E.Message);
      end;
    end;   
  end;
  FReplicator.Free;        
end;
{******************************************************************************}
end.
