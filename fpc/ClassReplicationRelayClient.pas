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
unit ClassReplicationRelayClient;

{$IFDEF FPC}
  {$MODE Delphi}
{$ENDIF}

{$UNDEF REPDEBUG}
interface 

uses
  ReplicationRelayStructures,Declarations, ClassModule, Dateutils, Classes, SysUtils, ClassDebug, IdTCPClient, IdIOHandler,  IdGlobal, StrUtils,HFNV1A;

const

  CONST_NOTHING=0;

type

  TReplicationRelayClient = class
  protected
    FTCPClient: TIdTCPClient;
    FHBufferStream: TMemoryStream;
    FBufferStream: TMemoryStream;
    FServerHost: String;
    FServerPort: Integer;
    
    FClientID: Integer;
    FClientToken: Integer;
    
  public
    ClientDisconnected: Boolean;
    
    constructor Create;
    destructor Destroy; override;
    
    function Connect(Host: String;Port : Integer): Integer;
    
    procedure SetClientID(AClientID: Integer; AToken: DWord); overload;
    procedure SetClientID(AClientID: Integer; ASecret: String); overload;
    
    function SendCommand(ACommand,ASubcommand: Integer): Integer;
    function GetReply(AStream: TMemoryStream):Integer;

    function CommandPing(): Integer;
    function CommandAuth(AClientName: String; AAuthKey: QWord): Integer;
    function CommandReqFilter(AReqType: Byte; ASchemaName,ATableName: String;ADiscardType: Byte; AQueueLimit: Integer): Integer;
    function CommandPoll(): TQueueItem;
    function CommandPollV2(): TQueueItem;
    
  published
  end;   

       
implementation

{******************************************************************************}
function PopStringFromStream(AStream:TMemoryStream): String;
var
  i,len: DWord;
begin
  Result:='';
  len:=AStream.ReadDWord(); 
  
  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'strlen = '+IntToStr(len));
  {$ENDIF}
  
  if(len>1) then  // 0 size string has size 1 (null terminator)
    for i:=0 to len-2 do
      Result:=Result+Char(AStream.ReadByte());
  AStream.ReadByte(); // Read $00
  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'PopStringFromStream read ['+Result+']');
  {$ENDIF}
end;
{******************************************************************************}
{ TReplicationRelayClient }
{******************************************************************************}
procedure PushStringToStream(AStream:TMemoryStream;AString: String);
var
  len: DWord;
begin
  len:=Length(AString)+1;
  AStream.WriteDWord(len);
  AStream.Write(AString[1],Length(AString));
  AStream.WriteByte(0);
end;
{******************************************************************************}
constructor TReplicationRelayClient.Create;
begin
  FClientID:=0;
  FClientToken:=0; 
  ClientDisconnected:=true;
  FTCPClient:=nil;
  FHBufferStream:=TMemoryStream.Create;
  FBufferStream:=TMemoryStream.Create;
end;
{******************************************************************************}
destructor TReplicationRelayClient.Destroy;
begin
  FreeAndNil(FBufferStream);
  FreeAndNil(FHBufferStream);
	inherited;
end;
{******************************************************************************}
procedure TReplicationRelayClient.SetClientID(AClientID: Integer; AToken: DWord); overload;
begin
  FClientID:=AClientID;
  FClientToken:=AToken;
end;
{******************************************************************************}
procedure TReplicationRelayClient.SetClientID(AClientID: Integer; ASecret: String); overload;
begin
  FClientID:=AClientID;
  FClientToken:=FNV1A(ASecret);
end;
{******************************************************************************}
function TReplicationRelayClient.Connect(Host: String;Port : Integer): Integer;
var
  tmp: String;
  resu,i: Integer;
begin
  Result:=0;
	AddToLog(llWarning,'TReplicationRelayClient.Init');

	FServerHost:=Host;
	FServerPort:=Port;

  try
    if(FTCPClient<>nil) then 
    begin
      AddToLog(llWarning,'Reconnect Detected');
      FreeAndNil(FTCPClient);  // This is a reconnect
    end;
    FTCPClient:=TIdTCPClient.Create;
    FTCPClient.Host:=FServerHost;
    FTCPClient.Port:=FServerPort;
    FTCPClient.Connect();
  except
    on E:Exception do 
    begin
      AddToLog(llError,'Error connecting to host '+FServerHost+' : '+E.Message);
      Exit;
    end; 
  end;
  
  if(FTCPClient.Connected) then
  begin
    ClientDisconnected:=false;
    AddToLog(llWarning,'Re/Connection to Relay Server Successful');
    
    resu:=CommandPing();
    
    if(resu=0) then
    begin
      AddToLog(llWarning,'Ping failed');
      Exit;
    end;
    AddToLog(llWarning,'Ping successful');
    Result:=1;
  end;
end;
{******************************************************************************}
function TReplicationRelayClient.SendCommand(ACommand,ASubcommand: Integer): Integer;
var
  i: Integer;
  lHeader: TReplicationHeader;
begin
  Result:=0;
  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'SendCommand '+IntToStr(ACommand)+','+IntToStr(ASubcommand)+', payload size = '+IntToStr(FBufferStream.Size));
  {$ENDIF}
  
  lHeader.Command:=ACommand;
  lHeader.Subcommand:=ASubcommand;
  lHeader.PayloadSize:=FBufferStream.Size;
  lHeader.Version:=REPRELAY_VERSION;
  lHeader.ClientId:=FClientID;
  lHeader.ClientToken:=FClientToken;
  lHeader.Reserved1:=0;
  lHeader.Reserved2:=0;
  lHeader.Checksum:=lHeader.Command+lHeader.PayloadSize+lHeader.Subcommand+lHeader.Version+lHeader.ClientId+lHeader.ClientToken+lHeader.Reserved1+lHeader.Reserved2;
  
  //AddToLog(llWarning,'Header Checksum = '+IntToHex(lHeader.Checksum,8));
  
  if(FBufferStream.Size>0) then
  begin
    FBufferStream.Seek(0, soFromBeginning);
    for i:=0 to FBufferStream.Size-1 do
      lHeader.Checksum:=lHeader.Checksum+Ord(FBufferStream.ReadByte());
    FBufferStream.Seek(0, soFromBeginning);
  end;
  
  try  
    FTCPClient.Socket.Write(RawToBytes(lHeader,Sizeof(lHeader))); 
    
    if(FBufferStream.Size>0) then 
    begin
      FTCPClient.Socket.Write(FBufferStream,FBufferStream.Size);
    end;
    Result:=1;
    
  except
    on E:Exception do
    begin
      AddToLog(llError, 'TReplicationRelayClient.SendCommand (Disconnect?) => '+E.Message);
      Sleep(1000);  // Prevent cycling fast
      Result:=0;
    end;
  end;  
end; 
{******************************************************************************}
function TReplicationRelayClient.GetReply(AStream: TMemoryStream):Integer;
var
  lHeader: TReplicationReplyHeader;
  i,lCheck: Integer;
  lBuf: TIdBytes;
  str: String;
  tickcount: QWord;
begin
  tickcount:=GetTickCount64;
 try  

 FTCPClient.IOHandler.ReadBytes(lBuf,SizeOf(lHeader)); 

 except
   on E:Exception do
   begin
     AddToLog(llError, 'TReplicationRelayClient.GetReply (Disconnect?) => '+E.Message);
     Sleep(1000);  // Prevent cycling fast
     Result:=$F0; // Disconnect
     Exit;
   end;
 end;  
  
  BytesToRaw(lBuf,lHeader,SizeOf(lHeader));

  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'Got reply '+IntToStr(lHeader.Result)+' : Payload size '+IntToStr(lHeader.PayloadSize)+' : Checksum '+IntToHex(lHeader.Checksum,4));
  {$ENDIF}

  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'Payload Size = '+IntToStr(lHeader.PayloadSize));
  {$ENDIF}
  
  AStream.Clear();
  
  try    
    tickcount:=GetTickCount64;
    FTCPClient.IOHandler.ReadStream(AStream,lHeader.PayloadSize);
    if((GetTickCount64-tickcount)>1000) then AddToLog(llWarning,'Removed Halt, but ReadStram Delay = '+IntToStr(GetTickCount64-tickcount)+' ms');
  except
   on E:Exception do
   begin
     AddToLog(llError, 'TReplicationRelayClient.GetReply (Disconnect?) => '+E.Message);
     Sleep(1000);  // Prevent cycling fast
     Result:=$F0; // Disconnect
     Exit;
   end;
 end;    
 
  AStream.Seek(0,soFromBeginning);
  lCheck:=lHeader.Result+lHeader.PayloadSize;
  //AddToLog(llWarning,'Checksum header = '+IntToHex(lCheck,4));
  for i:=0 to lHeader.PayloadSize-1 do 
  begin
    lCheck:=lCheck+AStream.ReadByte;
  end;
  
  if(lCheck<>lHeader.Checksum) then
  begin
    AddToLog(llWarning,'Checksum mismatch in reply + '+IntToHex(lCheck,4)+' <> '+IntToHex(lHeader.Checksum,4));
    Result:=$FE;
  end
  else
    Result:=lHeader.Result;
end;
{******************************************************************************}
function TReplicationRelayClient.CommandPing(): Integer;
var
  tmp: String;
  resu,len,i: Integer;
  lStr: String;
begin
  Result:=0;
  FBufferStream.Clear();
  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'CommandPing');
  {$ENDIF}
  resu:=SendCommand(REPRELAY_PING,0);
  if(resu=0) then
  begin
    Result:=0;
    Exit;
  end;
  resu:=GetReply(FBufferStream);
  if(resu=$F0) then
  begin
    Result:=0;
    Exit;
  end;
  
  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'CommandPing Result : '+IntToStr(resu));
  AddToLog(llWarning,'Payload Size : '+IntToStr(FBufferStream.Size));
  {$ENDIF}
  
  FBufferStream.Seek(0, soFromBeginning);
  lStr:='';
  for i:=0 to FBufferStream.Size-1 do
    lStr:=lStr+Char(FBufferStream.ReadByte());
  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'Message : '+lStr);
  {$ENDIF}
  
  if(resu=0) then Result:=1;
end;
{******************************************************************************}
function TReplicationRelayClient.CommandAuth(AClientName: String; AAuthKey: QWord): Integer;
var
  tmp: String;
  resu,len,i: Integer;
  lStr: String;
  lPayload: TReplicationMessageAuth;
begin
  Result:=0;
  FBufferStream.Clear();
  AddToLog(llWarning,'CommandAuth');
  AddToLog(llWarning,'AuthKey = '+IntToHEx(AAuthKey,16));
  
  lPayload.AuthKey:=AAuthKey;
  lPayload.ClientID:=0;
  lPayload.ClientToken:=0;
  lPayload.ClientNameSize:=Length(AClientName)+1;
  

  FBufferStream.Write(lPayload,8+4+4+4);
  FBufferStream.Write(AClientName[1],Length(AClientName));
  FBufferStream.WriteByte(0);
  
  resu:=SendCommand(REPRELAY_AUTH,0);
  if(resu=0) then
  begin
    Result:=0;
    Exit;
  end;
 
  
  resu:=GetReply(FBufferStream);
  if(resu=$F0) then
  begin
    Result:=0;
    Exit;
  end;
  
  FBufferStream.Seek(0, soFromBeginning);
  FBufferStream.Read(lPayload,8+4+4);
  
  if(resu=0) then
  begin
    FClientID:=lPayload.ClientID;
    FClientToken:=lPayload.ClientToken;
  end;

  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'CommandAuth Result : '+IntToStr(resu));
  AddToLog(llWarning,'Payload Size : '+IntToStr(FBufferStream.Size));
  {$ENDIF}
  AddToLog(llWarning,'Client ID : '+IntToStr(lPayload.ClientID)+' Token : '+IntToHex(lPayload.ClientToken,8));
  
  
  if(resu=0) then Result:=1;
end;
{******************************************************************************}
function TReplicationRelayClient.CommandReqFilter(AReqType: Byte; ASchemaName,ATableName: String; ADiscardType: Byte; AQueueLimit: Integer): Integer;
var
  tmp: String;
  resu,len,i: Integer;
  lStr: String;
begin
  Result:=0;
  FBufferStream.Clear();
  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'CommandReqFilter');
  {$ENDIF}

  FBufferStream.WriteByte(AReqType);

  FBufferStream.WriteByte(ADiscardType);
  FBufferStream.WriteDWord(AQueueLimit);
  
  len:=Length(ASchemaName)+1; // Extra Null
  FBufferStream.WriteDWord(len);
  FBufferStream.Write(ASchemaName[1],Length(ASchemaName));
  FBufferStream.WriteByte(0);

  len:=Length(ATableName)+1; // Extra Null
  FBufferStream.WriteDWord(len);
  FBufferStream.Write(ATableName[1],Length(ATableName));
  FBufferStream.WriteByte(0);
  
  resu:=SendCommand(REPRELAY_ADDFILTER,0);
  if(resu=0) then
  Begin
    Result:=0;
    Exit;
  End;
  
  resu:=GetReply(FBufferStream);
  if(resu=$F0) then
  begin
    Result:=0;
    Exit;
  end;

  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'CommandReqFilter Result : '+IntToStr(resu));
  {$ENDIF}
  
  if(resu=0) then Result:=1;
end;
{******************************************************************************}
function TReplicationRelayClient.CommandPoll(): TQueueItem;
var
  resu,i: Integer;
  lQueueItem: TQueueItem;
begin
  Result:=nil;
  FBufferStream.Clear();
  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'CommandPoll');
  {$ENDIF}
  resu:=SendCommand(REPRELAY_POLL,0);
  if(resu=0) then
  Begin
    ClientDisconnected:=true;
    Exit;
  End;
  resu:=GetReply(FBufferStream);
  //AddToLog(llWarning,'CommandPoll:GotReply');
  if(resu=$F0) then
  begin
    AddToLog(llWarning,'Client Disconnected');
    ClientDisconnected:=true;
    Exit;
  end;
  
  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'CommandPoll Result : '+IntToStr(resu));
  {$ENDIF}
  if(resu=0) then
  begin
    lQueueItem:=TQueueItem.Create;
    
    {$IFDEF REPDEBUG}
    AddToLog(llWarning,'FBufferStream Length : '+IntToStr(FBufferStream.Size));
    {$ENDIF}
     
    FBufferStream.Seek(0, soFromBeginning);
    
    lQueueItem.EventType:=FBufferStream.ReadByte();
    lQueueItem.EventPosition:=FBufferStream.ReadQWord();
    lQueueItem.QueueSize:=FBufferStream.ReadDWord();
    lQueueItem.SchemaName:=PopStringFromStream(FBufferStream);
    lQueueItem.TableName:=PopStringFromStream(FBufferStream);
    i:=FBufferStream.ReadDWord();
    SetLength(lQueueItem.Cols,i);
    for i:=0 to Length(lQueueItem.Cols)-1 do
    begin
      lQueueItem.Cols[i].Name:=PopStringFromStream(FBufferStream);
      lQueueItem.Cols[i].Before:=PopStringFromStream(FBufferStream);
      lQueueItem.Cols[i].After:=PopStringFromStream(FBufferStream);
    end;
    
    Result:=lQueueItem;
  end;
end;
{******************************************************************************}
function TReplicationRelayClient.CommandPollV2(): TQueueItem;
var
  resu,i: Integer;
  lQueueItem: TQueueItem;
begin
  Result:=nil;
  FBufferStream.Clear();
  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'CommandPollV2');
  {$ENDIF}
  resu:=SendCommand(REPRELAY_POLLV2,0);
  if(resu=0) then
  Begin
    ClientDisconnected:=true;
    Exit;
  End;
  resu:=GetReply(FBufferStream);
  if(resu=$F0) then
  begin
    AddToLog(llWarning,'Client Disconnected');
    ClientDisconnected:=true;
    Exit;
  end;
  
  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'CommandPollV2 Result : '+IntToStr(resu));
  //AddToLog(llWarning,'CommandPoll Result : '+IntToStr(resu));
  {$ENDIF}
  if(resu=0) then
  begin
    lQueueItem:=TQueueItem.Create;
    
    {$IFDEF REPDEBUG}
    AddToLog(llWarning,'FBufferStream Length : '+IntToStr(FBufferStream.Size));
    {$ENDIF}
     
    FBufferStream.Seek(0, soFromBeginning);
    
    lQueueItem.EventType:=FBufferStream.ReadByte();
    lQueueItem.EventPosition:=FBufferStream.ReadQWord();
    lQueueItem.QueueSize:=FBufferStream.ReadDWord();
    lQueueItem.InstanceID:=FBufferStream.ReadDWord();
    lQueueItem.SchemaNameID:=FBufferStream.ReadDWord();
    lQueueItem.SchemaName:=PopStringFromStream(FBufferStream);
    lQueueItem.TableNameID:=FBufferStream.ReadDWord();
    lQueueItem.TableName:=PopStringFromStream(FBufferStream);
    i:=FBufferStream.ReadDWord();
    SetLength(lQueueItem.Cols,i);
    for i:=0 to Length(lQueueItem.Cols)-1 do
    begin
      lQueueItem.Cols[i].NameID:=FBufferStream.ReadDWord();
      lQueueItem.Cols[i].Name:=PopStringFromStream(FBufferStream);
      lQueueItem.Cols[i].Before:=PopStringFromStream(FBufferStream);
      lQueueItem.Cols[i].After:=PopStringFromStream(FBufferStream);
    end;
    
    Result:=lQueueItem;
  end;
end;
{******************************************************************************}
end.