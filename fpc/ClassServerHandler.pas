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
unit ClassServerHandler;

{$UNDEF REPDEBUG}

interface

uses
  Declarations, Classes, SysUtils, Contnrs, IdTCPServer, IdException,
  IdExceptionCore, IdCustomTCPServer, IdGlobal, IdYarn, IdTCPConnection,
  IdContext,ReplicationRelayStructures;

type

  TServerHandler = class(TIdServerContext)
  private
    FController: TObject;
    FReplyHeader: TReplicationReplyHeader;
    
  protected
  public
    FClientID: Integer;
    FCID: Integer;
    KillSwitch: Boolean;
  
    procedure Read;
    
    procedure SendReply(AResult: byte;AStream:TMemoryStream);
    procedure Fail(AStr: String);
        
    procedure HandlePing();
    procedure HandleAuth(ABuf: TIdBytes);
    procedure HandleAddFilter(AClientID: Integer; ABuf: TIdBytes);
    procedure HandlePoll(AClientID: Integer; ABuf: TIdBytes);
    procedure HandlePollV2(AClientID: Integer; ABuf: TIdBytes);
         
  published
    property Controller: TObject read FController write FController;
  end;

implementation

uses
  ClassDebug, IdIOHandler, ClassReplicationRelay;

{ TServerHandler }

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
procedure TServerHandler.SendReply(AResult: byte; AStream:TMemoryStream);
var
  i,lCheck:DWord;
  lStreamOut: TMemoryStream;
begin
  FReplyHeader.Result:=AResult;
  FReplyHeader.PayloadSize:=AStream.Size;
  lCheck:=FReplyHeader.PayloadSize;
  lCheck:=lCheck+AResult;
  AStream.Seek(0,soFromBeginning); 
  
  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'SendReply, Payload Size = '+IntToStr(AStream.Size));
  {$ENDIF}
  //AddToLog(llWarning,'Checksum header = '+IntToHex(lCheck,4));
  if(AStream.Size>0) then
    for i:=0 to AStream.Size-1 do
    begin
      lCheck:=lCheck+AStream.ReadByte();
      //AddToLog(llWarning,IntToStr(i)+' : Checksum in = '+IntToHex(lCheck,4));
    end;
  //AddToLog(llWarning,'Checksum final = '+IntToHex(lCheck,4));
  FReplyHeader.Checksum:=lCheck;  // +1 here to simulate checksum failure
  
  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'Send Reply : Result = '+IntToStr(AResult)+' : Size = '+IntToStr(FReplyHeader.PayloadSize)+' : Checksum = '+IntToStr(FReplyHeader.Checksum));
  {$ENDIF}

  (*
  // Bad as it triggers delayed ACK 21x/sec
  Connection.IOHandler.Write(RawToBytes(FReplyHeader,Sizeof(FReplyHeader))); 
  AStream.Seek(0,soFromBeginning);
  Connection.IOHandler.Write(AStream);
  Connection.IOHandler.WriteBufferClose;
  *)

  // Fix for Nagle's algorithm causing delay of 40ms sometimes on small packets (~21x/sec)
  
  lStreamOut:=TMemoryStream.Create;  
  AStream.Seek(0,soFromBeginning);
  lStreamOut.Write(FReplyHeader, Sizeof(FReplyHeader));
  lStreamOut.CopyFrom(AStream,AStream.Size);
  //lStreamOut.Write(AStream.Memory,AStream.Size);
  
  lStreamOut.Seek(0,soFromBeginning);
  Connection.IOHandler.Write(lStreamOut);
  Connection.IOHandler.WriteBufferClose;

  FreeAndNil(lStreamOut);    
  
end;
{******************************************************************************}
procedure TServerHandler.Fail(AStr: String);
var
  lStreamOut: TMemoryStream;
begin
  lStreamOut:=TMemoryStream.Create;
  
  lStreamOut.Write(AStr[1], Length(AStr));
  lStreamOut.WriteByte($00);
  
  SendReply($FF,lStreamOut);
  
  FreeAndNil(lStreamOut);  
end;
{******************************************************************************}
procedure TServerHandler.HandlePing();
var
  lStreamOut: TMemoryStream;
  lStr: String;
begin
  lStreamOut:=TMemoryStream.Create;
  
  lStr:='PONG';
  lStreamOut.Write(lStr[1], Length(lStr));
  lStreamOut.WriteByte($00);
  
  SendReply($00,lStreamOut);
  
  FreeAndNil(lStreamOut);  
end;
{******************************************************************************}
procedure TServerHandler.HandleAuth(ABuf: TIdBytes);
var
  lStreamOut: TMemoryStream;
  lPayload: TReplicationMessageAuth;
  i,resu: Integer;
begin  
  lStreamOut:=TMemoryStream.Create;
  {$notes off}
  BytesToRaw(ABuf,lPayload,8+4+4+4);
  {$notes on}
  lPayload.ClientName:='';
  for i:=0 to lPayload.ClientNameSize-1 do 
    lPayload.ClientName:=lPayload.ClientName+Char(ABuf[20+i]);
  
  resu:=TReplicationRelay(FController).RegisterClient(self,lPayload.ClientName,lPayload.AuthKey,lPayload.ClientToken);
  
  lPayload.ClientID:=resu;
  FClientID:=resu;
  
  lStreamOut.Write(lPayload,8+4+4);
  
  if(resu>0) then SendReply($00,lStreamOut)
  else SendReply($FF,lStreamOut);
  
  FreeAndNil(lStreamOut);  
end;
{******************************************************************************}
procedure TServerHandler.HandleAddFilter(AClientID: Integer; ABuf: TIdBytes);
var
  lStreamOut: TMemoryStream;
  lPayload: TReplicationMessageAddFilter;
  i,resu: Integer;
  offset: Integer;
begin  
  lStreamOut:=TMemoryStream.Create;
  
  {$IFDEF REPDEBUG}  
  for i:=0 to Length(ABuf)-1 do
    AddToLog(llWarning,'Byte #'+IntToStr(i)+' : '+IntToHex(Ord(ABuf[i]),2));
  {$ENDIF}
    
  //BytesToRaw(ABuf,lPayload,1+4);  

  offset:=0;
  
  lPayload.FilterType:=ABuf[offset];
  Inc(offset);
  
  lPayload.FilterDiscardType:=ABuf[offset];
  Inc(offset);
  
  lPayload.FilterQueueLimit:=BytesToLongInt(ABuf,offset);
  offset:=offset+4;

  lPayload.SchemaNameLen:=BytesToLongInt(ABuf,offset);
  offset:=offset+4;
  
  lPayload.SchemaName:='';
  for i:=0 to lPayload.SchemaNameLen-2 do 
    lPayload.SchemaName:=lPayload.SchemaName+Char(ABuf[offset+i]);
  
  offset:=offset+lPayload.SchemaNameLen;

  lPayload.TableNameLen:=BytesToLongInt(ABuf,offset);
  offset:=offset+4;
  
  lPayload.TableName:='';
  for i:=0 to lPayload.TableNameLen-2 do 
    lPayload.TableName:=lPayload.TableName+Char(ABuf[offset+i]);

  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'FilterType = '+IntToStr(lPayload.FilterType));
  AddToLog(llWarning,'SchemaNameLen = '+IntToStr(lPayload.SchemaNameLen));
  AddToLog(llWarning,'TableNameLen = '+IntToStr(lPayload.TableNameLen));
  AddToLog(llWarning,'FilterDiscardType = '+IntToStr(lPayload.FilterDiscardType));
  AddToLog(llWarning,'FilterQueueLimit = '+IntToStr(lPayload.FilterQueueLimit));
  {$ENDIF}
    
  //AddToLog(llWarning,'AddFilter '+IntToStr(lPayload.FilterType)+' on '+lPayload.SchemaName+'.'+lPayload.TableName+' (Client #'+IntToStr(AClientID)+')');
  
  resu:=TReplicationRelay(FController).AddFilter(AClientID,lPayload.FilterType,lPayload.SchemaName,lPayload.TableName,lPayload.FilterDiscardType,lPayload.FilterQueueLimit);
  
  if(resu>0) then SendReply($00,lStreamOut)
  else SendReply($FF,lStreamOut);
  
  FreeAndNil(lStreamOut);  
end;
{******************************************************************************}
procedure TServerHandler.HandlePoll(AClientID: Integer; ABuf: TIdBytes);
var
  lStreamOut: TMemoryStream;
  lItem: TQueueItem;
  i,resu: Integer;
begin  
  lStreamOut:=TMemoryStream.Create;
  
  //AddToLog(llWarning,'HandlePoll => In');
  
  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'HandlePoll, Client #'+IntToStr(AClientID));
  {$ENDIF}
  
  //AddToLog(llWarning,'HandlePoll => PollEvent');
  
  lItem:=TReplicationRelay(FController).PollEvent(AClientID);
  
  //AddToLog(llWarning,'HandlePoll => PolledEvent');
    
  if(lItem=nil) then SendReply($FC,lStreamOut)
  else
  begin
  //AddToLog(llWarning,'Got Item EID '+IntToStr(lItem.EventPosition)+', QueueSize '+IntToStr(lItem.QueueSize));
  //AddToLog(llWarning,'HandlePoll => Prep Reply');
  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'Got Item EID '+IntToStr(lItem.EventPosition));
  AddToLog(llWarning,'QueueSize '+IntToStr(lItem.QueueSize));
  {$ENDIF}
    lStreamOut.WriteByte(lItem.EventType);
    lStreamOut.WriteQWord(lItem.EventPosition);
    lStreamOut.WriteDWord(lItem.QueueSize);
    PushStringToStream(lStreamOut,lItem.SchemaName);
    PushStringToStream(lStreamOut,lItem.TableName);
    
    resu:=Length(lItem.Cols);
    lStreamOut.WriteDWord(resu);
    for i:=0 to resu-1 do
    begin
      PushStringToStream(lStreamOut,lItem.Cols[i].Name);
      PushStringToStream(lStreamOut,lItem.Cols[i].Before);
      PushStringToStream(lStreamOut,lItem.Cols[i].After);
    end;
  
    //AddToLog(llWarning,'SendReply');
    
    
    SendReply($00,lStreamOut);
    
    
    //AddToLog(llWarning,'Sentreply');
    
    FreeAndNil(lItem); 
  end;
  //AddToLog(llWarning,'HandlePoll => Out');
  lStreamOut.Clear;
  FreeAndNil(lStreamOut);  
end;
{******************************************************************************}
procedure TServerHandler.HandlePollV2(AClientID: Integer; ABuf: TIdBytes);
var
  lStreamOut: TMemoryStream;
  lItem: TQueueItem;
  i,resu: Integer;
begin  
  lStreamOut:=TMemoryStream.Create;
  
  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'HandlePollV2, Client #'+IntToStr(AClientID));
  {$ENDIF}
  
  lItem:=TReplicationRelay(FController).PollEventV2(AClientID);
    
  if(lItem=nil) then SendReply($FC,lStreamOut)
  else
  begin
  //AddToLog(llWarning,'Got Item EID '+IntToStr(lItem.EventPosition)+', QueueSize '+IntToStr(lItem.QueueSize));

  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'Got Item EID '+IntToStr(lItem.EventPosition));
  AddToLog(llWarning,'QueueSize '+IntToStr(lItem.QueueSize));
  {$ENDIF}
    lStreamOut.WriteByte(lItem.EventType);
    lStreamOut.WriteQWord(lItem.EventPosition);
    lStreamOut.WriteDWord(lItem.QueueSize);
    lStreamOut.WriteDWord(lItem.InstanceID);
    lStreamOut.WriteDWord(lItem.SchemaNameID);
    PushStringToStream(lStreamOut,lItem.SchemaName);
    lStreamOut.WriteDWord(lItem.TableNameID);
    PushStringToStream(lStreamOut,lItem.TableName);
    
    resu:=Length(lItem.Cols);
    lStreamOut.WriteDWord(resu);
    for i:=0 to resu-1 do
    begin
      lStreamOut.WriteDWord(lItem.Cols[i].NameID);
      PushStringToStream(lStreamOut,lItem.Cols[i].Name);
      PushStringToStream(lStreamOut,lItem.Cols[i].Before);
      PushStringToStream(lStreamOut,lItem.Cols[i].After);
    end;
  
    //AddToLog(llWarning,'SendReply');
    
    SendReply($00,lStreamOut);
    
    //AddToLog(llWarning,'Sentreply');
    
    FreeAndNil(lItem); 
  end;
  lStreamOut.Clear;
  FreeAndNil(lStreamOut);  
end;
{******************************************************************************}
procedure TServerHandler.Read;
var
 i: Integer;
 lHeader: TReplicationHeader; 
 lBufHdr: TIdBytes;
 lBuf: TIdBytes;
 lCheck: DWord;
begin
  //AddToLog(llWarning,'TServerHandler.Read FCID '+IntToStr(FCID));
  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'TServerHandler.Read');
  {$ENDIF}
  //AddToLog(llWarning,'TServerHandler.Read');
  try
    Connection.IOHandler.ReadTimeout := 1000;

    if(Killswitch) then 
    begin
      AddToLog(llWarning,'TServerHandler => Killswitch');
      Connection.Disconnect;
      Sleep(1000);
      Exit;
    end;

    if not Connection.Connected then
      Exit;

    if Connection.Connected then
    begin     

    
      Connection.IOHandler.ReadBytes(lBufHdr,Sizeof(TReplicationHeader));
  {$notes off}      
      BytesToRaw(lBufHdr,lHeader,Sizeof(TReplicationHeader)); 
  {$notes on}      

//      AddToLog(llWarning,'TServerHandler.HeaderPost (Len = '+IntToStr(Sizeof(TReplicationHeader))+')');
      
 
      {$IFDEF REPDEBUG}
      AddToLog(llWarning,'Header : ');
      AddToLog(llWarning,'=> Command     : '+IntToHex(lHeader.Command,4));
      AddToLog(llWarning,'=> Subcommand  : '+IntToHex(lHeader.Subcommand,4));
      AddToLog(llWarning,'=> PayloadSize : '+IntToHex(lHeader.PayloadSize,8));
      AddToLog(llWarning,'=> Version     : '+IntToHex(lHeader.Version,8));
      AddToLog(llWarning,'=> ClientID    : '+IntToHex(lHeader.ClientID,8));
      AddToLog(llWarning,'=> ClientToken : '+IntToHex(lHeader.ClientToken,8));
      AddToLog(llWarning,'=> Checksum    : '+IntToHex(lHeader.Checksum,8));
      AddToLog(llWarning,'=> Reserved1   : '+IntToHex(lHeader.Reserved1,8));
      AddToLog(llWarning,'=> Reserved2   : '+IntToHex(lHeader.Reserved2,8));
      {$ENDIF}
      
      lCheck:=lHeader.Command+lHeader.Subcommand+lHeader.PayloadSize+lHeader.Version+lHeader.ClientID+lHeader.CLientToken+lHeader.Reserved1+lHeader.Reserved2;
    
      {$IFDEF REPDEBUG}
      AddToLog(llWarning,'Header Checksum '+IntToHex(lCheck,8));
      {$ENDIF}

    
      // Load Payload (if any)
      if(lHeader.PayloadSize>0) then
      begin     
        Connection.IOHandler.ReadBytes(lBuf,lHeader.PayloadSize); 
        for i:=0 to lHeader.PayloadSize-1 do
          lCheck:=lCheck+lBuf[i];
      end;

      //AddToLog(llWarning,'TServerHandler.ReadPayload');      
      
      {$IFDEF REPDEBUG}
      AddToLog(llWarning,'Final Checksum '+IntToHex(lCheck,8));
      {$ENDIF}
      
      // Checksum failed?
      if(lHeader.Checksum<>lCheck) then
      begin
        AddToLog(llWarning,'Packet Rejected, Invalid Checksum '+IntToHex(lHeader.Checksum,8)+' <> '+IntToHex(lCheck,8));        
        Fail('Invalid Checksum');
        Connection.Disconnect;
        Exit;
      end;
      
      {$IFDEF REPDEBUG}
      AddToLog(llWarning,'Command Received : '+IntToHex(lHeader.Command,2));
      {$ENDIF}
      
     // AddToLog(llWarning,'TServerHandler.ReadProcessCommand');
      
      // Process commands        
      case lHeader.Command of
        REPRELAY_PING:
        begin
          HandlePing();
          // Connection.Disconnect;
        end;
        REPRELAY_AUTH:
        begin
          HandleAuth(lBuf);
        end;
        REPRELAY_ADDFILTER:
        begin
          HandleAddFilter(lHeader.ClientID,lBuf);
        end;
        REPRELAY_POLL:
        begin
          HandlePoll(lHeader.ClientID,lBuf);
        end;
        REPRELAY_POLLV2:
        begin
          HandlePollV2(lHeader.ClientID,lBuf);
        end        
        else
        begin
          AddToLog(llWarning,'Unhandled command '+IntToHex(lHeader.Command,2));
        end;
      end; 
  
    end;
  //AddToLog(llWarning,'TServerHandler.ReadDone');    
  except
    on E: EIdConnClosedGracefully do
    begin
      AddToLog(llDebug, 'TServerHandler::Read => Closed Gracefully');
      Raise Exception.Create('Closed Gracefully => Die please');
      //if Connection.IOHandler <> nil then
      //  Connection.IOHandler.DiscardAll;            
    end;

    on E: EIdReadTimeout do
    begin
      // Do nothing, thread might be alive but doing nothing
    end;

    on E:Exception do
    begin
      AddToLog(llError, 'TServerHandler::Read ('+IntToStr(FClientID)+') => '+E.Message);
      Raise Exception.Create('Exception => Die please ('+E.Message+')');
      //Connection.Disconnect;
    end;
  end;
end;
{******************************************************************************}

end.

