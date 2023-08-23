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
program relayclient;

{$IFDEF FPC}
  {$MODE Delphi}
{$ENDIF}


uses
  {$IFDEF FPC}
  CThreads,
  {$ENDIF}
  Declarations in 'Declarations.pas',
  ReplicationRelayStructures in 'ReplicationRelayStructures.pas',
  HFNV1A in 'HFNV1A.pas',
  ClassReplicationRelayClient in 'ClassReplicationRelayClient.pas',
  ClassServer in 'ClassServer.pas',
  ClassServerHandler in 'ClassServerHandler.pas',
  ClassReplicationRelay in 'ClassReplicationRelay.pas',
  ClassCompat in '.\ClassCompat.pas',
  ClassDebug in '.\ClassDebug.pas',
  ClassMySQL in '.\ClassMySQL.pas',
  
  IdThreadSafe in '..\..\Components\Indy10.5\Lib\Core\IdThreadSafe.pas',
  IdIDN in '..\..\Components\Indy10.5\Lib\System\IdIDN.pas',
  IdGlobal in '..\..\Components\Indy10.5\Lib\System\IdGlobal.pas',
  IdStream in '..\..\Components\Indy10.5\Lib\System\IdStream.pas',
  IdStreamVCL in '..\..\Components\Indy10.5\Lib\System\IdStreamVCL.pas',
  IdResourceStrings in '..\..\Components\Indy10.5\Lib\System\IdResourceStrings.pas',
  IdException in '..\..\Components\Indy10.5\Lib\System\IdException.pas',
  IdBaseComponent in '..\..\Components\Indy10.5\Lib\System\IdBaseComponent.pas',
  IdTCPServer in '..\..\Components\Indy10.5\Lib\Core\IdTCPServer.pas',
  IdCustomTCPServer in '..\..\Components\Indy10.5\Lib\Core\IdCustomTCPServer.pas',
  IdContext in '..\..\Components\Indy10.5\Lib\Core\IdContext.pas',
  IdComponent in '..\..\Components\Indy10.5\Lib\System\IdComponent.pas',
  IdStack in '..\..\Components\Indy10.5\Lib\System\IdStack.pas',
  IdStackConsts in '..\..\Components\Indy10.5\Lib\System\IdStackConsts.pas',
  IdStackBSDBase in '..\..\Components\Indy10.5\Lib\System\IdStackBSDBase.pas',
  IdSocketHandle in '..\..\Components\Indy10.5\Lib\Core\IdSocketHandle.pas',
  IdAntiFreezeBase in '..\..\Components\Indy10.5\Lib\System\IdAntiFreezeBase.pas',
  IdTCPConnection in '..\..\Components\Indy10.5\Lib\Core\IdTCPConnection.pas',
  IdExceptionCore in '..\..\Components\Indy10.5\Lib\Core\IdExceptionCore.pas',
  IdIntercept in '..\..\Components\Indy10.5\Lib\Core\IdIntercept.pas',
  IdResourceStringsCore in '..\..\Components\Indy10.5\Lib\Core\IdResourceStringsCore.pas',
  IdBuffer in '..\..\Components\Indy10.5\Lib\Core\IdBuffer.pas',
  IdIOHandler in '..\..\Components\Indy10.5\Lib\Core\IdIOHandler.pas',
  IdIOHandlerSocket in '..\..\Components\Indy10.5\Lib\Core\IdIOHandlerSocket.pas',
  IdCustomTransparentProxy in '..\..\Components\Indy10.5\Lib\Core\IdCustomTransparentProxy.pas',
  IdSocks in '..\..\Components\Indy10.5\Lib\Core\IdSocks.pas',
  IdAssignedNumbers in '..\..\Components\Indy10.5\Lib\Core\IdAssignedNumbers.pas',
  IdIPAddress in '..\..\Components\Indy10.5\Lib\Core\IdIPAddress.pas',
  IdTCPClient in '..\..\Components\Indy10.5\Lib\Core\IdTCPClient.pas',
  IdIOHandlerStack in '..\..\Components\Indy10.5\Lib\Core\IdIOHandlerStack.pas',
  IdReply in '..\..\Components\Indy10.5\Lib\Core\IdReply.pas',
  IdReplyRFC in '..\..\Components\Indy10.5\Lib\Core\IdReplyRFC.pas',
  IdScheduler in '..\..\Components\Indy10.5\Lib\Core\IdScheduler.pas',
  IdThread in '..\..\Components\Indy10.5\Lib\Core\IdThread.pas',
  IdSchedulerOfThread in '..\..\Components\Indy10.5\Lib\Core\IdSchedulerOfThread.pas',
  IdServerIOHandler in '..\..\Components\Indy10.5\Lib\Core\IdServerIOHandler.pas',
  IdServerIOHandlerStack in '..\..\Components\Indy10.5\Lib\Core\IdServerIOHandlerStack.pas',
  IdServerIOHandlerSocket in '..\..\Components\Indy10.5\Lib\Core\IdServerIOHandlerSocket.pas',
  IdGlobalCore in '..\..\Components\Indy10.5\Lib\Core\IdGlobalCore.pas',
  IdSchedulerOfThreadDefault in '..\..\Components\Indy10.5\Lib\Core\IdSchedulerOfThreadDefault.pas',
  IdHTTP in '..\..\Components\Indy10.5\Lib\Protocols\IdHTTP.pas', 
  IdHTTPServer in '..\..\Components\Indy10.5\Lib\Protocols\IdHTTPServer.pas',  
  IdCustomHTTPServer in '..\..\Components\Indy10.5\Lib\Protocols\IdCustomHTTPServer.pas',  
  IdHeaderList in '..\..\Components\Indy10.5\Lib\Protocols\IdHeaderList.pas',
  IdGlobalProtocols in '..\..\Components\Indy10.5\Lib\Protocols\IdGlobalProtocols.pas',
  IdCharsets in '..\..\Components\Indy10.5\Lib\Protocols\IdCharsets.pas',
  IdResourceStringsProtocols in '..\..\Components\Indy10.5\Lib\Protocols\IdResourceStringsProtocols.pas',
  IdHTTPHeaderInfo in '..\..\Components\Indy10.5\Lib\Protocols\IdHTTPHeaderInfo.pas',
  IdAuthentication in '..\..\Components\Indy10.5\Lib\Protocols\IdAuthentication.pas',
  IdCoderMIME in '..\..\Components\Indy10.5\Lib\Protocols\IdCoderMIME.pas',
  IdCoder3to4 in '..\..\Components\Indy10.5\Lib\Protocols\IdCoder3to4.pas',
  IdCoder in '..\..\Components\Indy10.5\Lib\Protocols\IdCoder.pas',
  IdSSL in '..\..\Components\Indy10.5\Lib\Protocols\IdSSL.pas',
  IdZLibCompressorBase in '..\..\Components\Indy10.5\Lib\Protocols\IdZLibCompressorBase.pas',
  IdURI in '..\..\Components\Indy10.5\Lib\Protocols\IdURI.pas',
  IdUriUtils in '..\..\Components\Indy10.5\Lib\Protocols\IdUriUtils.pas',
  IdResourceStringsUriUtils in '..\..\Components\Indy10.5\Lib\Protocols\IdResourceStringsUriUtils.pas',
  IdCookie in '..\..\Components\Indy10.5\Lib\Protocols\IdCookie.pas',
  IdCookieManager in '..\..\Components\Indy10.5\Lib\Protocols\IdCookieManager.pas',
  IdAuthenticationManager in '..\..\Components\Indy10.5\Lib\Protocols\IdAuthenticationManager.pas',
  IdMultipartFormData in '..\..\Components\Indy10.5\Lib\Protocols\IdMultipartFormData.pas',
  IdCoderHeader in '..\..\Components\Indy10.5\Lib\Protocols\IdCoderHeader.pas',
  IdEMailAddress in '..\..\Components\Indy10.5\Lib\Protocols\IdEMailAddress.pas',
  IdHeaderCoderBase in '..\..\Components\Indy10.5\Lib\Protocols\IdHeaderCoderBase.pas',
  IdAllHeaderCoders in '..\..\Components\Indy10.5\Lib\Protocols\IdAllHeaderCoders.pas',
  IdHeaderCoderPlain in '..\..\Components\Indy10.5\Lib\Protocols\IdHeaderCoderPlain.pas',
  IdHeaderCoder2022JP in '..\..\Components\Indy10.5\Lib\Protocols\IdHeaderCoder2022JP.pas',
  IdHeaderCoderIndy in '..\..\Components\Indy10.5\Lib\Protocols\IdHeaderCoderIndy.pas',
  IdCoderQuotedPrintable in '..\..\Components\Indy10.5\Lib\Protocols\IdCoderQuotedPrintable.pas',
  IdAllAuthentications in '..\..\Components\Indy10.5\Lib\Protocols\IdAllAuthentications.pas',
  IdAuthenticationNTLM in '..\..\Components\Indy10.5\Lib\Protocols\IdAuthenticationNTLM.pas',
  IdResourceStringsOpenSSL in '..\..\Components\Indy10.5\Lib\Protocols\IdResourceStringsOpenSSL.pas',
  IdSSLOpenSSLHeaders in '..\..\Components\Indy10.5\Lib\Protocols\IdSSLOpenSSLHeaders.pas',
  IdCTypes in '..\..\Components\Indy10.5\Lib\System\IdCTypes.pas',
  IdFIPS in '..\..\Components\Indy10.5\Lib\Protocols\IdFIPS.pas',
  IdSSLOpenSSL in '..\..\Components\Indy10.5\Lib\Protocols\IdSSLOpenSSL.pas',
  IdNTLM in '..\..\Components\Indy10.5\Lib\Protocols\IdNTLM.pas',
  IdStruct in '..\..\Components\Indy10.5\Lib\System\IdStruct.pas',
  IdHash in '..\..\Components\Indy10.5\Lib\Protocols\IdHash.pas',
  IdHashMessageDigest in '..\..\Components\Indy10.5\Lib\Protocols\IdHashMessageDigest.pas',
  IdResourceStringsSSPI in '..\..\Components\Indy10.5\Lib\Protocols\IdResourceStringsSSPI.pas',
  IdAuthenticationDigest in '..\..\Components\Indy10.5\Lib\Protocols\IdAuthenticationDigest.pas',
  IdDNSResolver in '..\..\Components\Indy10.5\Lib\Protocols\IdDNSResolver.pas',
  IdNetworkCalculator in '..\..\Components\Indy10.5\Lib\Protocols\IdNetworkCalculator.pas',
  IdDNSCommon in '..\..\Components\Indy10.5\Lib\Protocols\IdDNSCommon.pas',
  IdContainers in '..\..\Components\Indy10.5\Lib\Protocols\IdContainers.pas',
  IdUDPClient in '..\..\Components\Indy10.5\Lib\Core\IdUDPClient.pas',
  IdUDPBase in '..\..\Components\Indy10.5\Lib\Core\IdUDPBase.pas',
  {$IFDEF MSWINDOWS}
  IdStackWindows in '..\..\Components\Indy10.5\Lib\System\IdStackWindows.pas',
  IdWinsock2 in '..\..\Components\Indy10.5\Lib\System\IdWinsock2.pas',
  IdWship6 in '..\..\Components\Indy10.5\Lib\System\IdWship6.pas',
  IdAuthenticationSSPI in '..\..\Components\Indy10.5\Lib\Protocols\IdAuthenticationSSPI.pas',
  IdSSPI in '..\..\Components\Indy10.5\Lib\Protocols\IdSSPI.pas',
  {$ELSE}
  IdResourceStringsUnix in '..\..\Components\Indy10.5\Lib\System\IdResourceStringsUnix.pas',
  IdStackUnix in '..\..\Components\Indy10.5\Lib\System\IdStackUnix.pas',
  {$ENDIF}
  IdTask in '..\..\Components\Indy10.5\Lib\Core\IdTask.pas',
  IdYarn in '..\..\Components\Indy10.5\Lib\Core\IdYarn.pas',
  IdHashSHA1 in '..\..\Components\Indy10.5\Lib\Protocols\IdHashSHA1.pas',
  
  MySQL in '.\MySQL.pas',
  Inifiles,
  SysUtils;

var
  GRelayClient: TReplicationRelayClient;
  lItem: TQueueItem;
  resu,i,j: Integer;
  mypid: Integer;
  FOptions: TIniFile;
begin
  mypid:=GetProcessID();
  
  // Open/Create ini file
  //
  // Format:
  //
  // [Client]
  // ServerAddr = 127.0.0.1 // Address of replication relay
  // ServerPort = 6001      // Port of replication relay
  // AuthSecret = same_auth_secret_as_replication_relay
  FOptions := TIniFile.Create('./relayclient.conf');
  
  GDebugPath := '/var/log/relayclient/';
  GDebugPrefix := 'relay-client-'+IntToStr(mypid);
  CreatePidFile('relay-client-'+IntToStr(mypid));

  AddToLog(llWarning, 'Starting Relay Client Test Program');
  libmysql_load(nil);

  // Create a TReplicationRelayClient object
  GRelayClient:=TReplicationRelayClient.Create;
  
  // Connect the Client to the replication relay
  resu:=GRelayClient.Connect(FOptions.ReadString('Client','ServerAddr', ''),StrToInt(FOptions.ReadString('Client','ServerPort', '')));
  AddToLog(llWarning,'Relay Client Connection result : '+IntToStr(resu));
  if(resu=0) then Exit; 
  
  // Authenticate the client on the server
  resu:=GRelayClient.CommandAuth('test-test',FNV1AQ(FOptions.ReadString('Client','AuthSecret', '')));  
  AddToLog(llWarning,'Relay Client Auth result : '+IntToStr(resu));
  if(resu=0) then Exit;
   
  // Add Requested Filters to the client
  // Add a request for inserts and updates made on "test" table in "replicate" database, with a queue of maximum 32768 item, discarding oldest items if the queue is full
  resu:=GRelayClient.CommandReqFilter(REPRELAYREQ_INSERT OR REPRELAYREQ_UPDATE,'replicate','test',REPRELAY_DISCARD_UNQUEUEOLDEST,32768); 
  AddToLog(llWarning,'Relay Client ReqFilter result : '+IntToStr(resu));
  if(resu=0) then Exit;
     
  // At this point the client is ready for polling events  
  AddToLog(llWarning,'Relay Client : Polling Events');
  
  while (true) do
  begin    
    AddToLog(llWarning,'Poll');
    // CommandPoll: will return Nil if the queue is empty, otherwise will return a mysql event (according to the filter set earlier)
    lItem:=GRelayClient.CommandPoll();
    AddToLog(llWarning,'Polled');
    
    if(lItem<>nil) then
    begin
      if(lItem.EventType=REPRELAYREQ_INSERT) then
      begin
        // Display detail of INSERT Event for test, process would have to be done here
        AddToLog(llWarning,'INSERT '+IntToStr(lItem.EventType)+' on '+lItem.SchemaName+'.'+lItem.TableName);
        for i:=0 to Length(lItem.Cols)-1 do
        begin
          AddToLog(llWarning,'> '+lItem.Cols[i].Name+' = '+lItem.Cols[i].After);
        end;
      end
      else if(lItem.EventType=REPRELAYREQ_UPDATE) then 
      begin
        // Display detail of UPDATE Event for test, process would have to be done here
        AddToLog(llWarning,'UPDATE '+IntToStr(lItem.EventType)+' on '+lItem.SchemaName+'.'+lItem.TableName);
        for i:=0 to Length(lItem.Cols)-1 do
        begin
          AddToLog(llWarning,'> '+lItem.Cols[i].Name+' : '+lItem.Cols[i].Before+' => '+lItem.Cols[i].After);
        end;
      end
      else if(lItem.EventType=REPRELAYREQ_DELETE) then
      begin
        // Display detail of DELETE Event for test, process would have to be done here
        AddToLog(llWarning,'DELETE '+IntToStr(lItem.EventType)+' on '+lItem.SchemaName+'.'+lItem.TableName);
        for i:=0 to Length(lItem.Cols)-1 do
        begin
          AddToLog(llWarning,'> '+lItem.Cols[i].Name+' = '+lItem.Cols[i].Before);
        end;
      end;
      
      // Important: lItem has to be freed by client
      FreeAndNil(lItem);
    end 
    else 
    begin
      // Check for a disconnection event
      if(GRelayClient.ClientDisconnected) then
      begin
        AddToLog(llWarning,'Client Disconnected, Goodbye!');
        GRelayClient.Free;
        Halt;
      end;
      Sleep(1000);
    end;
    
  end;
    
  GRelayClient.Free;
end.