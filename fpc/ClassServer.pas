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

unit ClassServer;

{$IFDEF FPC}
  {$MODE Delphi}
{$ENDIF}

interface

uses
  Declarations, Classes, SysUtils, Contnrs, IdTCPServer, IdContext;

type
  TServer = class
  protected
    FServer: TIdTCPServer;
    FStarted: TDateTime;
    FRelay: TObject;
    FConnexionIndex: Integer;
  public
    constructor Create; 
    destructor Destroy; override;

    procedure Start(APort: Integer);

    procedure DoOnConnect(AContext: TIdContext);
    procedure DoOnDisconnect(AContext: TIdContext);
    procedure DoOnExecute(AContext: TIdContext);
    procedure DoOnException(AContext: TIdContext; AException: Exception);
    
    procedure Check;
    property Relay: TObject read FRelay write FRelay;
    property Server: TIdTCPServer read FServer write FServer;
    
  end;

var
  GServer: TServer;


implementation

uses
  ClassDebug, ClassCompat, IdIOHandlerSocket, ClassReplicationRelay, ClassServerHandler,
  DateUtils;

{ TPredictServer }

{******************************************************************************}
constructor TServer.Create;
begin
  FConnexionIndex:=0;
end;
{******************************************************************************}
destructor TServer.Destroy;
begin
  FreeAndNil(FServer);
  inherited;
end;
{******************************************************************************}
procedure TServer.Start(APort: Integer);
begin
  AddToLog(llWarning, 'Server is initializing');
  FServer := TIdTCPServer.Create(nil);
  FServer.Active := false;
  FServer.ContextClass := TServerHandler;
  FServer.Bindings.DefaultPort := APort;
  FServer.OnExecute := DoOnExecute;
  FServer.OnConnect := DoOnConnect;
  FServer.OnDisconnect := DoOnDisconnect;
  FServer.OnException := DoOnException;
  FServer.MaxConnections := 0;
  FServer.Active := true;
end; 
{******************************************************************************}
procedure TServer.DoOnConnect(AContext: TIdContext);
begin
  Inc(FConnexionIndex);
  
  {$IFDEF REPDEBUG}
  AddToLog(llWarning, 'TServer::DoOnConnect => Connection opened from ' + TIdIOHandlerSocket(AContext.Connection.IOHandler).Binding.PeerIP+', FConnexionIndex = '+IntToStr(FConnexionIndex));
  {$ENDIF}
  TServerHandler(AContext).FClientID:=0;
  TServerHandler(AContext).FCID:=FConnexionIndex;
  TServerHandler(AContext).KillSwitch:=false;
  TServerHandler(AContext).Controller := Relay;    
  
end;
{******************************************************************************}
procedure TServer.DoOnDisconnect(AContext: TIdContext);
begin
  {$IFDEF REPDEBUG}
  AddToLog(llWarning, 'TServer::DoOnDisconnect => Connection lost from ' + TIdIOHandlerSocket(AContext.Connection.IOHandler).Binding.PeerIP+', FConnexionIndex = '+IntToStr(TServerHandler(AContext).FCID));
  {$ENDIF}
  if(TServerHandler(AContext).FClientID>0) then TReplicationRelay(Relay).FClients[TServerHandler(AContext).FClientID].ConnectionStatus:=0;
end;
{******************************************************************************}
procedure TServer.DoOnException(AContext: TIdContext;
  AException: Exception);
begin
  AddToLog(llError, 'DoOnException : Exception: '+AException.Message);
  if(TServerHandler(AContext).FClientID>0) then TReplicationRelay(Relay).FClients[TServerHandler(AContext).FClientID].ConnectionStatus:=2;
end;
{******************************************************************************}
procedure TServer.DoOnExecute(AContext: TIdContext);
begin
  {$IFDEF REPDEBUG}
  AddToLog(llWarning, 'TServer::DoOnExecute'+', FConnexionIndex = '+IntToStr(TServerHandler(AContext).FCID));
  {$ENDIF}
  try
    if(not AContext.Connection.IOHandler.InputBufferIsEmpty) then TServerHandler(AContext).Read;
    Sleep(0);
  except
    on E:Exception do
    begin
      AddToLog(llError, 'TServer::DoOnExecute => E:'+E.Message+', FConnexionIndex = '+IntToStr(TServerHandler(AContext).FCID));
      Raise Exception.Create('Die please');
    end;
  end;
end;
{******************************************************************************}
procedure TServer.Check;
begin
  if not FServer.Active then
  begin
    AddToLog(llError, 'Server dead? Attempt to recover');
    FServer.Active := true;
  end;
end;
{******************************************************************************}

end.
