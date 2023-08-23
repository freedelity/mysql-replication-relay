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
unit ReplicationRelayStructures;

{$IFDEF FPC}
  {$MODE Delphi}
{$ENDIF}

interface

uses
	SysUtils, Classes;

const
  REPRELAY_VERSION=$00010020;


  REPRELAYREQ_INSERT=$01;
  REPRELAYREQ_UPDATE=$02;
  REPRELAYREQ_DELETE=$04;
  
  
  REPRELAY_PING=$01;
  REPRELAY_AUTH=$02;
  REPRELAY_ADDFILTER=$03;
  REPRELAY_POLL=$04;
  REPRELAY_POLLV2=$05;
  
  REPRELAY_DISCARD_NODISCARD=$00;
  REPRELAY_DISCARD_UNQUEUEOLDEST=$01;
  REPRELAY_DISCARD_IGNORENEWEST=$02;


// =============================================================
// Relay Protocol
//
// Client sends command : TReplicationHeader + Payload 
// Server replies : TReplicationReplyHeader + Payload
//
// Admin messages identified by ClientID + ClientToken = Admin Hash (QWORD)
  
type
  TReplicationHeader = packed record
    Command: Word;
    Subcommand: Word;
    PayloadSize: DWord;
    Version: DWord;
    ClientID: DWord;
    ClientToken: DWord;
    Checksum: DWord;
    Reserved1: DWord;
    Reserved2: DWord;    
   end;

  TReplicationReplyHeader = packed record
    Result: Byte;
    PayloadSize: DWord;
    Checksum: DWord;
   end; 

  TReplicationMessageAuth = packed record
    AuthKey: QWord;    
    ClientToken: DWord;
    ClientId: DWord;
    // Following part in request only
    ClientNameSize: DWord;
    ClientName: String;
  end;

  TReplicationMessageAddFilter = packed record
    FilterType: Byte;
    FilterDiscardType: Byte;
    FilterQueueLimit: Integer;
    SchemaNameLen: DWord;
    SchemaName: String;
    TableNameLen: DWord;
    TableName: String;
  end;
  
// =============================  
  
  TQueueCol = object
    NameID: DWord;
    Name: String;
    Before: String;
    After: String;
  end;

  TQueueItem = class
    EventType: Byte;
    EventPosition: QWord;
    QueueSize: Integer;
    InstanceID: DWord;
    SchemaNameID: DWord;
    SchemaName: String;
    TableNameID: DWord;
    TableName: String;
    Cols: Array of TQueueCol;  
    
    destructor Destroy; override;
  end;
  
implementation

destructor TQueueItem.Destroy;
begin
  SetLength(Cols,0);  // DIDN'T fixed any leak
  Inherited;
end;

end.
