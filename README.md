**mysql-replication-relay** is an application that acts as a relay between a mysql/mariadb database and client applications that wish to receive specific database events on specific tables.
  
It is composed of a mysql/mariadb compatible replication client (binlogs), a queue system that will store database events for each client, and a server that communicates with the clients.
  
Clients are able to request specific events like Insert, Update or Delete on specific database table.
  
The replication relay is written in Free Pascal. A Client library + example is provided in Free Pascal as well, as well as a php implementation of the client protocol.
  
**Notes:**
- The implemented binlog decoding supports only `WRITE_ROWS_EVENTv1`, `UPDATE_ROWS_EVENTv1` and `DELETE_ROWS_EVENTv1` (mysql documents 3 versions : `v0`, `v1` and `v2`, we have not yet encountered any situation with other version than `v1`)
- The application may need some modifications for compatibility outside of a Linux environment.

# How to compile

## Replication relay

The replication relay requires :

- Installing the Free Pascal Compiler and the Lazarus package (https://wiki.freepascal.org/Installing_the_Free_Pascal_Compiler)
- The library Indy 10.5+ (https://www.indyproject.org/download/)
- mysql-dev or mariadb-dev library to be installed on the system.
  
1. The file [replicaterelay.dpr](fpc/replicaterelay.dpr) needs to be modified to adjust the paths to the location where Indy is located (`..\..\Components\Indy10.5` has to be changed to the path where Indy is located)
    
2. The following lines need to be modified as well:

   ```    
   FOptions := TIniFile.Create('./replicaterelay.conf');
   ```
   => Specify the path and file name of the configuration file (sample provided)

   ```
   GDebugPath := '/var/log/replicaterelay/';
   ```
   => Path where log file will be created

3. Compilation is done with the command : `fpc replicaterelay.dpr -MDelphi`
    
   Note: a modification should be done in Indy to disable the TCP/IP Nagle's algorithm, otherwise polling from client will be limited to ~25 request / second

   Add the following lines in **Indy10.5/Lib/System/IdStackUnix.pas**:

   ```
   SetSocketOption(Result, Id_SOL_TCP, TCP_NODELAY, 1);
   SetSocketOption(ASocket, Id_SOL_TCP, TCP_NODELAY, 1);
   ```

## Sample client

A sample client application is also provided ([relayclient.dpr](fpc/relayclient.dpr))
    
The following lines need to be modified before compilation :
            
- `FOptions := TIniFile.Create('./relayclient.conf');`

  Specify the path and file name of the configuration file (sample provided)
  
- `GDebugPath := '/var/log/relayclient/';`

  Path where log file will be created
        
Compilation is done with the command : `fpc relayclient.dpr -MDelphi`

# How to use

## Replication relay

- Replication must be setup on the mysql/mariadb server the same way as regular replication
   
- A regular access must be given to the replication relay as well (with full access, for schema query)
   
- The config file must be set up correctly:
  
  ```
  [MySQL]
  # MySQL access for schema
  host=localhost
  login=root
  password=mypassword
  database=mydatabase

  [Server]
  # Mysql configuration for binlog (replication client)
  IPAddr=localhost
  Login=root
  Password=mypassword
  Database=mydatabase

  # Shared secret for client access
  AuthSecret=Freedelity
  # Admin secret
  AdminSecret=AdminPassword
  # Port where the clients will connect to
  ServerPort=6002
  # IP/Port for http monitoring page
  HTTPAddress=0.0.0.0
  HTTPPort=6081

  # replication logger : name of database and eventually list of tables to exclude from logging
  [Logger]
  Schema=mydatabase
  Excludes=table_to_exclude1,table_to_exclude2
  ```

## Monitoring service

The replication relay has a monitoring page (http) which gives the following information :
    
- Server information : Various global counts about number of events received and current binlog position
- Clients information : List of clients connected to the replication relay, list of filters, number of events in queue, maximum queue size seen, number of event served/discarded and the list of the last 10 unqueued events and the delay inbetween
- Table usage : This give very helpful information about activity on the database (independant from clients), for each table of each database, according to the binlogs it will give the following information:
  - Total Insert/Update/Delete seen since server start
  - Total Insert/Update/Delete in the last rolling 24 hours
  - Total Insert/Update/Delete in the last rolling hour
- Schema information : Technical detail about the schemas of the databases

about the server status, the queues status, activity seen in 

## Logger

The section `[Logger]` in the config file refers to a logger, it will log in csv format each binlog event seen for the specified database (`Schema`) (It only supports one database at the moment).

It also allows to exclude particular tables from the log, the list of table names, comma separated, can be specified in the `Excludes` parameter

## Making a replication client

Refer to the example program [relayclient.dpr](fpc/relayclient.dpr) which comments each steps for using the client library.

# The protocol

Replication relay is listening on a TCP port (defined in parameter `ServerPort` in `[Server]` section of the config file) for receiving queries from multiple clients.

Here is the documentation about the protocol used to communicate with the replication relay on this TCP port.

The php client class ([classrelayclient.php](php-client/classrelayclient.php)) or FPC client class ([ClassReplicationRelayClient.pas](fpc/ClassReplicationRelayClient.pas)) can be used as a reference to illustrate the protocol.

## Client sendcommand (send a command to the relay server)

Command packet header is 32 bytes followed by PayloadSize bytes:
    
- `Command`       : Word
- `SubCommand`    : Word
- `PayloadSize`   : DWord
- `Version`       : DWord   // Protocol version, current version is 0x00010020 (As defined in [ReplicationRelayStructures.pas](fpc/ReplicationRelayStructures.pas))
- `CliendID`      : DWord   // Should be set to 0 before getting a ClientID after authentication 
- `ClientToken`   : DWord   // Should be set to 0 before getting a ClientID after authentication 
- `Checksum`      : DWord
- `Reserved1`     : DWord   // Should be always set to 0
- `Reserved2`     : DWord   // Should be always set to 0
    
- `Payload`       : `PayloadSize` bytes
    
`Checksum` is the sum of `Command`+`SubCommand`+`PayloadSize`+`Version`+`ClientID`+`ClientToken` and each byte of the `Payload`.
    
Possible `Commands` are :
    
- `0x0001` : **Ping**
      
  Ping the server, `SubCommand` = 0, `Payload` is empty (size 0, no payload)   
  `ClientID` and `ClientToken` should be 0 before authentication, or set accordingly to the values given by the server after authentication
  
  getreply returns 0 upon success
        
  Right after connecting to the server, a ping should be sent before Authentication

- `0x0002` : **Authenticate**

  `ClientID` = 0, `ClientToken` = 0

  `Payload` :

  - `AuthKey`: QWord, HFNV1A hash of the shared secret, functions are provided in [HFNV1A.pas](fpc/HFNV1A.pas) and in [classrelayclient.php](php-client/classrelayclient.php)
  - `ClientToken`     : DWord = 0
  - `ClientID`        : DWord = 0
  - `ClientNameSize`  : DWord = size of `ClientName`
  - `ClientName`      : Name of the client (should be unique per client) as a null terminated (`ClientNameSize` must encode number of bytes of this string including the null byte)

  getreply will return 0 upon success and the following payload :
  
  - `ClientID`        : DWord
  - `ClientToken`     : DWord
  
  All subsequent command after a successful authentication must include this `ClientID` and `ClientToken` 

- `0x0003` : **Add Filter**

  `Payload` : 
    
  - `FilterType`        : Byte 
  - `FilterDiscardType` : Byte
  - `FilterQueueLimit`  : DWord = Maximum number of event allowed in the queue, 0 for unlimited
  - `SchemaNameSize`    : DWord = size of `SchemaName`
  - `SchemaName`        : Name of the schema, null terminated as null terminated string (`SchemaNameSize` must encode number of bytes of this string including the null byte)
  - `TableNameSize`     : DWord = size of `TableName`
  - `TableName`         : Name of the table, as a null terminated string (`TableNameSize` must encode number of bytes of this string including the null byte)


  `FilterType` is a combination of the following flags:

  - `0x01`: Insert
  - `0w02`: Update 
  - `0x04`: Delete

  `FilterDiscardType` can be one of these values: 
    
  - `0x00` : No discard (if a queuelimit is specified, when it's full, new events are dropped)
  - `0x01` : Unqueue Oldest (if a queuelimit is specified, when it's full, the oldest event is dropped for the newest one)
  - `0x02` : Ignore Newest (if a queuelimit is specified, when it's full, the newest event is dropped)
          
      
- `0x0004` : **Poll Event**

  No payload

  Result from getreply upon success will have the following payload :

  - `EventType`       : Byte (`0x01` : Insert, `0x02` : Update, `0x04` : Delete)
  - `EventPosition`   : QWord : Mysql Event position in binlog
  - `QueueSize`       : DWord : Number of events remaining in queue
  - `SchemaNameSize`  : DWord : Size of `SchemaName` (including null byte)
  - `SchemaName`      : Name of the schema
  - `TableNameSize`   : DWord : Size of `TableName` (including null byte)
  - `TableName`       : Name of table
  - `ColumnsCount`    : DWord : Number of columns
  - `ColumnsData`     : Collection of `ColumnData` (`ColumnsCount` occurences)

  The format of each `ColumnData` is the following :

  - `ColumnNameSize`  : DWord : Size of `ColumnName` (including null byte)
  - `ColumnName`      : Name of column
  - `BeforeSize`      : DWord : Size of `Before` (including null byte)
  - `Before`          : Value of the column before an update or delete in case of Update event, unused for an insert event
  - `AfterSize`       : DWord : Size of `After` (including null byte)
  - `After`           : Value of the column after an update in case of insert or update, unused for a delete event

## Client getreply
  
Reply packet header is 9 bytes followed by `PayloadSize` bytes of data :

- `Result`        : Byte
- `PayloadSize`   : DWord
- `Checksum`      : DWord
- `Payload`       : `PayloadSize` bytes

`Checksum` should be equal to the sum of `Result`, `PayloadSize` + each bytes of the payload

Result : 

- `0x00`    : Ok
- `0xF0`    : Client Disconnected
- Other     : Error

# License

Most of the code in this repository is licensed under `GPL-3.0-or-later`. You can find the full license text in [LICENSE-GPL3](./LICENSE-GPL3).

The only exception is the file [MySQL.pas](fpc/MySQL.pas) which is licensed under `MPL-1.1`. You can find the full license text for this file in [LICENSE-MPL1.1](./LICENSE-MPL1.1).
