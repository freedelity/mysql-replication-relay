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
unit ClassMysqlReplicationClient;

{$IFDEF FPC}
  {$MODE Delphi}
{$ENDIF}

{$UNDEF REPDEBUG} 
interface 

uses
  Declarations, ClassModule, MySQL, ClassMySQL, Dateutils, Classes, SysUtils, ClassDebug, IdTCPClient, IdIOHandler, IdHashSHA1, IdGlobal;

const

  COM_QUERY				      = $03;
  COM_BINLOG_DUMP       = $12;
  COM_REGISTER_SLAVE    = $15;
  COM_BINLOG_DUMP_GTID  = $1E;
  
  CLIENT_LONG_PASSWORD=$00000001;
  CLIENT_FOUND_ROWS=$00000002;
  CLIENT_LONG_FLAG=$00000004;
  CLIENT_CONNECT_WITH_DB=$00000008;
  CLIENT_NO_SCHEMA=$00000010;
  CLIENT_COMPRESS=$00000020;
  CLIENT_ODBC=$00000040; 
  CLIENT_LOCAL_FILES=$00000080;
  CLIENT_IGNORE_SPACE=$00000100;
  CLIENT_PROTOCOL_41=$00000200;
  CLIENT_INTERACTIVE=$00000400;
  CLIENT_SSL=$00000800;
  CLIENT_IGNORE_SIGPIPE=$00001000;
  CLIENT_TRANSACTIONS=$00002000;
  CLIENT_RESERVED=$00004000;
  CLIENT_SECURE_CONNECTION=$00008000;
  CLIENT_MULTI_STATEMENTS=$00010000;
  CLIENT_MULTI_RESULTS=$00020000;
  CLIENT_PS_MULTI_RESULTS=$00040000;
  CLIENT_PLUGIN_AUTH=$00080000;
  CLIENT_CONNECT_ATTRS=$00100000;
  CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA=$00200000;
  CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS=$00400000;
  CLIENT_SESSION_TRACK=$00800000;
  CLIENT_DEPRECATE_EOF=$01000000;
  
  
  FIELD_TYPE_DECIMAL = 0;
  FIELD_TYPE_TINY = 1;
  FIELD_TYPE_SHORT = 2;
  FIELD_TYPE_LONG = 3;
  FIELD_TYPE_FLOAT = 4;
  FIELD_TYPE_DOUBLE = 5;
  FIELD_TYPE_NULL = 6;
  FIELD_TYPE_TIMESTAMP = 7;
  FIELD_TYPE_LONGLONG = 8;
  FIELD_TYPE_INT24 = 9;
  FIELD_TYPE_DATE = 10;
  FIELD_TYPE_TIME = 11;
  FIELD_TYPE_DATETIME = 12;
  FIELD_TYPE_YEAR = 13;
  FIELD_TYPE_NEWDATE = 14;
  FIELD_TYPE_VARCHAR = 15;
  FIELD_TYPE_BIT = 16;
  FIELD_TYPE_TIMESTAMP2 = 17;
  FIELD_TYPE_DATETIME2=18;
  FIELD_TYPE_TIME2=19;
  FIELD_TYPE_JSON=245;
  FIELD_TYPE_NEWDECIMAL=246;
  FIELD_TYPE_ENUM = 247;
  FIELD_TYPE_SET = 248;
  FIELD_TYPE_TINY_BLOB = 249;
  FIELD_TYPE_MEDIUM_BLOB = 250;
  FIELD_TYPE_LONG_BLOB = 251;
  FIELD_TYPE_BLOB = 252;
  FIELD_TYPE_VAR_STRING = 253;
  FIELD_TYPE_STRING = 254; 
  FIELD_TYPE_UNKNOWN = 255;
  
  IntDigToByte: array [0..9] of Integer = (0,1,1,2,2,3,3,4,4,4);
  
type

  TMysqlHandshakeV10 = record
    ProtocolVersion: Byte;
    ServerVersion: String;
    ConnectionId: Integer;
    AuthData: String;
    AuthDataLen: Integer;
    AuthPluginName: String;
    CapabilityFlags: Int64;
    Charset: Integer;
    StatusFlag: Integer;
    Reserved: String;
   end;
   
  TBinLogEventHeader = record
    Timestamp: Integer;
    EventType: Byte;
    ServerId: Integer;
    EventSize: Integer;
    LogPos: Integer;
    Flags: Word;
   end;
   
  TBinLogQueryEvent = record
    SlaveProxyId: Integer;
    ExecutionTime: Integer;
    SchemaLength: Byte;
    ErrorCode: Word;
    StatusVarsLength: Word;
    
    StatusVars: String;
    Schema: String;
    Query: String;
  end;

  TBinLogFormatDescriptionEvent = record
    Version: Integer;
    MySQLVersion: String;
    CreateTimestamp: Integer;
    HeaderLength: Byte;
    EventTypeHeaderLen: Array of Integer;
  end;


  TBinLogTableMapEvent = record
    TableId: Integer;
    TableIdHigh: Word; // some versions of the protocol have 4 bytes, others 6 for TableIdHigh
    Flags: Word;
    
    SchemaName: String;
    TableName: String;
    ColumnTypeDef: Array Of Byte;
    ColumnMetaDef: Array Of String;  
  end;
  
  TByteArray = Array of Byte;
  TStringArray = Array of String;
  
  TBinLogRowData = record   
    NullBitmap1: TByteArray;
    NullBitMap2: TByteArray;
    Values1: Array of String;
    Values2: Array of String;
  end;
  
  TBinLogRowEvent = object
    TableId: Integer;
    TableIdHigh: Word; // some versions of the protocol have 4 bytes, others 6 for TableIdHigh
    Flags: Word;
    
    MaxColumns: Integer;
    NumColumns: Integer;
    ColPresentMap1: TByteArray;
    NumColPresentMap1: Integer;
    ColPresentMap2: TByteArray;
    NumColPresentMap2: Integer;
    
    NumRows: Integer;
    MaxRows: Integer;
    Rows: Array of TBinLogRowData;
    
    procedure Init;
    procedure Resize(AMaxCol,AMaxRow: Integer);
  end;
    
  TBinLogTableColumnDef = record
    ColumnNameId: DWord;
    ColumnName: String;
    ColumnType: String;
    ColumnEnum: TStringArray;
    ColumnFieldType: Integer;
    ColumnFieldMeta: String;
  end;
  
  TBinLogTableDef = record
    SchemaNameId: DWord;
    SchemaName: String;
    TableNameId: DWord;
    TableName: String;
    TableId: Integer;
    Columns: Array of TBinLogTableColumnDef;
  end;
  
  TBinLogEventRow = record
    FieldsIn: Array of String;
    ValuesIn: Array of String; 
    FieldsOut: Array of String;
    ValuesOut: Array of String; 
  end;
  
  TBinLogEvent = object
    EventType: Integer;
    Schema: String;
    Table: String;
    NumResults: Integer;
    Data: String;
    NumColumns: Integer;
    MaxColumns: Integer;
    ColumnTypes: Array of Integer;
    NumRows: Integer;
    MaxRows: Integer;
    Rows: Array of TBinLogEventRow;

    procedure Init;
    procedure Resize(AMaxCol,AMaxRow: Integer);
  end;

  TMysqlReplicationClient = class
  protected
    FTCPClient: TIdTCPClient;
    FBufferStream: TMemoryStream;
    FSlaveId: Integer;
    FBinLogFormatDescription: TBinLogFormatDescriptionEvent;
    FModule: TModule;
    FSQL: TMySQL;
    FServerHost: String;
    FServerLogin: String;
    FServerPass: String;
    FServerDB: String;
    FBinLogCRC32: Byte;
       
    FRowEvent: TBinLogRowEvent;
    
    function GetIdTableIdx(AId: Integer): Integer;
    function GetSchemaTableIdx(AName: String;ASchema: String): Integer;
    
  public

    FBinLogFile: String;
    FBinLogOffset: Integer;
    FBinLogTables: Array of TBinLogTableDef;
   
    constructor Create;
    destructor Destroy; override;
    function Connect(Host,Login,Password,DB: String): Integer;
    function Init(ASlaveId: Integer): Integer;
    function Reconnect(): Integer;
    function LoadSchema(): Integer;
    function RegisterSlave(ASlaveId: Integer;Host,Login,Password: String): Integer;
    function BinlogDump(AFilename: String; APos: Integer): Integer;
    function Execute(ACommand: String): Integer;
    function GetEvent(var AResult: TBinLogEvent):Integer;
    
    function GetResponse(AStream:TMemoryStream): Integer;
    procedure GetHandshake(AStream:TMemoryStream; var AHandshake: TMysqlHandshakeV10);
    procedure Authenticate(AStream:TMemoryStream; AHandshake: TMysqlHandshakeV10; Login,Password, DB: String);
    
  published
  end;   

       
implementation

    procedure TBinLogRowEvent.Init;
    begin
      NumRows:=0;
      MaxRows:=0;
      NumColumns:=0;
      MaxColumns:=0;
    end;

    procedure TBinLogRowEvent.Resize(AMaxCol,AMaxRow: Integer);
    var 
      i: Integer;
    begin
      if(AMaxCol>MaxColumns) then
      begin
        MaxColumns:=AMaxCol;
        SetLength(ColPresentMap1,AMaxCol);
        SetLength(ColPresentMap2,AMaxCol);
      end;
      
      if(AMaxRow>MaxRows) then
      begin
        MaxRows:=AMaxRow;
        SetLength(Rows,AMaxRow);
      end;

      for i:=0 to AMaxRow-1 do
      begin
        SetLength(Rows[i].NullBitmap1,MaxColumns);
        SetLength(Rows[i].NullBitMap2,MaxColumns);
        SetLength(Rows[i].Values1,MaxColumns);
        SetLength(Rows[i].Values2,MaxColumns);
      end;
    end;
    
    procedure TBinLogEvent.Init;
    begin
      NumRows:=0;
      MaxRows:=0;
      NumColumns:=0;
      MaxColumns:=0;
    end;
    
    procedure TBinLogEvent.Resize(AMaxCol,AMaxRow: Integer);
    var 
      i: Integer;
    begin
      if(AMaxCol>MaxColumns) then
      begin
        MaxColumns:=AMaxCol;
      end;
      
      if(AMaxRow>MaxRows) then
      begin
        MaxRows:=AMaxRow;
        SetLength(Rows,AMaxRow);
      end;

      for i:=0 to AMaxRow-1 do
      begin
        SetLength(Rows[i].FieldsIn,MaxColumns);
        SetLength(Rows[i].ValuesIn,MaxColumns);
        SetLength(Rows[i].FieldsOut,MaxColumns);
        SetLength(Rows[i].ValuesOut,MaxColumns);
      end;
    end;    

{******************************************************************************}
function GetField(ARow: PMYSQL_ROW; AIndex: Integer): UTF8String; inline;
begin
  if ARow^[AIndex] = nil then
    result := ''
  else
    result := ARow^[AIndex];
end;  
{******************************************************************************}
function StrToHex(AStr:String) : String; inline;
var
  i: Integer;
begin
  Result:='';
  for i:=1 to Length(AStr) do
  begin
    if(i>1) then Result:=Result+' ';
    Result:=Result+IntToHex(Ord(AStr[i]),2);
  end;
end;
{******************************************************************************}
function GetLenEncInteger(AStream:TMemoryStream): Integer; inline;
var
  tmp: Integer;
begin
  tmp:=AStream.ReadByte;
  //AddToLog(llWarning,'GetLenEndInteger : tmp = '+IntToStr(tmp));
  if(tmp<251) then
  begin
    Result:=tmp;
  end
  else if(tmp=252) then
  begin
    Result:=AStream.ReadByte+256*AStream.ReadByte;
  end
  else if(tmp=253) then
  begin
    Result:=AStream.ReadByte+256*AStream.ReadByte+65536*AStream.ReadByte;
  end
  else if(tmp=254) then
  begin
    Result:=AStream.ReadByte+256*AStream.ReadByte+65536*AStream.ReadByte+16777216*AStream.ReadByte;
    AddToLog(llWarning,'GetLenEncInteger 8byte value, truncated to 32bits');
    AStream.ReadByte;
    AStream.ReadByte;
    AStream.ReadByte;
    AStream.ReadByte;
  end
  else
  begin
    AddToLog(llError,'GetLenEncInteger Invalid Value 255');
    Result:=0;
  end; 
end;
{******************************************************************************}
function GetBitmapToByteArray(AStream:TMemoryStream;var AMap: TByteArray;ANumColumns: Integer): Integer; inline;
var
  i,j,count,limit: Integer;
  curbyte,mask: Byte;
begin
  Result:=0;
  
//  AddToLog(llWarning,'GetBitmapToByteArray, ANumColumns = '+IntToStr(ANumColumns));
    
 // SetLength(AMap,ANumColumns);  // Supposed to be done in init/resize
  limit:=((ANumColumns+7) div 8);
  
//  AddToLog(llWarning,'GetBitmapToByteArray : ANumColumns = '+IntToStr(ANumColumns)+', limit = '+IntToStr(limit));  
  
  count:=0;
  for i:=0 to limit-1 do
  begin
    curbyte:=AStream.ReadByte();
    mask:=1;
    
    for j:=0 to 7 do
    begin
//      AddToLog(llWarning,'i = '+IntToStr(i)+' count = '+IntToStr(count)+' Length(AMap) = '+IntToStr(Length(AMap))+' curbyte = '+IntToStr(curbyte));
      if count=ANumColumns then break;
      AMap[count]:=0;
      if(curbyte and mask)<>0 then
      begin
        AMap[count]:=1;
        Inc(Result);
      end;
//      AddToLog(llWarning,'shl mask ('+IntToStr(mask)+')');
      if (j<7) then mask:=mask shl 1;
//      AddToLog(llWarning,'shl done');
      Inc(count);
    end;
  end;    
end;
{******************************************************************************}
function GetBigEndian(ABuf: Array of Byte; AIdx: Integer; ALen: Integer) : Integer; inline;
var
	i: Integer;
begin
	result:=0;
	if(ALen=0) then Exit;
	
	for i:=0 to ALen-1 do
	begin
		result:=result SHL 8;
		result:=result+ABuf[AIdx+i];
	end;
end;
{******************************************************************************}
function IntToFixStr(AVal: Integer;ALen: Integer): String; inline;
var tmp,i: Integer; 
begin
	tmp:=AVal;
	result:='';
	if(ALen=0) then Exit;
  SetLength(result,ALen);
	for i:=0 to ALen-1 do 
	begin
    result[ALen-i]:=Char(48+(tmp MOD 10)); // 48 = Ascii for 0
    tmp:=tmp DIV 10;
	end;	
end;
{******************************************************************************}
function DecimalNumberPart(var ABuf: Array of Byte; AIdx: Integer; ALen,APreLen: Integer): String;
var
	tmp,rsize: Integer;
begin
	rsize:=ALen MOD 4;
	result:='';
	tmp:=0;
	
	if(rsize=0) AND (ALen>0) then rsize:=4; // full initial 4 byte block need to be processed as first block (could be 7, 8 or 9 digits)
	
  {$notes off}
	tmp:=GetBigEndian(ABuf,AIdx,rsize);
  {$notes on}
	result:=result+IntToFixStr(tmp,APreLen); // Block of APreLen digits
	
	while (rsize<ALen) do
	begin
    {$notes off}
		tmp:=GetBigEndian(ABuf,AIdx+rsize,4);
    {$notes on}
		rsize:=rsize+4;
		result:=result+IntToFixStr(tmp,9); // Subsequents blocks are known to be 9 Digits
	end;
end;
{******************************************************************************}
function GetDecimal(AStream:TMemoryStream; APrecision,AScale: Integer): String;
var
	i,tmp,ilen,flen,iprelen,fprelen,mask: Integer;
	buf: Array of Byte;
begin
	Result:='';	
	ilen:=0;
	flen:=0;
	tmp:=AStream.ReadByte();
	mask:=(tmp AND $80);
	if(mask>0) then 
		mask:=0
	else mask:=$FF;
	tmp:=(tmp XOR $80);	// Xor the mask bit... 
	
	iprelen:=(APrecision-AScale) MOD 9;	// Number of digits for integral part
	ilen:=IntDigToByte[iprelen] + 4*((APrecision - AScale) DIV 9); // Number of bytes coding integral part
	fprelen:=AScale MOD 9;	// Number of digits for fractional part
	flen:=IntDigToByte[fprelen] + (4*(AScale DIV 9)); // Number of bytes coding fractional part
	
	SetLength(buf,ilen+flen);
	buf[0]:=tmp XOR mask;
	for i:=1 to (ilen+flen)-1 do
		buf[i]:=AStream.ReadByte() xor MASK;
	
//	for i:=0 to Length(buf)-1 do
//		AddToLog(llWarning,IntToStr(i)+' : '+IntToStr(buf[i]));
	
	if (mask=$FF) then Result:=Result+'-';
	Result:=Result+DecimalNumberPart(buf,0,ilen,iprelen);
	Result:=Result+'.';
	Result:=Result+DecimalNumberPart(buf,ilen,flen,fprelen);	
end;
{******************************************************************************}
function GetBlob(AStream:TMemoryStream; ASize: Integer): String; inline;
var
	mul,sz,i: Integer;
	
begin
	Result:='';	
	
	mul:=1;
	sz:=0;
	for i:=0 to ASize-1 do
	begin
		sz:=sz+mul*AStream.ReadByte();
		mul:=mul SHL 8;
	end;
  
  {$IFDEF REPDEBUG}
    AddToLog(llWarning,'text/blob size : '+IntToStr(sz));
  {$ENDIF}
	SetLength(Result,sz);
        AStream.Read(Result[1],sz);
	
	//for i:=0 to sz-1 do
	//	Result:=Result+Char(AStream.ReadByte());
end;
{******************************************************************************}
function GetColumn(AStream:TMemoryStream;AInType: Integer;AMeta: String;AEnum: TStringArray): String; inline;
var 
  sz,mxsz,i: Integer;
  AType: Integer;
  h,s,d,m,y: Integer;
  lint: QWord;
  lShort: ShortInt;
  lSingle: Single;
  lDouble: Double;
  qw: QWord;
begin
  Result:='';
  {$IFDEF REPDEBUG}
    AddToLog(llWarning,'GetColumn, Type = '+IntToStr(AInType)+' Meta = '+StrToHex(AMeta));
  {$ENDIF}
  
  AType:=AInType;
  
  if(AType=FIELD_TYPE_STRING) then
  begin
    if((Ord(AMeta[2])=FIELD_TYPE_ENUM) OR (Ord(AMeta[2])=FIELD_TYPE_SET)) then
    begin
      {$IFDEF REPDEBUG}
        AddToLog(llWarning,'Override type string to enum/set');
      {$ENDIF}
      AType:=Ord(AMeta[2]);     
    end;
  end;
  
    case AType of
	  FIELD_TYPE_TINY:
	  begin
      {$IFDEF REPDEBUG}
        AddToLog(llWarning,'TINY');
      {$ENDIF}
      lShort:=ShortInt(AStream.ReadByte());
      Result:=IntToStr(lShort);	  
	  end;
	  FIELD_TYPE_SHORT:
	  begin
      {$IFDEF REPDEBUG}
        AddToLog(llWarning,'SHORT');
      {$ENDIF}
      sz:=AStream.ReadWord();
      Result:=IntToStr(sz);
	  end;
    FIELD_TYPE_LONG:
    begin
      {$IFDEF REPDEBUG}
        AddToLog(llWarning,'LONG');
      {$ENDIF}
      sz:=Integer(AStream.ReadDWord());
      Result:=IntToStr(sz);
    end;
    FIELD_TYPE_LONGLONG:
    begin
      {$IFDEF REPDEBUG}
        AddToLog(llWarning,'LONGLONG');
      {$ENDIF}
      qw:=AStream.ReadQWord();
      Result:=IntToStr(qw);
    end;      
	  FIELD_TYPE_FLOAT:
	  begin
      {$IFDEF REPDEBUG}
        AddToLog(llWarning,'FLOAT');
      {$ENDIF}
      AStream.Read(lSingle,SizeOf(lSingle));
      Result:=FloatToSTr(lSingle);
	  end;
	  FIELD_TYPE_DOUBLE:
    begin
      {$IFDEF REPDEBUG}
        AddToLog(llWarning,'DOUBLE');
      {$ENDIF}
      AStream.Read(lDouble,SizeOf(lDouble));
      Result:=FloatToStr(lDouble);
	  end;
    FIELD_TYPE_VARCHAR:
    begin 
      {$IFDEF REPDEBUG}
        AddToLog(llWarning,'VARCHAR Meta = '+IntToStr(Ord(AMeta[1])));
      {$ENDIF}
      if(Ord(AMeta[1])>0) then // Meta = max size, if > 255 : header is 2 bytes otherwise 1
        sz:=AStream.ReadWord()
        else sz:=Ord(AStream.ReadByte()); //GetLenEncInteger(AStream);
      {$IFDEF REPDEBUG}
        AddToLog(llWarning,'size = '+IntToStr(sz));
      {$ENDIF}      
      Result:='';
      SetLength(Result,sz);
      AStream.Read(Result[1],sz);
      //for i:=0 to sz-1 do Result:=Result+Char(AStream.ReadByte());
    end;
    FIELD_TYPE_NEWDECIMAL:
    begin 
      {$IFDEF REPDEBUG}
        AddToLog(llWarning,'NEWDECIMAL('+IntToStr(Ord(AMeta[2]))+','+IntToStr(Ord(AMeta[1]))+')');
      {$ENDIF}
      Result:=GetDecimal(AStream,Ord(AMeta[2]),Ord(AMeta[1]));
    end;
	  FIELD_TYPE_BLOB:	  
	  begin
      {$IFDEF REPDEBUG}
        AddToLog(llWarning,'TEXT/BLOB('+IntToStr(Ord(AMeta[1]))+')');
      {$ENDIF}
      Result:=GetBlob(AStream,Ord(AMeta[1]));
	  end;
	  FIELD_TYPE_ENUM:	  
	  begin
      {$IFDEF REPDEBUG}
        AddToLog(llWarning,'ENUM ?');
      {$ENDIF}
      if(Ord(AMeta[1])=1) then sz:=AStream.ReadByte()
      else if(Ord(AMeta[1])=2) then sz:=AStream.ReadWord();
      {$IFDEF REPDEBUG}
        AddToLog(llWarning,'idx = '+IntToStr(sz)+' / Len enum = '+IntToStr(Length(AEnum)));
      {$ENDIF}      
      if(sz>0) AND (sz<Length(AEnum)) then Result:=AEnum[sz]
      else if (sz=0) then Result:=''
      else Result:='?';
	  end;	  
    FIELD_TYPE_STRING:	  
    begin
      {$IFDEF REPDEBUG}
        AddToLog(llWarning,'STRING');
      {$ENDIF}
      if(Ord(AMeta[2])=FIELD_TYPE_STRING) then
      begin
        mxsz:=Ord(AMeta[1]);
      end
      else
      begin
        mxsz:=Ord(AMeta[1])*Ord(AMeta[2])*256;
        mxsz:=((mxsz SHR 4) AND $0300) XOR $0300;       
        mxsz:=mxsz+Ord(AMeta[1]);
      end;
      //mxsz:=(Ord(AMeta[1]) AND $03) SHL 8;
      //mxsz:=mxsz+Ord(AMeta[2]);
      if(mxsz>255) then sz:=AStream.ReadWord()
      else sz:=Ord(AStream.ReadByte());
      {$IFDEF REPDEBUG}
        AddToLog(llWarning,'size = '+IntToStr(sz)+' mxsz = '+IntToStr(mxsz));
      {$ENDIF}      
      Result:='';
      SetLength(Result,sz);
      AStream.Read(Result[1],sz);
      //for i:=0 to sz-1 do Result:=Result+Char(AStream.ReadByte());      
    end;
	  FIELD_TYPE_DATETIME:
	  begin
      lint:=AStream.ReadQWord();		
      {$IFDEF REPDEBUG}      
        AddToLog(llWarning,'DATETIME Raw = '+IntToStr(lint));
      {$ENDIF}
      
      Result:=IntToFixStr(lint MOD 100,2);
      lint:=lint DIV 100;
      Result:=IntToFixStr(lint MOD 100,2)+':'+Result;
      lint:=lint DIV 100;
      Result:=IntToFixStr(lint MOD 100,2)+':'+Result;
      lint:=lint DIV 100;
      
      Result:=IntToFixStr(lint MOD 100,2)+' '+Result;
      lint:=lint DIV 100;
      Result:=IntToFixStr(lint MOD 100,2)+'-'+Result;
      lint:=lint DIV 100;
      Result:=IntToFixStr(lint,4)+':'+Result;		
	  end;
	  
	  FIELD_TYPE_DATETIME2:
	  begin
		//	 Date Time 2 Format, 5 bytes :
		//   1 bit  sign           (1= non-negative, 0= negative)
		//   17 bits year*13+month  (year 0-9999, month 0-12)
		//   5 bits day            (0-31)
		//   5 bits hour           (0-23)
		//   6 bits minute         (0-59)
		//   6 bits second         (0-59)
    {$IFDEF REPDEBUG}
      AddToLog(llWarning,'DATETIME2');
    {$ENDIF}
		lint:=0;
		for i:=0 to 4 do
		begin
			lint:=lint SHL 8;
			lint:=lint+Ord(AStream.ReadByte());
		end;
		
		s:=lint AND $3F;
		lint:=lint SHR 6;
		i:=lint AND $3F;
		lint:=lint SHR 6;
		h:=lint AND $1F;
		lint:=lint SHR 5;
		d:=lint AND $1F;
		lint:=lint SHR 5;
		y:=lint AND $1FFFF;
		m:=y MOD 13;
		y:=y DIV 13;
		
		Result:=IntToFixStr(y,4)+'-'+IntToFixStr(m,2)+'-'+IntToFixStr(d,2)+' '+IntToFixStr(h,2)+':'+IntToFixStr(i,2)+':'+IntToFixStr(s,2);
	  end;
  
	  FIELD_TYPE_DATE:
	  begin
      //	 Date Format, 3 bytes : !!!!!! LITTLE-ENDIAN HERE !!!!!!
      //   15 bits : year
      //   4 bits : month
      //   5 bits : day
      {$IFDEF REPDEBUG}
        AddToLog(llWarning,'DATE');
      {$ENDIF}
      lint:=Ord(AStream.ReadByte())+Ord(AStream.ReadByte())*256+Ord(AStream.ReadByte())*65536;
      
      {$IFDEF REPDEBUG}
        AddToLog(llWarning,'raw = '+IntToStr(lint));
      {$ENDIF}      
      
      d:=lint AND $1F;
      lint:=lint SHR 5;
      m:=lint AND $0F;
      lint:=lint SHR 4;
      y:=lint;
      
      Result:=IntToFixStr(y,4)+'-'+IntToFixStr(m,2)+'-'+IntToFixStr(d,2);
	  end;
      
    FIELD_TYPE_TIME2:
	  begin
      //	 Time2 Format, 3 bytes : !!!!!! BIG-ENDIAN HERE !!!!!!
      //   1 bit : sign
      //   1 bit : unused
      //   10 bits : hour
      //   6 bits : minute
      //   6 bits : second

      {$IFDEF REPDEBUG}
        AddToLog(llWarning,'FIELD_TYPE_TIME2');
      {$ENDIF}
      lint:=Ord(AStream.ReadByte()*65536)+Ord(AStream.ReadByte())*256+Ord(AStream.ReadByte());
      
      {$IFDEF REPDEBUG}
        AddToLog(llWarning,'raw = '+IntToStr(lint));
      {$ENDIF}      
      
      s:=lint AND $3F;
      lint:=lint SHR 6;
      m:=lint AND $3F;
      lint:=lint SHR 6;
      h:=lint AND $3FF;
      
      Result:=IntToFixStr(h,2)+'-'+IntToFixStr(m,2)+'-'+IntToFixStr(s,2);
	  end;
    
    end;
end;

{ TMysqlReplicationClient }
{******************************************************************************}
constructor TMysqlReplicationClient.Create;
begin
  FBinLogCRC32:=0;
  FBufferStream:=TMemoryStream.Create;
 
  FModule := TModule.Create;
  FSQL := TMySQL.Create; 
  FSQL.Module := FModule;
  FSQL.Connect;	    
  
end;
{******************************************************************************}
destructor TMysqlReplicationClient.Destroy;
begin
  FreeAndNil(FBufferStream);
	inherited;
end;
{******************************************************************************}
function TMysqlReplicationClient.GetSchemaTableIdx(AName: String; ASchema: String): Integer;
var
  i: Integer;
begin
  Result:=-1;
  for i:=0 to length(FBinLogTables)-1 do
  begin
    if(FBinLogTables[i].TableName=AName) AND (FBinLogTables[i].SchemaName=ASchema) then
    begin
      Result:=i;
      Exit;
    end;
  end;
end;
{******************************************************************************}
function TMysqlReplicationClient.GetIdTableIdx(AId: Integer): Integer;
var
  i: Integer;
begin
  Result:=-1;
  for i:=0 to length(FBinLogTables)-1 do
  begin
    if(FBinLogTables[i].TableId=AId) then
    begin
      Result:=i;
      Exit;
    end;
  end;
end;
{******************************************************************************}
function TMysqlReplicationClient.Execute(ACommand: String): Integer;
var
  i,tmp: Integer;
  str: String;
  p : ^Byte;
begin 
  {$IFDEF REPDEBUG}
    AddToLog(llWarning,'TMysqlReplicationClient.Execute : '+ACommand);
  {$ENDIF}
  Result:=0;

  FBufferStream.Clear;
  // Reserve Header
  tmp:=Length(ACommand)+1+1;
  FBufferStream.WriteDWord(tmp);
  
  FBufferStream.WriteByte(COM_QUERY);

  for i:=1 to Length(ACommand) do FBufferStream.WriteByte(Ord(ACommand[i]));
  FBufferStream.WriteByte($00);

  {$IFDEF REPDEBUG}
    AddToLog(llWarning,'Sending Command');
  {$ENDIF}
  
  p:=FBufferStream.Memory;
  
  p^:=(tmp AND $FF);
  tmp:=tmp shr 8;
  inc(p);
  p^:=(tmp AND $FF);
  tmp:=tmp shr 8;
  inc(p);
  p^:=(tmp AND $FF);
  inc(p);
  p^:=$00; // Packet seqnr 
     
  FBufferStream.Seek(0, soFromBeginning);  
  FTCPClient.IOHandler.Write(FBufferStream,FBufferStream.Size);
  
  {$IFDEF REPDEBUG}
    AddToLog(llWarning,'Packet Sent');
  {$ENDIF}

  if(GetResponse(FBufferStream)=1) then
  begin    
    str:='';
    for i:=0 to FBufferStream.Size-1 do str:=str+Char(FBufferStream.ReadByte);
    {$IFDEF REPDEBUG}  
      AddToLog(llWarning,'Data : '+IntToStr(FBufferStream.Size)+' bytes');      
      AddToLog(llWarning,' Response HEX    : '+StrToHex(str));
      AddToLog(llWarning,' Response STR    : '+str);
    {$ENDIF}
    if(Ord(str[1])=$00) then 
    begin
      {$IFDEF REPDEBUG}
        AddToLog(llWarning,'Success!');
      {$ENDIF}
      Result:=1; 
    end;
  end;
end;
{******************************************************************************}
function TMysqlReplicationClient.GetResponse(AStream:TMemoryStream): Integer;
var
  len,seq: Integer;
begin
  //AddToLog(llWarning,'GetResponse');
  Result:=0;
  AStream.Clear;   
  // Read Response Header : [len0][len1][len2][seqnr]
  
  
  try
  
    //WriteLn('=> MysqlRep.ReadHeader');
    //AddToLog(llWarning,'GetResponse.PreReadHeader');
    FTCPClient.IOHandler.ReadStream(AStream,4);
    //AddToLog(llWarning,'GetResponse.PostReadHeader');
    //WriteLn('=> MysqlRep.ReadHeaderDone');
    if(AStream.Size=4) then
    begin
      AStream.Seek(0, soFromBeginning);
      //AddToLog(llWarning,'Read Header '+IntToStr(AStream.Size)+' bytes');
      len:=AStream.ReadByte;
      len:=len+AStream.ReadByte*256;
      len:=len+AStream.ReadByte*65536;
      //AddToLog(llWarning,'Data Length  : '+IntToStr(len));      
      seq:=AStream.ReadByte;
      seq:=seq+1-1; // Disable note seq is unused
      //AddToLog(llWarning,'Packet Seqno : '+IntToStr(seq));
      AStream.Clear;
      //WriteLn('=> MysqlRep.ReadPayload');
      //AddToLog(llWarning,'GetResponse.PreReadPayload');
      FTCPClient.IOHandler.ReadStream(AStream,len);
      //AddToLog(llWarning,'GetResponse.PostReadHeader');
      //WriteLn('=> MysqlRep.ReadPayloadDone');
      AStream.Seek(0, soFromBeginning);
      Result:=1;
    end;   
  except
    on E:Exception do
    begin
      AddToLog(llWarning,'GetResponse Exception : '+E.Message);
      Result:=666;
    end;
  end;
end;
{******************************************************************************}
procedure TMysqlReplicationClient.GetHandshake(AStream:TMemoryStream; var AHandshake: TMysqlHandshakeV10);
var 
  i,tmp: Integer;
begin

  {$IFDEF REPDEBUG}
    AddToLog(llWarning,'GetHandshake a');
  {$ENDIF}

  AStream.Seek(0, soFromBeginning);
  
  AHandshake.ProtocolVersion:=AStream.ReadByte;
  
  AHandshake.ServerVersion:='';
  repeat
    tmp:=AStream.ReadByte;
    if(Ord(tmp)<>0) then AHandshake.ServerVersion:=AHandshake.ServerVersion+Char(tmp);
  until tmp=0;
  

  {$IFDEF REPDEBUG}
    AddToLog(llWarning,'GetHandshake b');
  {$ENDIF}
  
  AHandshake.ConnectionId:=AStream.ReadByte+AStream.ReadByte*256+AStream.ReadByte*65536+AStream.ReadByte*16777216;
  
  AHandshake.AuthData:='';
  for i:=0 to 7 do 
    AHandshake.AuthData:=AHandshake.AuthData+Char(AStream.ReadByte);
  

  {$IFDEF REPDEBUG}
    AddToLog(llWarning,'GetHandshake c');
  {$ENDIF}
  
  tmp:=AStream.ReadByte; // Filler
  
  AHandshake.CapabilityFlags:=AStream.ReadByte+AStream.ReadByte*256;

  {$IFDEF REPDEBUG}
    AddToLog(llWarning,'GetHandshake c2');
  {$ENDIF}
  
  AHandshake.Charset:=AStream.ReadByte;
  
  AHandshake.StatusFlag:=AStream.ReadByte+AStream.ReadByte*256;

  {$IFDEF REPDEBUG}
    AddToLog(llWarning,'GetHandshake c3');
  {$ENDIF}
    
  AHandshake.CapabilityFlags:=AHandshake.CapabilityFlags+AStream.ReadByte*65536+AStream.ReadByte*16777216;
  
  {$IFDEF REPDEBUG}
    AddToLog(llWarning,'GetHandshake c4');
  {$ENDIF}
  
  AHandshake.AuthDataLen:=AStream.ReadByte;
  
  {$IFDEF REPDEBUG}
    AddToLog(llWarning,'GetHandshake d');
  {$ENDIF}  
  
  AHandshake.Reserved:='';
  for i:=0 to 9 do
    AHandshake.Reserved:=AHandshake.Reserved+Char(AStream.ReadByte);  // 10 Reserved bytes
  
  if(AHandshake.AuthDataLen>0) then
  begin
    tmp:=AHandshake.AuthDataLen-8;    
    if(tmp>12) then tmp:=12;
    for i:=0 to tmp-1 do
      AHandshake.AuthData:=AHandshake.AuthData+Char(AStream.ReadByte);
  end;
  
  {$IFDEF REPDEBUG}
    AddToLog(llWarning,'GetHandshake e');
  {$ENDIF}  
  
  AHandshake.AuthPluginName:='';
  repeat
    tmp:=AStream.ReadByte;
    if(Ord(tmp)<>0) then AHandshake.AuthPluginName:=AHandshake.AuthPluginName+Char(tmp);
  until tmp=0;  
end;
{******************************************************************************}
procedure TMysqlReplicationClient.Authenticate(AStream:TMemoryStream; AHandshake: TMysqlHandshakeV10; Login,Password, DB: String);
var
  i,tmp: Integer;
  lAuthResponse: String;
  lHashP1: String;
  lPreHashP2: String;
  lPreHashP2C: String;
  lHashP2: String;
  lSHA : TIdHashSHA1;
  p : ^Byte;
begin
  AddToLog(llWarning,'Authenticate');
// Requires CLIENT_SECURE_CONNECTION
// SHA1( password ) XOR SHA1( "20-bytes random data from server" <concat> SHA1( SHA1( password ) ) ) 
  AStream.Clear;

  // Reserve Header
  AStream.WriteDWord($00000000);
  
  tmp:=CLIENT_NO_SCHEMA+CLIENT_LONG_PASSWORD+CLIENT_LONG_FLAG+CLIENT_TRANSACTIONS+CLIENT_SECURE_CONNECTION+CLIENT_PROTOCOL_41+CLIENT_CONNECT_WITH_DB;
  AStream.WriteDWord(tmp);
  
  tmp:=16777215;
  AStream.WriteDWord(tmp);
  
  AStream.WriteByte(AHandshake.Charset);
  
  for i:=0 to 22 do AStream.WriteByte($00); // 23 blank bytes
  
  for i:=1 to Length(Login) do AStream.WriteByte(Ord(Login[i]));
  AStream.WriteByte($00);
  
  lSHA:=TIdHashSHA1.Create;
  lHashP1:=BytesToString(lSHA.HashString(Password));
  lPreHashP2:=BytesToString(lSHA.HashString(lHashP1,IndyTextEncoding_UTF8,IndyTextEncoding_UTF8));
  lPreHashP2C:=AHandshake.AuthData+lPreHashP2;  
  lHashP2:=BytesToString(lSHA.HashString(lPreHashP2C,IndyTextEncoding_UTF8,IndyTextEncoding_UTF8));
  FreeAndNil(lSHA);
  
  lAuthResponse:='';
  for i:=1 to 20 do
    lAuthResponse:=lAuthResponse+Char(Ord(lHashP1[i]) XOR Ord(lHashP2[i]));
  
  AStream.WriteByte(20);
  for i:=1 to 20 do AStream.WriteByte(Ord(lAuthResponse[i]));

  for i:=1 to Length(DB) do AStream.WriteByte(Ord(DB[i]));
  AStream.WriteByte($00);  

  for i:=1 to Length(AHandshake.AuthPluginName) do AStream.WriteByte(Ord(AHandshake.AuthPluginName[i]));
  AStream.WriteByte($00);  
  
  tmp:=AStream.Size-4;  // Data size = packet size - header (4 Bytes)
  
  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'Sending Authentication');
  {$ENDIF}
  
  
  p:=AStream.Memory;
  
  p^:=(tmp AND $FF);
  tmp:=tmp shr 8;
  inc(p);
  p^:=(tmp AND $FF);
  tmp:=tmp shr 8;
  inc(p);
  p^:=(tmp AND $FF);
  inc(p);
  p^:=$01; // Packet seqnr 
    
  AStream.Seek(0, soFromBeginning);  
  FTCPClient.IOHandler.Write(AStream,AStream.Size);
  
  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'Authentication Sent');
  {$ENDIF}
  
end;
{******************************************************************************}
function TMysqlReplicationClient.Reconnect(): Integer;
var resu: Integer;
begin
  Result:=0;
  AddToLog(llWarning,'Reconnecting...');
  Sleep(1000);
  resu:=Connect(FServerHost,FServerLogin,FServerPass,FServerDB);
  if(resu<>1) then
  begin
    AddToLog(llError,'Error reconnecting to Master MySQL Server!');
    Exit;
  end;
  resu:=Init(FSlaveId);
  if(resu<>1) then
  begin
    AddToLog(llError,'Error in Replication Client Initialization!');
    Exit;
  end;         
  Result:=1;
end;
{******************************************************************************}
function TMysqlReplicationClient.Connect(Host,Login,Password,DB: String): Integer;
var
  lHandshake: TMysqlHandshakeV10;
  tmp: String;
  i: Integer;
begin
  Result:=0;
	AddToLog(llWarning,'TMysqlReplicationClient.Init');

	FServerHost:=Host;
	FServerLogin:=Login;
	FServerPass:=Password;
	FServerDB:=DB;

  try
    FTCPClient:=TIdTCPClient.Create;
    FTCPClient.Host:=Host;
    FTCPClient.Port:=3306;
    FTCPClient.Connect();
  except
    on E:Exception do 
    begin
      AddToLog(llError,'Error connecting to host '+Host+' : '+E.Message);
      Exit;
    end; 
  end;
    if(FTCPClient.Connected) then
    begin
      AddToLog(llWarning,'Connection to MySQL Server Successful');
      if(GetResponse(FBufferStream)=1) then
      begin
        {$IFDEF REPDEBUG}
        AddToLog(llWarning,'Data : '+IntToStr(FBufferStream.Size)+' bytes');
        {$ENDIF}
        {$hints off}
        GetHandshake(FBufferStream,lHandshake);
        {$hints on}
  
        {$IFDEF REPDEBUG}
          AddToLog(llWarning,'Handshake Header :');
          AddToLog(llWarning,' Protocol Version    : '+IntToStr(lHandshake.ProtocolVersion));
          AddToLog(llWarning,' Server Version      : '+lHandshake.ServerVersion);
          AddToLog(llWarning,' Connection ID       : '+IntToStr(lHandshake.ConnectionId));
          AddToLog(llWarning,' Server Capabilities : '+IntToHex(lHandshake.CapabilityFlags,4));
          AddToLog(llWarning,' Auth Plugin Name    : '+lHandshake.AuthPluginName);
          AddToLog(llWarning,' Auth Data Length    : '+IntToStr(lHandshake.AuthDataLen));
          AddToLog(llWarning,' Auth Data           : '+StrToHex(lHandshake.AuthData));
          AddToLog(llWarning,' Reserved            : '+StrToHex(lHandshake.Reserved));
        {$ENDIF}
  
        Authenticate(FBufferStream,lHandshake,FServerLogin,FServerPass,FServerDB);
  
        if(GetResponse(FBufferStream)=1) then
        begin          
          {$IFDEF REPDEBUG}
          AddToLog(llWarning,'Data : '+IntToStr(FBufferStream.Size)+' bytes');
          {$ENDIF}
          tmp:='';
          for i:=0 to FBufferStream.Size-1 do tmp:=tmp+Char(FBufferStream.ReadByte);
          {$IFDEF REPDEBUG}
            AddToLog(llWarning,' Auth Response HEX    : '+StrToHex(tmp));
          {$ENDIF}
          if(Ord(tmp[1])=$00) then 
          begin
            AddToLog(llWarning,'Authentication Successful!');
            Result:=1; 
          end;
        end;
      end;
    end;  	
end;
{******************************************************************************}
function Occurs(const AStr, ASep: string): integer;
var
  i, nSep: integer;
begin
  nSep:= 0;
  for i:= 1 to Length(AStr) do
    if AStr[i] = ASep then Inc(nSep);
  Result:= nSep;
end;
{******************************************************************************}
function SplitEnum(AStr: String;ASep: String): TStringArray;
var
  i, n: Integer;
  Strline,Strfield,tmp: String;
begin
  n:=Occurs(AStr, ASep);
{$warnings off}  
  SetLength(Result,n+1);
{$warnings on}  
  i:=0;
  Strline:= AStr;
  repeat
    if Pos(ASep, Strline) > 0 then
    begin
      Strfield:= Copy(Strline, 1, Pos(ASep,Strline) - 1);
      Strline:= Copy(Strline, Pos(ASep,Strline) + 1,Length(Strline) - pos(ASep,Strline));
    end
    else
    begin
      Strfield:= Strline;
      Strline:= '';
    end;
    Result[i]:= Strfield;
    Inc(i);
  until strline= '';
  if Result[High(Result)] = '' then SetLength(Result, Length(Result)-1);
  
  SetLength(Result,Length(Result)+1);
  for i:=Length(Result)-2 downto 0 do
  begin
	if(Result[i][1]='''') then 
	begin
		tmp:=Copy(Result[i],2,Length(Result[i])-2);
		Result[i]:=tmp;
	end;
	Result[i+1]:=Result[i];
  end;
  Result[0]:='';
end;      
{******************************************************************************}
function TMysqlReplicationClient.LoadSchema(): Integer;
var
  tmp: String;
  lSchemaID: Integer;
  lTableUID: Integer;
  lColUID: Integer;
  lTableID,lColId: Integer;
  lMaxTableID,lMaxColId: Integer;
  lLastTableName: String;
  lLastSchemaName: String;
  lRes: PMYSQL_RES;
  lRow: PMYSQL_ROW;  
begin
  AddToLog(llWarning,'Getting Table Schemas');
 
  lMaxTableId:=255;
  lMaxColId:=64;
    
  lTableID:=0;
  lColId:=0;
  
  lSchemaID:=1; // 0 is reserved
  lTableUID:=1; // 0 is reserved
  lColUID:=1;   // 0 is reserved
  
  lLastTableName:='';
  lLastSchemaName:='';
  
  SetLength(FBinLogTables,lMaxTableId+1);
      
 
	//if FSQL.Query('SELECT TABLE_NAME,COLUMN_NAME,COLLATION_NAME,CHARACTER_SET_NAME,COLUMN_COMMENT,DATA_TYPE,COLUMN_TYPE,COLUMN_KEY,TABLE_SCHEMA from information_schema.COLUMNS where TABLE_SCHEMA="'+FServerDB+'" order by TABLE_NAME,ORDINAL_POSITION') then
  if FSQL.Query('SELECT TABLE_NAME,COLUMN_NAME,COLLATION_NAME,CHARACTER_SET_NAME,COLUMN_COMMENT,DATA_TYPE,COLUMN_TYPE,COLUMN_KEY,TABLE_SCHEMA from information_schema.COLUMNS order by TABLE_SCHEMA,TABLE_NAME,ORDINAL_POSITION') then
	begin    
		lRes := FSQL.StoreResults;
		lRow := FSQL.FetchRow(lRes);
		while (lRow<>nil) do
		begin       
      if(lLastTableName<>GetField(lRow,0)) then
      begin
        {$IFDEF REPDEBUG}  
          AddToLog(llWarning,'New Table '+GetField(lRow,0));
        {$ENDIF}
        if(lLastTableName<>'') then
        begin
          SetLength(FBinLogTables[lTableId].Columns,lColId);  // Resize to effective length
          Inc(lTableId);
        end;
        if(lTableId=lMaxTableId) then
        begin
          // Dynamically increase size
          lMaxTableId:=lMaxTableID*2;
          SetLength(FBinLogTables,lMaxTableID+1);
        end;
        lLastTableName:=GetField(lRow,0);
        SetLength(FBinLogTables[lTableId].Columns,lMaxColId+1);
        FBinLogTables[lTableId].TableName:=GetField(lRow,0);
        FBinLogTables[lTableId].TableNameID:=lTableUID;
        Inc(lTableUID);

        if(lLastSchemaName<>GetField(lRow,8)) then
        begin
          if(lLastSchemaName<>'') then Inc(lSchemaID);
          lLastSchemaName:=GetField(lRow,8);          
        end;
        
        FBinLogTables[lTableId].SchemaName:=GetField(lRow,8);
        FBinLogTables[lTableId].SchemaNameID:=lSchemaID;
        
        FBinLogTables[lTableId].TableId:=-1; // Will be given by TABLE_MAP_EVENT
        lColId:=0;
      end;

      {$IFDEF REPDEBUG}
        AddToLog(llWarning,'Column '+GetField(lRow,1)+' : '+GetField(lRow,5)+' ('+GetField(lRow,6)+')');
      {$ENDIF}

      FBinLogTables[lTableId].Columns[lColId].ColumnName:=GetField(lRow,1);
      FBinLogTables[lTableId].Columns[lColId].ColumnNameID:=lColUID;
      Inc(lColUID);
      FBinLogTables[lTableId].Columns[lColId].ColumnType:=GetField(lRow,5);
      if(UpperCase(GetField(lRow,5))='ENUM') then 
      begin
        tmp:=Copy(GetField(lRow,6),6,Length(GetField(lRow,6))-6);
        {$IFDEF REPDEBUG}
          AddToLog(llWarning,'Enum list : '+tmp);		
        {$ENDIF}
        FBinLogTables[lTableId].Columns[lColId].ColumnEnum:=SplitEnum(tmp,',');
        {$IFDEF REPDEBUG}
          for i:=0 to Length(FBinLogTables[lTableId].Columns[lColId].ColumnEnum)-1 do
            AddToLog(llWarning,IntToStr(i)+' => '+FBinLogTables[lTableId].Columns[lColId].ColumnEnum[i]);
        {$ENDIF}
      end;
	 
      FBinLogTables[lTableId].Columns[lColId].ColumnFieldType:=FIELD_TYPE_UNKNOWN;  // Will be given by TABLE_MAP_EVENT
      FBinLogTables[lTableId].Columns[lColId].ColumnFieldMeta:='';  // Will be given by TABLE_MAP_EVENT
	  
      Inc(lColId);      
      if(lColId=lMaxColId) then
      begin
        // Dynamically increase size
        lMaxColId:=lMaxColId*2;
        SetLength(FBinLogTables[lTableId].Columns,lMaxColID+1);
      end;
      
      lRow := FSQL.FetchRow(lRes);
    end;
    FSQL.FreeResult(lRes);
	end;

  SetLength(FBinLogTables[lTableId].Columns,lColId); // Resize to effective length  
  SetLength(FBinLogTables,lTableId+1);
    
  AddToLog(llWarning,'Loaded Schema for '+IntToStr(Length(FBinLogTables))+' tables');
  GInstanceID:=DateTimeToUnix(Now);
  AddToLog(llWarning, 'New Instance ID = '+IntToHex(GInstanceID,4));

  {$IFDEF REPDEBUG}  
    for i:=0 to Length(FBinLogTables)-1 do
    begin
      AddToLog(llWarning,'Table '+FBinLogTables[i].TableName);
      for j:=0 to Length(FBinLogTables[i].Columns)-1 do
      begin
        AddToLog(llWarning,'=> '+FBinLogTables[i].Columns[j].ColumnName+' : '+FBinLogTables[i].Columns[j].ColumnType);
      end;
    end;
  {$ENDIF}
	Result:=1;
end;
{******************************************************************************}
function TMysqlReplicationClient.Init(ASlaveId: Integer): Integer;
var
  resu: Integer;
  lRes: PMYSQL_RES;
  lRow: PMYSQL_ROW;  
begin
	AddToLog(llWarning,'Enabling binlog checksum');
	
  FBinLogCRC32:=1;
	resu:=Execute('SET @master_binlog_checksum = @@global.binlog_checksum');
	if(resu<>1) then
	begin
		AddToLog(llError,'Error setting binlog checksum');
		Result:=0;
		Exit;
	end;  
	
  AddToLog(llWarning,'Getting Master Status');
	if FSQL.Query('SHOW MASTER STATUS') then
	begin    
		lRes := FSQL.StoreResults;
		lRow := FSQL.FetchRow(lRes);
		if lRow=nil then
		begin
			AddToLog(llError,'SHOW MASTER STATUS Didn''t return anything');
			Result:=0;
			Exit;		
		end;
		
		FBinLogFile:=GetField(lRow,0);
		FBinLogOffset:=StrToInt(GetField(lRow,1));
		AddToLog(llWarning,'MASTER BinLog File   : '+FBinLogFile);
		AddToLog(llWarning,'MASTER BinLog Offset : '+IntToStr(FBinLogOffset));
		FSQL.FreeResult(lRes);
	end;

  LoadSchema();

  AddToLog(llWarning,'Registering Slave');
  resu:=RegisterSlave(ASlaveId,FServerHost,FServerLogin,FServerPass);
  if(resu<>1) then
  begin
    AddToLog(llError,'Error registering as slave!');
    Exit;
  end;
  
  AddToLog(llWarning,'Requesting Binlog Dump from '+FBinLogFile+':'+IntToStr(FBinLogOffset));
  resu:=BinlogDump(FBinLogFile,FBinLogOffset); 
  if(resu<>1) then
  begin
    AddToLog(llError,'BinlogDump returned an error!');
    //Exit;
  end;	
	
	Result:=1;
 end;
{******************************************************************************}
function TMysqlReplicationClient.RegisterSlave(ASlaveId: Integer;Host,Login,Password: String): Integer;
var
  i,tmp: Integer;
  str: String;
  p : ^Byte;
begin
  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'TMysqlReplicationClient.RegisterSlave');
  {$ENDIF}
  Result:=0;
  
  FSlaveId:=ASlaveId;

  FBufferStream.Clear;
  // Reserve Header
  FBufferStream.WriteDWord($00000000);
  
  FBufferStream.WriteByte(COM_REGISTER_SLAVE);
  
  FBufferStream.WriteDWord(FSlaveId);  // Slave ID
  
  FBufferStream.WriteByte(Length(Host));  
  for i:=1 to Length(Host) do FBufferStream.WriteByte(Ord(Host[i]));

  FBufferStream.WriteByte(Length(Login));  
  for i:=1 to Length(Login) do FBufferStream.WriteByte(Ord(Login[i]));

  FBufferStream.WriteByte(Length(Password));  
  for i:=1 to Length(Password) do FBufferStream.WriteByte(Ord(Password[i]));
  
  FBufferStream.WriteWord(3306); // Port, leave empty
  
  FBufferStream.WriteDWord($00000000); // Rank, ignored
  
  FBufferStream.WriteDWord($00000000); // Master ID
 
  tmp:=FBufferStream.Size-4;  // Data size = packet size - header (4 Bytes)
  
  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'Sending Register Slave Command');
  {$ENDIF}
  
  p:=FBufferStream.Memory;
  
  p^:=(tmp AND $FF);
  tmp:=tmp shr 8;
  inc(p);
  p^:=(tmp AND $FF);
  tmp:=tmp shr 8;
  inc(p);
  p^:=(tmp AND $FF);
  inc(p);
  p^:=$00; // Packet seqnr => Initial = 0
    
  FBufferStream.Seek(0, soFromBeginning);  
  FTCPClient.IOHandler.Write(FBufferStream,FBufferStream.Size);
  
  {$IFDEF REPDEBUG}
    AddToLog(llWarning,'Packet Sent');
  {$ENDIF}

  if(GetResponse(FBufferStream)=1) then
  begin          
    {$IFDEF REPDEBUG}
    AddToLog(llWarning,'Data : '+IntToStr(FBufferStream.Size)+' bytes');
    {$ENDIF}
    str:='';
    for i:=0 to FBufferStream.Size-1 do str:=str+Char(FBufferStream.ReadByte);
    {$IFDEF REPDEBUG}
      AddToLog(llWarning,' Auth Response HEX    : '+StrToHex(str));
      AddToLog(llWarning,' Auth Response STR    : '+str);
    {$ENDIF}
    
    if(Ord(str[1])=$00) then 
    begin
      AddToLog(llWarning,'Slave Registration Successful!');
      Result:=1; 
    end;
  end;
end;
{******************************************************************************}
function TMysqlReplicationClient.BinlogDump(AFilename: String; APos: Integer): Integer;
var
  i,tmp: Integer;
  str: String;
  p : ^Byte;
begin
  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'TMysqlReplicationClient.BinlogDump');
  {$ENDIF}
  Result:=0;

  FBufferStream.Clear;
  // Reserve Header
  FBufferStream.WriteDWord($00000000);
  
  FBufferStream.WriteByte(COM_BINLOG_DUMP);
  
  FBufferStream.WriteDWord(APos);
  
  FBufferStream.WriteWord($0000);
  
  FBufferStream.WriteDWord(FSlaveId);  // Slave ID
  
  for i:=1 to Length(AFilename) do FBufferStream.WriteByte(Ord(AFilename[i]));
  //FBufferStream.WriteByte($00); 
 
  tmp:=FBufferStream.Size-4;  // Data size = packet size - header (4 Bytes)
  
  {$IFDEF REPDEBUG}
  AddToLog(llWarning,'Sending Binlog Dump Command');
  {$ENDIF}
  
  p:=FBufferStream.Memory;
  
  p^:=(tmp AND $FF);
  tmp:=tmp shr 8;
  inc(p);
  p^:=(tmp AND $FF);
  tmp:=tmp shr 8;
  inc(p);
  p^:=(tmp AND $FF);
  inc(p);
  p^:=$00; // Packet seqnr 
     
  FBufferStream.Seek(0, soFromBeginning);  
  FTCPClient.IOHandler.Write(FBufferStream,FBufferStream.Size);
  
  {$IFDEF REPDEBUG}
    AddToLog(llWarning,'Packet Sent');
  {$ENDIF}

  if(GetResponse(FBufferStream)=1) then
  begin          
    {$IFDEF REPDEBUG}
    AddToLog(llWarning,'Data : '+IntToStr(FBufferStream.Size)+' bytes');
    {$ENDIF}
    str:='';
    for i:=0 to FBufferStream.Size-1 do str:=str+Char(FBufferStream.ReadByte);
    
    {$IFDEF REPDEBUG}
      AddToLog(llWarning,' Response HEX    : '+StrToHex(str));
      AddToLog(llWarning,' Response STR    : '+str);
    {$ENDIF}
    
    if(Ord(str[1])=$00) then 
    begin
      AddToLog(llWarning,'Binlog Dump Command Successful!');
      Result:=1; 
    end;
  end;
end;
{******************************************************************************}


{******************************************************************************}
function TMysqlReplicationClient.GetEvent(var AResult: TBinLogEvent): Integer;
var
  i,j,tmp,tmp2,tidx,resu: Integer;
  q: QWord;
  str: String;
  lHeader: TBinLogEventHeader;
  lQueryPostHeader: TBinLogQueryEvent;
  lTableMap: TBinLogTableMapEvent;  
  lResultCode: Byte;
  
begin
  Result:=0;
  AResult.EventType:=0;  
  AResult.Schema:='';
  AResult.Table:='';
  
  AResult.Resize(8,8);
  FRowEvent.Resize(8,8);
  
  resu:=GetResponse(FBufferStream);
  if(resu<>1) then
  begin
    AResult.EventType:=666;   
    Result:=666;
    Exit;
  end;
  
  if(resu=1) then
  begin          
    {$IFDEF REPDEBUG}
      AddToLog(llWarning,'****************************** EVENT ******************************');
      AddToLog(llWarning,'Data : '+IntToStr(FBufferStream.Size)+' bytes');
    {$ENDIF}
    
    lResultCode:=FBufferStream.ReadByte;
    {$IFDEF REPDEBUG}
      AddToLog(llWarning,'lResultCode : '+IntToStr(lResultCode));
    {$ENDIF}
    
    if(lResultCode<>0) then
    begin
      AddToLog(llError,' RESULT CODE ERROR '+IntToStr(lResultCode));
      FBufferStream.Seek(0, soFromBeginning);
      str:='';
      SetLength(str,FBufferStream.Size);
      FBufferStream.Read(str[1],FBufferStream.Size);
      //for i:=0 to FBufferStream.Size-1 do str:=str+Char(FBufferStream.ReadByte);
      AddToLog(llWarning,' Event HEX     : '+StrToHex(str));
      //AddToLog(llWarning,' Event STR     : '+str);
      Exit;
    end;
     
    lHeader.Timestamp:=FBufferStream.ReadDWord;
    lHeader.EventType:=FBufferStream.ReadByte;
    lHeader.ServerId:=FBufferStream.ReadDWord;
    lHeader.EventSize:=FBufferStream.ReadDWord;
    lHeader.EventSize:=lHeader.EventSize-4*FBinLogCRC32;
    lHeader.LogPos:=FBufferStream.ReadDWord;
    lHeader.Flags:=FBufferStream.ReadWord;

    if(lHeader.LogPos>0) then FBinLogOffset:=lHeader.LogPos;

    {$IFDEF REPDEBUG}
      AddToLog(llWarning,' Binlog Header :');
      AddToLog(llWarning,' Timestamp     : '+DatetimeToStr(UnixToDateTime(lHeader.Timestamp)));    
      AddToLog(llWarning,' ServerId      : 0x'+IntToHex(lHeader.ServerId,8));
      AddToLog(llWarning,' EventSize     : '+IntToStr(lHeader.EventSize));
      AddToLog(llWarning,' LogPos        : '+IntToStr(lHeader.LogPos));
      AddToLog(llWarning,' Flags         : 0x'+IntToHex(lHeader.Flags,4));
      AddToLog(llWarning,' EventType     : 0x'+IntToHex(lHeader.EventType,2));
    {$ENDIF}
    str:='';
    SetLength(str,19);
    FBufferStream.Read(str[1],19);
    //for i:=0 to lHeader.EventSize-19-1 do  // Remove 19 bytes of header
    //  str:=str+Char(FBufferStream.ReadByte);
     
    {$IFDEF REPDEBUG} 
      AddToLog(llWarning,' Event HEX     : '+StrToHex(str));
      //AddToLog(llWarning,' Event STR     : '+str);
    {$ENDIF}
    
    FBufferStream.Seek(20, soFromBeginning); // Reset position to beginning of payload
        
    AResult.EventType:=lHeader.EventType;    
        
    case lHeader.EventType of
      $02:
      begin
        {$IFDEF REPDEBUG}
          AddToLog(llWarning,'>> QUERY_EVENT <<');
        {$ENDIF}
        lQueryPostHeader.SlaveProxyId:=FBufferStream.ReadDWord;
        lQueryPostHeader.ExecutionTime:=FBufferStream.ReadDWord;
        lQueryPostHeader.SchemaLength:=FBufferStream.ReadByte;
        lQueryPostHeader.ErrorCode:=FBufferStream.ReadWord;
        lQueryPostHeader.StatusVarsLength:=FBufferStream.ReadWord;

        lQueryPostHeader.StatusVars:='';
        for i:=0 to lQueryPostHeader.StatusVarsLength-1 do lQueryPostHeader.StatusVars:=lQueryPostHeader.StatusVars+Char(FBufferStream.ReadByte);

        lQueryPostHeader.Schema:='';
        for i:=0 to lQueryPostHeader.SchemaLength-1 do lQueryPostHeader.Schema:=lQueryPostHeader.Schema+Char(FBufferStream.ReadByte);
        
        FBufferStream.ReadByte; // 00

        tmp:=lHeader.EventSize-19-13-lQueryPostHeader.StatusVarsLength-lQueryPostHeader.SchemaLength-1;
        lQueryPostHeader.Query:='';
        for i:=0 to tmp-1 do lQueryPostHeader.Query:=lQueryPostHeader.Query+Char(FBufferStream.ReadByte);

        {$IFDEF REPDEBUG}
          AddToLog(llWarning,' => SlaveProxyId      = 0x'+IntToHex(lQueryPostHeader.SlaveProxyId,8));
          AddToLog(llWarning,' => ExecutionTime     = '+IntToStr(lQueryPostHeader.ExecutionTime));
          AddToLog(llWarning,' => SchemaLength      = '+IntToStr(lQueryPostHeader.SchemaLength));
          AddToLog(llWarning,' => ErrorCode         = 0x'+IntToHex(lQueryPostHeader.ErrorCode,4));
          AddToLog(llWarning,' => StatusVarsLength  = '+IntToStr(lQueryPostHeader.StatusVarsLength));
          AddToLog(llWarning,' => StatusVars        = '+StrToHex(lQueryPostHeader.StatusVars));
          AddToLog(llWarning,' => Schema            = '+StrToHex(lQueryPostHeader.Schema));
          AddToLog(llWarning,' => Query             = '+lQueryPostHeader.Query);
        {$ENDIF} 
         AResult.Data:=lQueryPostHeader.Query;
         
         if(Pos('ALTER',UpperCase(lQueryPostHeader.Query))>0) OR (Pos('CREATE',UpperCase(lQueryPostHeader.Query))>0) then
         begin
          AddToLog(llWarning,'Reloading Schema');
          LoadSchema();
         end;
         
      end;
      
      $04:
      begin
        {$IFDEF REPDEBUG}
          AddToLog(llWarning,'>> ROTATE_EVENT <<');
        {$ENDIF}
        
        q:=FBufferStream.ReadQWord;
        
        tmp:=lHeader.EventSize-19-8;
        str:='';
        SetLength(str,tmp);
        FBufferStream.Read(str[1],tmp);
        //for i:=0 to tmp-1 do 
        //  str:=str+Char(FBufferStream.ReadByte);

        FBinLogOffset:=q;
        FBinLogFile:=str;
        
        AddToLog(llWarning,'LOGROTATE :');
        AddToLog(llWarning,'MASTER BinLog File   : '+FBinLogFile);
        AddToLog(llWarning,'MASTER BinLog Offset : '+IntToStr(FBinLogOffset));
      end;
      $0F:
      begin
        {$IFDEF REPDEBUG}
          AddToLog(llWarning,'>> FORMAT_DESCRIPTION_EVENT <<');
        {$ENDIF}
        FBinLogFormatDescription.Version:=FBufferStream.ReadWord;
        FBinLogFormatDescription.MysqlVersion:='';
        
        SetLength(FBinLogFormatDescription.MysqlVersion,50);
        FBufferStream.Read(FBinLogFormatDescription.MysqlVersion[1],50);
        for i:=1 to 50 do // 50 Chars fixed
        begin
          if(Ord(FBinLogFormatDescription.MysqlVersion[i])=0) then
          begin
            SetLength(FBinLogFormatDescription.MysqlVersion,i-1);
            Break;
          end;
        end;
        FBinLogFormatDescription.CreateTimestamp:=FBufferStream.ReadDWord;
        FBinLogFormatDescription.HeaderLength:=FBufferStream.ReadByte;
        
        tmp:=lHeader.EventSize-19-57;
        SetLength(FBinLogFormatDescription.EventTypeHeaderLen,tmp+1);
        for i:=0 to tmp-1 do
          FBinLogFormatDescription.EventTypeHeaderLen[i+1]:=FBufferStream.ReadByte; // Indexed by BinlogEventType -1 ...
        
        {$IFDEF REPDEBUG}
          AddToLog(llWarning,' CreateTimestamp     : '+DatetimeToStr(UnixToDateTime(FBinLogFormatDescription.CreateTimestamp)));    
          AddToLog(llWarning,' binlog-version      : '+IntToStr(FBinLogFormatDescription.Version));
          AddToLog(llWarning,' MySQL Version       : '+FBinLogFormatDescription.MysqlVersion);
          AddToLog(llWarning,' Header Length       : '+IntToStr(FBinLogFormatDescription.HeaderLength));
          AddToLog(llWarning,' Event Header Length : ');
          for i:=0 to tmp-1 do AddToLog(llWarning,' => Event 0x'+IntToHex(i,2)+' : '+IntToStr(FBinLogFormatDescription.EventTypeHeaderLen[i]));
        {$ENDIF}
      end;
      $10:
      begin
        {$IFDEF REPDEBUG}
          AddToLog(llWarning,'>> XID_EVENT <<');
        {$ENDIF}
        tmp:=FBufferStream.ReadDWord;
        tmp2:=FBufferStream.ReadDWord;
        {$IFDEF REPDEBUG}
          AddToLog(llWarning,' => Transaction ID    = 0x'+IntToHex(tmp2,8)+IntToHex(tmp,8));
        {$ENDIF}
        AResult.Data:='0x'+IntToHex(tmp2,8)+IntToHex(tmp,8);
      end;
      $13:
      begin
        {$IFDEF REPDEBUG}
          AddToLog(llWarning,'>> TABLE_MAP_EVENT <<');
        {$ENDIF}
               
        lTableMap.TableId:=FBufferStream.ReadDWord;
        {$IFDEF REPDEBUG}
          AddToLog(llWarning,'HeaderLen =  '+IntToStr(FBinLogFormatDescription.EventTypeHeaderLen[$13]));
        {$ENDIF}
        if(FBinLogFormatDescription.EventTypeHeaderLen[$13]<>6) then
          lTableMap.TableIdHigh:=FBufferStream.ReadWord;
        
        lTableMap.Flags:=FBufferStream.ReadWord;

        tmp:=FBufferStream.ReadByte;        
        {$IFDEF REPDEBUG}
          AddToLog(llWarning,'Sch Len = '+IntToStr(tmp));
        {$ENDIF}
        lTableMap.SchemaName:='';
        for i:=0 to tmp-1 do lTableMap.SchemaName:=lTableMap.SchemaName+Char(FBufferStream.ReadByte);
        FBufferStream.ReadByte; // Null

        tmp:=FBufferStream.ReadByte;        
        {$IFDEF REPDEBUG}
          AddToLog(llWarning,'Tab Len = '+IntToStr(tmp));
        {$ENDIF}
        lTableMap.TableName:='';
        for i:=0 to tmp-1 do lTableMap.TableName:=lTableMap.TableName+Char(FBufferStream.ReadByte);
        FBufferStream.ReadByte; // Null
        
        tmp:=GetLenEncInteger(FBufferStream);
        
        SetLength(lTableMap.ColumnTypeDef,tmp);
        SetLength(lTableMap.ColumnMetaDef,tmp);
        
        for i:=0 to tmp-1 do lTableMap.ColumnTypeDef[i]:=FBufferStream.ReadByte;	
        
        {$IFDEF REPDEBUG}
          AddToLog(llWarning,'Loading Meta...');
        {$ENDIF}
        
        tmp:=GetLenEncInteger(FBufferStream);
        
        {$IFDEF REPDEBUG}
          AddtoLog(llWarning,'Meta String Length = '+IntToStr(tmp)+' bytes');
        {$ENDIF}
        
        for i:=0 to Length(lTableMap.ColumnTypeDef)-1 do
        begin			  		
          j:=lTableMap.ColumnTypeDef[i];  // 0, 1 or 2 bytes according to type
          if(j=FIELD_TYPE_BIT) OR (j=FIELD_TYPE_ENUM) OR (j=FIELD_TYPE_SET) or (j=FIELD_TYPE_NEWDECIMAL) or (j=FIELD_TYPE_DECIMAL) or (j=FIELD_TYPE_VARCHAR) or (j=FIELD_TYPE_VAR_STRING) or (j=FIELD_TYPE_STRING)
            then lTableMap.ColumnMetaDef[i]:=Char(FBufferStream.ReadByte)+Char(FBufferStream.ReadByte)
          else if(j=FIELD_TYPE_TINY_BLOB) OR (j=FIELD_TYPE_MEDIUM_BLOB) or (j=FIELD_TYPE_LONG_BLOB) or (j=FIELD_TYPE_BLOB) or (j=FIELD_TYPE_FLOAT) or (j=FIELD_TYPE_DOUBLE) or (j=FIELD_TYPE_TIMESTAMP2) or (j=FIELD_TYPE_DATETIME2)
            then lTableMap.ColumnMetaDef[i]:=Char(FBufferStream.ReadByte)
          else lTableMap.ColumnMetaDef[i]:='';
        end;
          
        {$IFDEF REPDEBUG}
          AddToLog(llWarning,' Table ID      = '+IntToStr(lTableMap.TableId));
          AddToLog(llWarning,' Flags         = 0x'+IntToHex(lTableMap.Flags,4));
          AddToLog(llWarning,' Schema Name   = '+lTableMap.SchemaName);
        {$ENDIF}
        
        tidx:=GetSchemaTableIdx(lTableMap.TableName,lTableMap.SchemaName);
        
        FBinLogTables[tidx].TableId:=lTableMap.TableId;
               
        {$IFDEF REPDEBUG}
          AddToLog(llWarning,' Table Name    = '+lTableMap.TableName+' (idx '+IntToStr(tmp)+')');
          AddToLog(llWarning,' Column Count  = '+IntToStr(Length(lTableMap.ColumnTypeDef)));
          AddToLog(llWarning,' Column Def    :');
        {$ENDIF}  

        AResult.Table:=lTableMap.TableName;
        AResult.Schema:=lTableMap.SchemaName;

        tmp:=Length(lTableMap.ColumnTypeDef);
        
        for i:=0 to tmp-1 do
        begin
          {$IFDEF REPDEBUG}
            AddToLog(llWarning,' => '+IntToStr(i)+' ('+FBinLogTables[tidx].Columns[i].ColumnName+') : '+IntToStr(lTableMap.ColumnTypeDef[i])+' Meta = '+StrToHex(lTableMap.ColumnMetaDef[i]));
          {$ENDIF}
          FBinLogTables[tidx].Columns[i].ColumnFieldType:=lTableMap.ColumnTypeDef[i];  // Set ColumnFieldType from TABLE_MAP_EVENT
          FBinLogTables[tidx].Columns[i].ColumnFieldMeta:=lTableMap.ColumnMetaDef[i];  // Set ColumnFieldMeta from TABLE_MAP_EVENT
        end;
      end;
      $17:
      begin
        {$IFDEF REPDEBUG}
          AddToLog(llWarning,'>> WRITE_ROWS_EVENTv1 <<');
          AddToLog(llWarning,'Stream Position = '+IntToStr(FBufferStream.Position));
        {$ENDIF}
        FRowEvent.TableId:=FBufferStream.ReadDWord;        
        if(FBinLogFormatDescription.EventTypeHeaderLen[$17]<>6) then  // TableIdHigh present in header
          FRowEvent.TableIdHigh:=FBufferStream.ReadWord;        
        FRowEvent.Flags:=FBufferStream.ReadWord;   
        
        FRowEvent.NumColumns:=GetLenEncInteger(FBufferStream);
        AResult.NumColumns:=FRowEvent.NumColumns;
        if(AResult.NumColumns>AResult.MaxColumns) then AResult.resize(AResult.NumColumns,AResult.MaxRows);
        
        if(FRowEvent.NumColumns>FRowEvent.MaxColumns) then FRowEvent.Resize(FRowEvent.NumColumns,FRowEvent.MaxRows); // Resize cols
        if(FRowEvent.NumColumns>AResult.MaxColumns) then AResult.Resize(FRowEvent.NumColumns,AResult.MaxRows);        
        
        tidx:=GetIdTableIdx(FRowEvent.TableId);
        AResult.Table:=FBinLogTables[tidx].TableName;
        AResult.Schema:=FBinLogTables[tidx].SchemaName;
        {$IFDEF REPDEBUG}
          AddToLog(llWarning,'TableID    = '+IntToStr(FRowEvent.TableId));
          AddToLog(llWarning,'Table Name = '+FBinLogTables[tidx].TableName);
          AddToLog(llWarning,'Flags      = '+IntToStr(FRowEvent.Flags));
          AddToLog(llWarning,'NumColumns = '+IntToSTr(FRowEvent.NumColumns));     
        {$ENDIF}
        
        FRowEvent.NumColPresentMap1:=GetBitmapToByteArray(FBufferStream,FRowEvent.ColPresentMap1,FRowEvent.NumColumns);
        
        {$IFDEF REPDEBUG}
          AddToLog(llWarning,'B');
        {$ENDIF}        
        
        str:='';
        for i:=0 to FRowEvent.NumColumns-1 do str:=str+IntToStr(FRowEvent.ColPresentMap1[i]);
        
        SetLength(AResult.ColumnTypes,FRowEvent.NumColumns);
        for i:=0 to FRowEvent.NumColumns-1 do
        begin
          AResult.ColumnTypes[i]:=FBinLogTables[tidx].Columns[i].ColumnFieldType;
        end;

        {$IFDEF REPDEBUG}
          AddToLog(llWarning,'C');
        {$ENDIF}        
		
        {$IFDEF REPDEBUG}
          AddToLog(llWarning,'ColPresentMap1 bit array = '+str);
          AddToLog(llWarning,'Stream Position = '+IntToStr(FBufferStream.Position));
        {$ENDIF}
        
        FRowEvent.NumRows:=0;
        AResult.NumRows:=0;
        
        while(FBufferStream.Position<lHeader.EventSize) do
        begin
          {$IFDEF REPDEBUG}
            AddToLog(llWarning,'row #'+IntToStr(FRowEvent.NumRows));
            AddToLog(llWarning,'Stream Position = '+IntToStr(FBufferStream.Position));
          {$ENDIF}
          
          if(AResult.NumRows=AResult.MaxRows) then AResult.Resize(AResult.MaxColumns,AResult.MaxRows*2);
          if(FRowEvent.NumRows=FRowEvent.MaxRows) then FRowEvent.Resize(FRowEvent.MaxColumns,FRowEvent.MaxRows*2);
          
          GetBitmapToByteArray(FBufferStream,FRowEvent.Rows[FRowEvent.NumRows].NullBitmap1,FRowEvent.NumColPresentMap1);
		  
          {$IFDEF REPDEBUG}
            str:='';
            for i:=0 to FRowEvent.NumColumns-1 do str:=str+IntToStr(FRowEvent.Rows[FRowEvent.NumRows].NullBitmap1[i]);
            AddToLog(llWarning,'NullBitmap1 bit array '+IntToStr(i)+' = '+str);
          {$ENDIF}
          for i:=0 to FRowEvent.NumColumns-1 do
          begin
            if(FRowEvent.Rows[FRowEvent.NumRows].NullBitmap1[i]=1) then
            begin
              AResult.Rows[AResult.NumRows].FieldsOut[i]:=FBinLogTables[tidx].Columns[i].ColumnName;
              AResult.Rows[AResult.NumRows].ValuesOut[i]:='NULL';
              {$IFDEF REPDEBUG}
                AddToLog(llWarning,'=> '+FBinLogTables[tidx].Columns[i].ColumnName+' = NULL');
              {$ENDIF}
            end
            else
            begin   
              //AddToLog(llWarning,'ElseIn');			
              str:=GetColumn(FBufferStream,FBinLogTables[tidx].Columns[i].ColumnFieldType,FBinLogTables[tidx].Columns[i].ColumnFieldMeta,FBinLogTables[tidx].Columns[i].ColumnEnum);
              AResult.Rows[AResult.NumRows].FieldsOut[i]:=FBinLogTables[tidx].Columns[i].ColumnName;
              AResult.Rows[AResult.NumRows].ValuesOut[i]:=str;
              {$IFDEF REPDEBUG}
                AddToLog(llWarning,'=> '+FBinLogTables[tidx].Columns[i].ColumnName+' = '+str);
              {$ENDIF}
            end;
          end;          
            
          Inc(FRowEvent.NumRows);
          Inc(AResult.NumRows);
        end;         
      end;
      $18:
      begin
        {$IFDEF REPDEBUG}
          AddToLog(llWarning,'>> UPDATE_ROWS_EVENTv1 <<');
          AddToLog(llWarning,'Stream Position = '+IntToStr(FBufferStream.Position));
        {$ENDIF}
        FRowEvent.TableId:=FBufferStream.ReadDWord;        
        if(FBinLogFormatDescription.EventTypeHeaderLen[$17]<>6) then
          FRowEvent.TableIdHigh:=FBufferStream.ReadWord;        
        FRowEvent.Flags:=FBufferStream.ReadWord;   
        
        FRowEvent.NumColumns:=GetLenEncInteger(FBufferStream);
        AResult.NumColumns:=FRowEvent.NumColumns;
        if(AResult.NumColumns>AResult.MaxColumns) then AResult.resize(AResult.NumColumns,AResult.MAxRows);
        
        if(FRowEvent.NumColumns>FRowEvent.MaxColumns) then FRowEvent.Resize(FRowEvent.NumColumns,FRowEvent.MaxRows); // Resize cols
        if(FRowEvent.NumColumns>AResult.MaxColumns) then AResult.Resize(FRowEvent.NumColumns,AResult.MaxRows);        
        
        tidx:=GetIdTableIdx(FRowEvent.TableId);
        AResult.Table:=FBinLogTables[tidx].TableName;
        AResult.Schema:=FBinLogTables[tidx].SchemaName;
        {$IFDEF REPDEBUG}
          AddToLog(llWarning,'TableID    = '+IntToStr(FRowEvent.TableId));
          AddToLog(llWarning,'Table Name = '+FBinLogTables[tidx].TableName);
          AddToLog(llWarning,'Flags      = '+IntToStr(FRowEvent.Flags));
          AddToLog(llWarning,'NumColumns = '+IntToSTr(FRowEvent.NumColumns));
        {$ENDIF}
        
        FRowEvent.NumColPresentMap1:=GetBitmapToByteArray(FBufferStream,FRowEvent.ColPresentMap1,FRowEvent.NumColumns);
        FRowEvent.NumColPresentMap2:=GetBitmapToByteArray(FBufferStream,FRowEvent.ColPresentMap2,FRowEvent.NumColumns);
        
        {$IFDEF REPDEBUG}
          str:='';
          for i:=0 to FRowEvent.NumColumns-1 do str:=str+IntToStr(FRowEvent.ColPresentMap1[i]);
          AddToLog(llWarning,'ColPresentMap1 bit array = '+str);
          str:='';
          for i:=0 to FRowEvent.NumColumns-1 do str:=str+IntToStr(FRowEvent.ColPresentMap2[i]);        
          AddToLog(llWarning,'ColPresentMap2 bit array = '+str);
          AddToLog(llWarning,'Stream Position = '+IntToStr(FBufferStream.Position));
        {$ENDIF}
        
        SetLength(AResult.ColumnTypes,FRowEvent.NumColumns);
        for i:=0 to FRowEvent.NumColumns-1 do
        begin
          AResult.ColumnTypes[i]:=FBinLogTables[tidx].Columns[i].ColumnFieldType;
        end;        
        
        FRowEvent.NumRows:=0;
        AResult.NumRows:=0;
        
        while(FBufferStream.Position<lHeader.EventSize) do
        begin
          {$IFDEF REPDEBUG}
            AddToLog(llWarning,'row #'+IntToStr(FRowEvent.NumRows));
            AddToLog(llWarning,'Stream Position = '+IntToStr(FBufferStream.Position));
          {$ENDIF}
          
          if(AResult.NumRows=AResult.MaxRows) then AResult.Resize(AResult.MaxColumns,AResult.MaxRows*2);
          if(FRowEvent.NumRows=FRowEvent.MaxRows) then FRowEvent.Resize(FRowEvent.MaxColumns,FRowEvent.MaxRows*2);

          GetBitmapToByteArray(FBufferStream,FRowEvent.Rows[FRowEvent.NumRows].NullBitmap1,FRowEvent.NumColPresentMap1);
          
          {$IFDEF REPDEBUG}
            str:='';
            for i:=0 to FRowEvent.NumColPresentMap1-1 do str:=str+IntToStr(FRowEvent.Rows[FRowEvent.NumRows].NullBitmap1[i]);
            AddToLog(llWarning,'NullBitmap1 array = '+str);
          {$ENDIF}
          for i:=0 to FRowEvent.NumColumns-1 do
          begin
            //AddToLog(llWarning,'Column #'+IntToStr(i)+' ('+FBinLogTables[tidx].Columns[i].ColumnName+') type = '+IntToStr(FBinLogTables[tidx].Columns[i].ColumnFieldType));
            
           if(FRowEvent.Rows[FRowEvent.NumRows].NullBitmap1[i]=1) then
            begin
              AResult.Rows[AResult.NumRows].FieldsIn[i]:=FBinLogTables[tidx].Columns[i].ColumnName;
              AResult.Rows[AResult.NumRows].ValuesIn[i]:='NULL';
              {$IFDEF REPDEBUG}
                AddToLog(llWarning,'Match => '+FBinLogTables[tidx].Columns[i].ColumnName+' = NULL');
              {$ENDIF}
            end
            else
            begin              
              str:=GetColumn(FBufferStream,FBinLogTables[tidx].Columns[i].ColumnFieldType,FBinLogTables[tidx].Columns[i].ColumnFieldMeta,FBinLogTables[tidx].Columns[i].ColumnEnum);
              AResult.Rows[AResult.NumRows].FieldsIn[i]:=FBinLogTables[tidx].Columns[i].ColumnName;
              AResult.Rows[AResult.NumRows].ValuesIn[i]:=str;
              {$IFDEF REPDEBUG}
                AddToLog(llWarning,'Match => '+FBinLogTables[tidx].Columns[i].ColumnName+' = '+str);
                AddToLog(llWarning,'a');
              {$ENDIF}
            end;
          end;          
   
          GetBitmapToByteArray(FBufferStream,FRowEvent.Rows[FRowEvent.NumRows].NullBitmap2,FRowEvent.NumColPresentMap2);
          
          {$IFDEF REPDEBUG}
            str:='';
            for i:=0 to FRowEvent.NumColPresentMap2-1 do str:=str+IntToStr(FRowEvent.Rows[FRowEvent.NumRows].NullBitmap2[i]);
            AddToLog(llWarning,'NullBitmap2 bit array = '+str);
           {$ENDIF}
          for i:=0 to FRowEvent.NumColumns-1 do
          begin
            //AddToLog(llWarning,'Column #'+IntToStr(i)+' ('+FBinLogTables[tidx].Columns[i].ColumnName+') type = '+IntToStr(FBinLogTables[tidx].Columns[i].ColumnFieldType));
            
           if(FRowEvent.Rows[FRowEvent.NumRows].NullBitmap2[i]=1) then
            begin
              AResult.Rows[AResult.NumRows].FieldsOut[i]:=FBinLogTables[tidx].Columns[i].ColumnName;
              AResult.Rows[AResult.NumRows].ValuesOut[i]:='NULL';
              {$IFDEF REPDEBUG}
                AddToLog(llWarning,'Change => '+FBinLogTables[tidx].Columns[i].ColumnName+' = NULL');
              {$ENDIF}
            end
            else
            begin              
              str:=GetColumn(FBufferStream,FBinLogTables[tidx].Columns[i].ColumnFieldType,FBinLogTables[tidx].Columns[i].ColumnFieldMeta,FBinLogTables[tidx].Columns[i].ColumnEnum);
              AResult.Rows[AResult.NumRows].FieldsOut[i]:=FBinLogTables[tidx].Columns[i].ColumnName;
              AResult.Rows[AResult.NumRows].ValuesOut[i]:=str;
              {$IFDEF REPDEBUG}
                AddToLog(llWarning,'Change => '+FBinLogTables[tidx].Columns[i].ColumnName+' = '+str);
              {$ENDIF}
            end;
          end;          
          Inc(FRowEvent.NumRows);
          Inc(AResult.NumRows);
        end;          
      end;
      $19:
      begin
        {$IFDEF REPDEBUG}
          AddToLog(llWarning,'>> DELETE_ROWS_EVENTv1 <<');
          AddToLog(llWarning,'Stream Position = '+IntToStr(FBufferStream.Position));
        {$ENDIF}
        FRowEvent.TableId:=FBufferStream.ReadDWord;        
        if(FBinLogFormatDescription.EventTypeHeaderLen[$17]<>6) then
          FRowEvent.TableIdHigh:=FBufferStream.ReadWord;        
        FRowEvent.Flags:=FBufferStream.ReadWord;   
        
        FRowEvent.NumColumns:=GetLenEncInteger(FBufferStream);
        AResult.NumColumns:=FRowEvent.NumColumns;
        if(AResult.NumColumns>AResult.MaxColumns) then AResult.resize(AResult.NumColumns,AResult.MAxRows);
        
        if(FRowEvent.NumColumns>FRowEvent.MaxColumns) then FRowEvent.Resize(FRowEvent.NumColumns,FRowEvent.MaxRows); // Resize cols
        if(FRowEvent.NumColumns>AResult.MaxColumns) then AResult.Resize(FRowEvent.NumColumns,AResult.MaxRows);
        
        tidx:=GetIdTableIdx(FRowEvent.TableId);
        AResult.Table:=FBinLogTables[tidx].TableName;
        AResult.Schema:=FBinLogTables[tidx].SchemaName;
        {$IFDEF REPDEBUG}
          AddToLog(llWarning,'TableID    = '+IntToStr(FRowEvent.TableId));
          AddToLog(llWarning,'Table Name = '+FBinLogTables[tidx].TableName);
          AddToLog(llWarning,'Flags      = '+IntToStr(FRowEvent.Flags));
          AddToLog(llWarning,'NumColumns = '+IntToSTr(FRowEvent.NumColumns));
        {$ENDIF}
        
        FRowEvent.NumColPresentMap1:=GetBitmapToByteArray(FBufferStream,FRowEvent.ColPresentMap1,FRowEvent.NumColumns);
        
        {$IFDEF REPDEBUG}
          str:='';
          for i:=0 to FRowEvent.NumColumns-1 do str:=str+IntToStr(FRowEvent.ColPresentMap1[i]);
          AddToLog(llWarning,'ColPresentMap1 bit array = '+str);
          AddToLog(llWarning,'Stream Position = '+IntToStr(FBufferStream.Position));
        {$ENDIF}
        
        SetLength(AResult.ColumnTypes,FRowEvent.NumColumns);
        for i:=0 to FRowEvent.NumColumns-1 do
        begin
          AResult.ColumnTypes[i]:=FBinLogTables[tidx].Columns[i].ColumnFieldType;
        end;        
        
        FRowEvent.NumRows:=0;
        AResult.NumRows:=0;
        
        while(FBufferStream.Position<lHeader.EventSize) do
        begin
          {$IFDEF REPDEBUG}
            AddToLog(llWarning,'row #'+IntToStr(FRowEvent.NumRows));
            AddToLog(llWarning,'Stream Position = '+IntToStr(FBufferStream.Position));
          {$ENDIF}
          if(AResult.NumRows=AResult.MaxRows) then AResult.Resize(AResult.MaxColumns,AResult.MaxRows*2);
          if(FRowEvent.NumRows=FRowEvent.MaxRows) then FRowEvent.Resize(FRowEvent.MaxColumns,FRowEvent.MaxRows*2);
          
          GetBitmapToByteArray(FBufferStream,FRowEvent.Rows[FRowEvent.NumRows].NullBitmap1,FRowEvent.NumColPresentMap1);
          {$IFDEF REPDEBUG}
            for i:=0 to FRowEvent.NumColPresentMap1-1 do
              AddToLog(llWarning,'NullBitmap1 bit '+IntToStr(i)+' = '+IntToStr(FRowEvent.Rows[FRowEvent.NumRows].NullBitmap1[i]));
          {$ENDIF}
            
          for i:=0 to FRowEvent.NumColumns-1 do
          begin
            //AddToLog(llWarning,'Column #'+IntToStr(i)+' ('+FBinLogTables[tidx].Columns[i].ColumnName+') type = '+IntToStr(FBinLogTables[tidx].Columns[i].ColumnFieldType));
            
            if(FRowEvent.Rows[FRowEvent.NumRows].NullBitmap1[i]=1) then
            begin
              AResult.Rows[AResult.NumRows].FieldsIn[i]:=FBinLogTables[tidx].Columns[i].ColumnName;
              AResult.Rows[AResult.NumRows].ValuesIn[i]:='NULL';
              {$IFDEF REPDEBUG}
                AddToLog(llWarning,'Match => '+FBinLogTables[tidx].Columns[i].ColumnName+' = NULL');
              {$ENDIF}
            end
            else
            begin              
              str:=GetColumn(FBufferStream,FBinLogTables[tidx].Columns[i].ColumnFieldType,FBinLogTables[tidx].Columns[i].ColumnFieldMeta,FBinLogTables[tidx].Columns[i].ColumnEnum);
              AResult.Rows[AResult.NumRows].FieldsIn[i]:=FBinLogTables[tidx].Columns[i].ColumnName;
              AResult.Rows[AResult.NumRows].ValuesIn[i]:=str;
              {$IFDEF REPDEBUG}
                AddToLog(llWarning,'Match => '+FBinLogTables[tidx].Columns[i].ColumnName+' = '+str);
                AddToLog(llWarning,'b');
              {$ENDIF}
            end;
          end;          
          {$IFDEF REPDEBUG}
            AddToLog(llWarning,'=> DELETE');  
          {$ENDIF}
          Inc(FRowEvent.NumRows);
          Inc(AResult.NumRows);
        end;  
      end;
      else
      begin
        AddToLog(llWarning,'!! Unhandled Event '+IntToHex(lHeader.EventType,2)+' !!');
      end;
    end;    
  end;  
end;

end.
