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
unit ClassMySQL;

{$IFDEF FPC}
  {$MODE Delphi}
{$ENDIF}

interface

uses
  Declarations, MySQL, SysUtils, ClassDebug, DateUtils, SyncObjs;

type
  TMySQL = class
  private
    FMySQL: PMYSQL;
    FSection: UTF8String;
    FModule: TObject;
    FLastNilError: TDateTime;
    FLastQuery: TDateTime;
    FCritical: TCriticalSection;
  public
    constructor Create;
    destructor Destroy; override;
    procedure SetIniSection(ASection: String);
    procedure Connect;
    
    function Query(AValue: UTF8String; out AResult: PMYSQL_RES; out ALastId: Integer): Boolean; overload;
    function Query(AValue: UTF8String): Boolean; overload;
    function StoreResults: PMYSQL_RES;
    function UseResults: PMYSQL_RES;
    function QueryNoRes(AValue: UTF8String): Boolean;
    procedure FreeResult(AResult: PMYSQL_RES);
    procedure ResetRow(AResult: PMYSQL_RES);
    function FetchRow(AResult: PMYSQL_RES): PMYSQL_ROW;
    function NumRows(AResult: PMYSQL_RES): Integer;
    function NumFields(AResult: PMYSQL_RES): Integer;
    function InsertId: Integer;
    function UpdatedRows: Integer;

    property Module: TObject read FModule write FModule;
    property LastQuery: TDateTime read FLastQuery;
  end;

implementation

uses
  Classes, ClassModule;

{ TMySQL }

{*****************************************************************************}
procedure TMySQL.SetIniSection(ASection: String);
begin
  FSection:=ASection;
end;
{*****************************************************************************}
procedure TMySQL.Connect;
var
 lHost, lUser, lPass, lDatabase: UTF8String;
begin
  
  AddToLog(llDebug, 'TMySQL::Connect');

  if FMySQL = nil then
    FMySQL := mysql_init(nil);

  if FMySQL <> nil then
  begin
    lHost := UTF8Encode(TModule(FModule).GetOption(FSection, 'host'));
    lUser := UTF8Encode(TModule(FModule).GetOption(FSection, 'login'));
    lPass := UTF8Encode(TModule(FModule).GetOption(FSection, 'password'));
    lDatabase := UTF8Encode(TModule(FModule).GetOption(FSection, 'database'));
    AddToLog(llDebug, 'TMySQL::Connect to '+lHost+' '+lUser+' '+lPass+' '+lDatabase);
    mysql_options(FMySQL, MYSQL_OPT_CONNECT_TIMEOUT, '5');
    if mysql_real_connect(FMySQL, PChar(lHost), PChar(lUser),
        PChar(lPass), PChar(lDatabase), 0, nil, 0) <> nil then
    begin
      if mysql_select_db(FMySQL, PChar(lDatabase)) <> 0 then
      begin
        AddToLog(llWarning, 'TMySQL::Connect Failed '+mysql_error(FMySQL));
   		  mysql_close(FMySQL);
        FMySQL := nil;
      end
      else
      begin
        AddToLog(llDebug, 'TMySQL::Connect OK');
        //mysql_query(FMySQL, 'SET CHARACTER SET ''utf8''');
        //mysql_query(FMySQL, 'SET NAMES UTF8');
        mysql_set_character_set(FMySQL, 'utf8mb4');
      end;
    end
    else
    begin
      AddToLog(llWarning, 'TMySQL::Connect Failed '+mysql_error(FMySQL));
      //mysql_close(FMySQL);
      FMySQL := nil;
    end;
  end
  else
    AddToLog(llWarning, 'TMySQL::Connect MySQL is NIL...');
end;
{*****************************************************************************}
constructor TMySQL.Create;
begin
  FCritical := TCriticalSection.Create;
  FSection := 'MySQL';
  FMySQL := mysql_init(nil);
  FLastNilError := 0;
  FLastQuery := Now;
end;
{*****************************************************************************}
destructor TMySQL.Destroy;
begin
  if FMySQL <> nil then
  begin
    mysql_close(FMySQL);
    FMySQL := nil;
  end;
  FreeAndNil(FCritical);
  inherited;
end;
{*****************************************************************************}
procedure TMySQL.ResetRow(AResult: PMYSQL_RES);
begin
	mysql_data_seek(AResult,0);
end;
{*****************************************************************************}
function TMySQL.FetchRow(AResult: PMYSQL_RES): PMYSQL_ROW;
begin
  FLastQuery := Now;
  if FMySQL <> nil then
    result := mysql_fetch_row(AResult)
  else
    result := nil;
end;
{*****************************************************************************}
procedure TMySQL.FreeResult(AResult: PMYSQL_RES);
begin
  AddToLog(llDebug, 'MySQL: Free Result');
  if AResult <> nil then
    mysql_free_result(AResult);
end;
{*****************************************************************************}
function TMySQL.InsertId: Integer;
begin
  result := mysql_insert_id(FMySQL);
end;
{*****************************************************************************}
function TMySQL.NumRows(AResult: PMYSQL_RES): Integer;
begin
  if AResult <> nil then
    result := mysql_num_rows(AResult)
  else
    result := 0;
end;
{*****************************************************************************}
function TMySQL.NumFields(AResult: PMYSQL_RES): Integer;
begin
  if AResult <> nil then
    result := mysql_num_fields(AResult)
  else
    result := 0;
end;
{*****************************************************************************}
function TMySQL.Query(AValue: UTF8String; out AResult: PMYSQL_RES; out ALastId: Integer): Boolean;
begin
  FLastQuery := Now;
  AResult := nil;

  FCritical.Enter;

  try
    if FMySQL = nil then
      Connect;
    //AddToLog(llError, 'Sending query '+AValue);

    if FMySQL = nil then
    begin
      if MinutesBetween(Now, FLastNilError) > 1 then
      begin
        AddToLog(llError, 'MySQL is NULL, kabouuum');
        FLastNilError := Now;
      end;
      result := false;
    end
    else
    begin
      if mysql_ping(FMySQL) <> 0 then
      begin
        AddToLog(llWarning, 'MySQL: Ping Failed');
        try
          //mysql_close(FMySQL);
          FMySQL := nil;
        except
        end;
        Connect;
      end;

      if FMySQL <> nil then
      begin
        AddToLog(llDebug, 'MySQL: Launching query');
        result := mysql_query(FMySQL, PChar(AValue)) = 0;
        if not result then
          AddToLog(llError, 'MySQL: '+mysql_error(FMySQL) + ' for '+AValue);
      end
      else
        result := false;
    end;

    if result then
    begin
      AddToLog(llDebug, 'MySQL: Query was fine '+AValue);
      AValue := UpperCase(AValue);
      if (Pos('INSERT ', AValue) > 0) and (Pos('INSERT ', AValue) < 10) then
        ALastId := mysql_insert_id(FMySQL)
      else if (Pos('SELECT ', AValue) > 0) and (Pos('SELECT ', AValue) < 10) then
      begin
        AddToLog(llDebug, 'MySQL: Store Result');
        AResult := mysql_store_result(FMySQL);
      end;
    end;
  finally
    FCritical.Leave;
  end;
end;
{*****************************************************************************}
function TMySQL.Query(AValue: UTF8String): Boolean;
begin
  FLastQuery := Now;
  FCritical.Enter;

  try
    if FMySQL = nil then
      Connect;
    //AddToLog(llError, 'Sending query '+AValue);

    if FMySQL = nil then
    begin
      if MinutesBetween(Now, FLastNilError) > 1 then
      begin
        AddToLog(llError, 'MySQL is NULL, kabouuum');
        FLastNilError := Now;
      end;
      result := false;
    end
    else
    begin
      if mysql_ping(FMySQL) <> 0 then
      begin
        AddToLog(llWarning, 'MySQL: Ping Failed');
        try
          //mysql_close(FMySQL);
          FMySQL := nil;
        except
        end;
        Connect;
      end;

      if FMySQL <> nil then
      begin
        AddToLog(llDebug, 'MySQL: Launching query');
        //result := mysql_query(FMySQL, PChar(AValue)) = 0;
        //WriteLn('Query = '+AValue);
        result := mysql_query(FMySQL, PChar(AValue)) = 0;
        if not result then
          AddToLog(llError, 'MySQL: '+mysql_error(FMySQL) + ' for '+AValue);
      end
      else
        result := false;
    end;
  finally
    FCritical.Leave;
  end;
end;
{*****************************************************************************}
function TMySQL.QueryNoRes(AValue: UTF8String): Boolean;
begin
  FLastQuery := Now;

  FCritical.Enter;

  try
    if FMySQL = nil then
      Connect;
    //AddToLog(llError, 'Sending query '+AValue);

    if FMySQL = nil then
    begin
      if MinutesBetween(Now, FLastNilError) > 1 then
      begin
        AddToLog(llError, 'MySQL is NULL, kabouuum');
        FLastNilError := Now;
      end;
      result := false;
    end
    else
    begin
      if mysql_ping(FMySQL) <> 0 then
      begin
        AddToLog(llWarning, 'MySQL: Ping Failed');
        try
          //mysql_close(FMySQL);
          FMySQL := nil;
        except
        end;
        Connect;
      end;

      if FMySQL <> nil then
      begin
        result := mysql_query(FMySQL, PChar(AValue)) = 0;
        if not result then
          AddToLog(llError, 'MySQL: '+mysql_error(FMySQL) + ' for '+AValue);
      end
      else
        result := false;
    end;
  finally
    FCritical.Leave;
  end;
end;
{*****************************************************************************}
function TMySQL.StoreResults: PMYSQL_RES;
begin
  result := mysql_store_result(FMySQL);
end;
{*****************************************************************************}
function TMySQL.UpdatedRows: Integer;
begin
  if FMySQL <> nil then
    result := mysql_affected_rows(FMySQL)
  else
    result := 0;
end;
{*****************************************************************************}
function TMySQL.UseResults: PMYSQL_RES;
begin
  result := mysql_use_result(FMySQL);
end;
{*****************************************************************************}

end.
