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
unit ClassModule;

{$IFDEF FPC}
  {$MODE Delphi}
{$ENDIF}

interface

uses
  Declarations, Classes, SysUtils, IniFiles, ClassMySQL;

type
  TModule = class
  protected
    FOptions: TIniFile;
  public
    constructor Create;
    destructor Destroy; override;
    function GetOption(ASection, AName: UTF8String): UTF8String;
  published
  end;

implementation


{ TModule }

{******************************************************************************}
constructor TModule.Create;
var
 lValue: UTF8String;
begin
  FOptions := TIniFile.Create('/etc/kelare/fidelid-repserver.conf'); 
  
  lValue := FOptions.ReadString('module', 'loglevel', 'warning');
  if (lValue = 'error') then
    GLogLevel := llError
  else if (lValue = 'debug') then 
    GLogLevel := llDebug
  else if (lValue = 'warning') then
    GLogLevel := llWarning
end;
{******************************************************************************}
destructor TModule.Destroy;
begin
  FreeAndNil(FOptions);
  inherited;
end;
{******************************************************************************}
function TModule.GetOption(ASection, AName: UTF8String): UTF8String;
begin
  result := FOptions.ReadString(ASection, AName, '');
end;
{******************************************************************************}

end.
