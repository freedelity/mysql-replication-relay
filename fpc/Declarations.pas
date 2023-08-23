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
unit Declarations;

{$IFDEF FPC}
  {$MODE Delphi}
{$ENDIF}

interface

uses
	SysUtils, Classes;

const
	CONST_ONE = 1;

type
	TLogLevel = (llDebug = 0, llWarning = 1, llError = 2);

var
	GLogLevel: TLogLevel = llWarning;
  GInstanceID: DWord; 

implementation

end.
