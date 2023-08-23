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
unit HFNV1A;
interface

function FNV1A(AVal:LongWord): Longword; overload; inline;
function FNV1A(AVal:LongWord; AVal2: LongWord): Longword; overload; inline;
function FNV1A(AVal:String): Longword; overload; inline;
function FNV1AQ(AVal:String): QWord; overload;

implementation

{******************************************************************************}       
function FNV1A32(AByte: Byte; ASeed: Longword): Longword; inline;
begin
  Result:=(AByte XOR ASeed)*$01000193;
end;

function FNV1A64(AByte: Byte; ASeed: QWord): QWord; inline;
begin
  Result:=(AByte XOR ASeed)*$00000100000001B3;
end;

function FNV1A(AVal:LongWord): Longword; overload; inline;
var
  hash,tmp:LongWord;
begin
  tmp:=AVal;
  hash:=FNV1A32((tmp AND $FF),$811c9dc5);
  tmp:=tmp SHR 8;
  hash:=FNV1A32((tmp AND $FF),hash);
  tmp:=tmp SHR 8;
  hash:=FNV1A32((tmp AND $FF),hash);
  tmp:=tmp SHR 8;
  Result:=FNV1A32((tmp AND $FF),hash);
end;

function FNV1A(AVal:LongWord; AVal2: LongWord): Longword; overload; inline;
var
  hash,tmp:LongWord;
begin
  tmp:=AVal;
  hash:=FNV1A32((tmp AND $FF),$811c9dc5);
  tmp:=tmp SHR 8;
  hash:=FNV1A32((tmp AND $FF),hash);
  tmp:=tmp SHR 8;
  hash:=FNV1A32((tmp AND $FF),hash);
  tmp:=tmp SHR 8;
  hash:=FNV1A32((tmp AND $FF),hash);
  tmp:=AVal2;
  hash:=FNV1A32((tmp AND $FF),hash);
  tmp:=tmp SHR 8;
  hash:=FNV1A32((tmp AND $FF),hash);
  tmp:=tmp SHR 8;
  hash:=FNV1A32((tmp AND $FF),hash);
  tmp:=tmp SHR 8;
  Result:=FNV1A32((tmp AND $FF),hash);
end;      

function FNV1A(AVal:String): Longword; overload; inline;
var
  i: Integer;
  hash:LongWord;
begin
  hash:=$811c9dc5;
  for i:=0 to Length(AVal)-1 do
  begin
    hash:=FNV1A32(Ord(AVal[i]),hash);
  end;
  Result:=hash;
end;   

function FNV1AQ(AVal:String): QWord; overload;
var
  i: Integer;
  hash:QWord;
begin
  hash:=QWord($cbf29ce484222325);
  for i:=0 to Length(AVal)-1 do
  begin
    hash:=FNV1A64(Ord(AVal[i]),hash);
  end;
  Result:=hash;
end;   

end.