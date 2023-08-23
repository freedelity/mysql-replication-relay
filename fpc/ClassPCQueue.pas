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
unit ClassPCQueue;

{$IFDEF FPC}
  {$MODE Delphi}
{$ENDIF}

interface

uses
 SysUtils, Classes, Math;

const

	CONST_PCQ_INITIAL_SIZE = 10000000;

type

  TPCQueue = class
  private
    FPointerTable: Array of Pointer;
    FMaxSize: Integer;
    FCurSize: Integer;
    FHeadIdx: Integer;
    FTailIdx: Integer;

  public
    constructor Create; 
    destructor Destroy; override;  

    function Count(): Integer;
    function Max(): Integer;
    function HeadIdx(): Integer;
    function TailIdx(): Integer;
    function Push(AData: Pointer):Pointer;
    function Pop():Pointer;
    
  end;

implementation


{ TPCQueue }

{*****************************************************************************}
constructor TPCQueue.Create;
begin
  FMaxSize:=CONST_PCQ_INITIAL_SIZE;
  SetLength(FPointerTable,FMaxSize);
  FCurSize:=0;
  FHeadIdx:=0;
  FTailIdx:=0;
end;
{*****************************************************************************}
destructor TPCQueue.Destroy;
begin
  SetLength(FPointerTable,0);
  inherited;
end;
{*****************************************************************************}
function TPCQueue.Count(): Integer;
begin
  Result:=FCurSize;
end;
{*****************************************************************************}
function TPCQueue.Max(): Integer;
begin
  Result:=FMaxSize;
end;
{*****************************************************************************}
function TPCQueue.HeadIdx(): Integer;
begin
  Result:=FHeadIdx;
end;
{*****************************************************************************}
function TPCQueue.TailIdx(): Integer;
begin
  Result:=FTailIdx;
end;
{*****************************************************************************}
function TPCQueue.Push(AData: Pointer):Pointer;
var
  i,oldsz,lPreHead: Integer;
begin
  if(FCurSize>=FMaxSize) then
  begin
    oldsz:=FMaxSize;
    FMaxSize:=FMaxSize+(FMaxSize); // x2, guarantees easy handling of rollover
    SetLength(FPointerTable,FMaxSize);
    lPreHead:=(FHeadIdx-1) MOD oldsz;
    if(lPreHead<FTailIdx) then // Rollover, we need to move some data
    begin
      // Indexes 0 -> FHeadIdx need to be moved to oldsz->
      for i:=0 to lPreHead do
        FPointerTable[oldsz+i]:=FPointerTable[i];
      FHeadIdx:=(FHeadIdx+oldsz) MOD FMaxSize;
    end;
  end;
  FPointerTable[FHeadIdx]:=AData;
  FHeadIdx:=(FHeadIdx+1) MOD FMaxSize;
  Inc(FCurSize);
  Result:=AData;
end;
{*****************************************************************************}
function TPCQueue.Pop():Pointer;
begin
  if(FCurSize=0) then Result:=Nil
  else
  begin
    Result:=FPointerTable[FTailIdx];  
    FTailIdx:=(FTailIdx+1) MOD FMaxSize;
    Dec(FCurSize);
  end;
  
end;
{*****************************************************************************}
end.
