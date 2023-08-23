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

unit ClassDebug;

{$IFDEF FPC}
  {$MODE Delphi}
{$ENDIF}

interface

uses
  Declarations, ClassCompat, SyncObjs, SysUtils, Contnrs;

type
  TDebugFile = class
    Handle: textfile;
    Path, LastMessage: UTF8String;
  end;

procedure AddToLog(ALevel: TLogLevel; Value: UTF8String);
function GetLogFile(APrefix: UTF8String): UTF8String;

{$DEFINE DEBUGMS}

{$IFDEF DEBUGMS}
var
 GLastTime: Cardinal;
{$ENDIF}

var
 GFiles: TObjectList;
 GDebugPath: string = '';
 GDebugPrefix: string = '';
 GMessage: UTF8String = '';
 GCount: Int64;

const
  CMaxLog = 10*1024*1024*1024; //10GB
  CMaxFileSize = 10*1024*1024*1024; //10GB

implementation

uses
 Classes;

var
  GDebugAccess: TCriticalSection;
  GLastMemory: Cardinal;

{**************************************************************************}
function GetLogFile(APrefix: UTF8String): UTF8String;
begin
  try
    result := FormatDateTime('yyyymmdd-',Now);
    {$IFDEF LINUX}
    result := GDebugPath + result + APrefix + '.log';
    {$ELSE}
    result := GDebugPath + result + APrefix + '.log';
    {$ENDIF}
    ForceDirectories(ExtractFilePath(result)); { *Converted from ForceDirectories* }
  except
  end;
end;
{**************************************************************************}
function myFileSize(APath: UTF8String): Int64;
begin
   try
     with TFileStream.Create(APath, fmOpenRead) do
	 try
	   result := Size;
	 finally
	   Free;
	 end;
   except
      result := 0;
   end;
end;
{**************************************************************************}
procedure AddToLogEx(Ip, Prefix, Value: UTF8String);
var
 lName: WideString;
 lMem, lVirtual: Cardinal;
 i: Integer;
 lFile: TDebugFile;
{$IFDEF DEBUGMS}
 lCount: Cardinal;
{$ENDIF}
begin
  try
    lFile := nil;

    {$IFDEF DEBUGMS}
    {$WARNINGS OFF}lCount := GetTickCount - GLastTime;{$WARNINGS ON}
    {$WARNINGS OFF}GLastTime := GetTickCount;{$WARNINGS ON}
    Value := Format('%-15s %s (%d ms)',[Ip, Trim(Value), lCount]);
    {$ELSE}
    Value := Format('%-15s %s',[Ip, Trim(Value)]);
    {$ENDIF}

    try
      {$WARNINGS OFF}lName := GetLogFile(Prefix);{$WARNINGS ON}
      {$HINTS OFF}CurrentMemoryUsageEx(lMem, lVirtual);{$HINTS ON}

      //Writeln(Value);

      lFile := nil;
      for i:=0 to GFiles.Count-1 do
        {$WARNINGS OFF}if TDebugFile(GFiles[i]).Path = lName then{$WARNINGS ON}
        begin
          lFile := TDebugFile(GFiles[i]);
          Break;
        end;
      if lFile = nil then
      begin
        lFile := TDebugFile.Create;
        {$WARNINGS OFF}lFile.Path := lName;{$WARNINGS ON}
        AssignFile(lFile.Handle, lName);
        if FileExists(lName) { *Converted from FileExists* } then
          Append(lFile.Handle)
        else
          Rewrite(lFile.Handle);
        GFiles.Add(lFile);
      end
      else if not FileExists(lName) { *Converted from FileExists* } then
      begin
        CloseFile(lFile.Handle);
        AssignFile(lFile.Handle, lName);
        Rewrite(lFile.Handle);
      end;

      try
        inc(GCount, Length(Value));
        if GCount > CMaxLog then
          Halt;

        if myFileSize(lFile.Path) < CMaxFileSize then
        begin
          if lFile.LastMessage <> Value then
          begin
            lFile.LastMessage := Value;
            WriteLn(lFile.Handle, Format('[%s] t:%05d d:%6d v:%7d %s',
              [FormatDateTime('hh:nn:ss', Now), lMem div 1024,
              lMem - GLastMemory, lVirtual, Trim(Value)]));
            Flush(lFile.Handle);
          end;
        end;
      finally
        GLastMemory := lMem;
      end;
    except
      try
        if lFile <> nil then
        begin
          CloseFile(lFile.Handle);
          AssignFile(lFile.Handle, lFile.Path);
          if FileExists(lFile.Path) { *Converted from FileExists* } then
            Append(lFile.Handle)
          else
            Rewrite(lFile.Handle);
        end;
      except
      end;
    end;
  except
  end;
end;
{**************************************************************************}
procedure AddToLog(ALevel: TLogLevel; Value: UTF8String);
begin
  if ALevel >= GLogLevel then
  begin
    if (ALevel = llError) and (GMessage <> '') then
    begin
      //AddToLogEx('', GDebugPrefix, 'Last message was '+GMessage);
      //GMessage := '';
    end;
    GDebugAccess.Acquire;
    AddToLogEx('', GDebugPrefix, Value);
    GDebugAccess.Release;
  end;
  //else
    //GMessage := Value;
end;
{**************************************************************************}

initialization
  GFiles := TObjectList.Create(true);
  GDebugAccess := TCriticalSection.Create;
finalization
  FreeAndNil(GFiles);
  FreeAndNil(GDebugAccess);
end.
