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

unit ClassCompat;

{$IFDEF FPC}
  {$MODE Delphi}
{$ENDIF}

interface

uses
  Classes, 
   {$IFDEF FPC}unix, dl, baseunix, unixtype, Process
   {$ELSE}
		 {$IFDEF LINUX}Libc
		 {$ELSE}Windows, ShellApi, PsAPI
	     {$ENDIF}
   {$ENDIF}, SysUtils;

{$IFDEF LINUX}
const
  CONST_SEGID             =        10016;
{$ENDIF}

{$IFDEF FPC}
const
  clib = 'c';

type
  PIOFile = Pointer;
{$ENDIF}

function GetProcessId: Integer;
function GetThreadId: Integer;
function GetParentProcessId: Integer;
function GetTempPath: WideString;
function CopyStringCount(Src: PChar;Count: Integer): WideString;
function CopyStringCountAnsi(Src: PChar;Count: Integer): string;
function GetTickCount: Cardinal;
function GetModulePath: WideString;
function LoadLibrary(const Name: WideString): Pointer;
function LoadLibError: WideString;
function GetProcAddress(hModule: Pointer; AName: PChar): pointer;

procedure CopyFile(ASource, ADest: UTF8String);
procedure MoveFile(ASource, ADest: UTF8String);
procedure DeleteFile(AFileName: UTF8String);
function FileDate(AFileName: UTF8String): TDateTime;
function ShellExec(AFileName: UTF8String): Boolean;
function ShellExecAndWait(AFileName: UTF8String): Boolean;
function ExecuteAndWait(AFileName: UTF8String): Boolean;
//function ShellExecProcess(AFileName: UTF8String): Boolean;
function ProcessRunning(AId: Integer): Boolean;
function GetPath(AId: Integer): UTF8String;
procedure KillProcess(AId: Integer);
procedure CleanZombies;

procedure UnloadLibrary(Module: Pointer);
procedure BeginCritical(var Value: TRTLCriticalSection);
procedure EndCritical(var Value: TRTLCriticalSection);
procedure InitCritical(var Value: TRTLCriticalSection);

function WideStringToPChar(const AValue: WideString): PChar;
function CreatePidFile(AName: UTF8String): Boolean;
function CurrentMemoryUsage: Cardinal;
function GetFilesDirectory(const APath: UTF8String): TStringList;
procedure CurrentMemoryUsageEx(var AReal, AVirtual: Cardinal);
function PosChar(AValue: char; AString: UTF8String): Integer;
{$IFDEF FPC}
function popen(__command:PChar; __type:Pchar):PIOFile;cdecl;external clib name 'popen';
function fwrite(__ptr:pointer; __size:size_t; __n:size_t; __s:PIOFile):size_t;cdecl;external clib name 'fwrite';
function pclose(__stream:PIOFile):longint;cdecl;external clib name 'pclose';
{$ENDIF}

implementation

uses
  Declarations, ClassDebug, DateUtils;
  
{$IFDEF FPC}  
const
  PkgLoadingMode = RTLD_LAZY ;
{$ENDIF}

{**************************************************************************}
function GetProcessId: Integer;
begin
  {$IFDEF LINUX}
  result := {$IFDEF FPC}fpgetpid{$ELSE}getpid{$ENDIF};
  {$ELSE}
  result := GetCurrentThreadId;
  {$ENDIF}
end;
{**************************************************************************}
function GetThreadId: Integer;
begin
  {$IFDEF LINUX}
  result := GetCurrentThreadID;
  {$ELSE}
  result := GetCurrentThreadId;
  {$ENDIF}
end;
{**************************************************************************}
function GetParentProcessId: Integer;
begin
  {$IFDEF LINUX}
  result := {$IFDEF FPC}fpgetppid{$ELSE}getppid{$ENDIF};
  {$ELSE}
  result := GetCurrentProcessId;
  {$ENDIF}
end;
{**************************************************************************}
function GetTempPath: WideString;
{$IFNDEF LINUX}
var
 buf: array[0..250] of char;
{$ENDIF}
begin
  {$IFDEF LINUX}
  result := '/tmp/';
  {$ELSE}
  Windows.GetTempPath(250, buf);
  Result := IncludeTrailingPathDelimiter(buf);
  {$ENDIF}
end;
{************************************************************************}
function CopyStringCount(Src: PChar;Count: Integer): WideString;
var
 i: Integer;
 lSt: UTF8String;
begin
  lSt := '';
  result := '';
  if Count = 0 then
    Exit;
  SetLength(lSt, Count);
  for i:=0 to Count-1 do
    lSt[i+1] := src[i];
  result := UTF8Decode(lSt);
end;
{**************************************************************************}
function CopyStringCountAnsi(Src: PChar;Count: Integer): string;
var
 i: Integer;
 lSt: UTF8String;
begin
  lSt := '';
  result := '';
  if Count = 0 then
    Exit;
  SetLength(lSt, Count);
  for i:=0 to Count-1 do
    lSt[i+1] := src[i];
  result := lSt;
end;
{**************************************************************************}
function GetTickCount: Cardinal;
{$IFDEF LINUX}
var
 val: TTimeVal;
 lResult: Int64;
{$ENDIF}
begin
{$IFDEF LINUX}
  try
    {$IFDEF FPC}fpgettimeofday(@val,nil);{$ELSE}gettimeofday(val,nil);{$ENDIF};
    lResult := (val.tv_sec*1000+val.tv_usec div 1000);
    result := lResult mod 1000000000;
  except
    result := 0;
  end;
{$ELSE}
  result := Windows.GetTickCount;
{$ENDIF}
end;
{**************************************************************************}
function GetModulePath: WideString;
    {$IFNDEF FPC}
var
  ModName: array[0..MAX_PATH] of Char;
  {$ENDIF}
begin
  result := '';
  {$IFDEF LINUX}
    {$IFDEF FPC}
	{$ELSE}
      SetString(Result, ModName, GetModuleFileName(HInstance,
       ModName, SizeOf(ModName))); 
      result := ExtractFilePath(result);
	 {$ENDIF}
  {$ELSE}
  SetString(Result, ModName, Windows.GetModuleFileName(HInstance,
    ModName, SizeOf(ModName)));
  result := ExtractFilePath(result);
  {$ENDIF}
end;
{**************************************************************************}
function LoadLibrary(const Name: WideString): Pointer;
begin
{$IFDEF MSWINDOWS}
  Result := Pointer(SafeLoadLibrary(Name));
{$ENDIF}
{$IFDEF LINUX}
  Result := dlOpen(PChar(UTF8Encode(Name)), PkgLoadingMode);
  //if result = nil then
  //  AddToLog(llError, dlError()); 
{$ENDIF}
end;
{**************************************************************************}
function LoadLibError: WideString;
{$IFDEF MSWINDOWS}
var
  lError: Integer;
{$ENDIF}
begin
{$IFDEF MSWINDOWS}
  lError := GetLastError;
  if GetLastError <> 0 then
    result := SysErrorMessage(lError)
  else
    result := '';
{$ENDIF}
{$IFDEF LINUX}
  Result := dlError;
{$ENDIF}
end;
{**************************************************************************}
procedure UnloadLibrary(Module: Pointer);
begin
{$IFDEF MSWINDOWS}
  FreeLibrary(Cardinal(Module));
{$ENDIF}
{$IFDEF LINUX}
  dlclose(Module);
  {$IFNDEF FPC}InvalidateModuleCache; {$ENDIF}
{$ENDIF}
end;
{**************************************************************************}
procedure BeginCritical(var Value: TRTLCriticalSection);
begin
  {$IFDEF DEBUG}AddToLog('', 'Begin Critical '+IntToStr(Integer(@Value)));{$ENDIF}
  EnterCriticalSection(Value);
end;
{**************************************************************************}
procedure EndCritical(var Value: TRTLCriticalSection);
begin
  {$IFDEF DEBUG}AddToLog('', 'End Critical '+IntToStr(Integer(@Value)));{$ENDIF}
  LeaveCriticalSection(Value);
end;
{**************************************************************************}
procedure InitCritical(var Value: TRTLCriticalSection);
begin
  {$IFDEF DEBUG}AddToLog('', 'Init Critical '+IntToStr(Integer(@Value)));{$ENDIF}
  {$IFDEF FPC}
  InitCriticalSection(Value); 
  {$ELSE}
  InitializeCriticalSection(Value); 
  {$ENDIF}
end;
{**************************************************************************}
function GetProcAddress(hModule: Pointer; AName: PChar): pointer;
begin
{$IFDEF MSWINDOWS}
  result := Windows.GetProcAddress(Cardinal(hModule),AName);
{$ENDIF}
{$IFDEF LINUX}
{$IFDEF FPC}Result := dl.dlsym(Pointer(hModule),AName);{$ELSE}Result := Libc.dlsym(Pointer(hModule),AName);{$ENDIF};  
{$ENDIF}
end;
{**************************************************************************}
function WideStringToPChar(const AValue: WideString): PChar;
var
 lTemp: AnsiString; 
begin
  {$WARNINGS OFF}
  lTemp := AValue;
  result := PChar(lTemp);
  {$WARNINGS ON}
end;
{**************************************************************************}
procedure CopyFile(ASource, ADest: UTF8String);
var
  lStream: TFileStream;
begin
  with TFileStream.Create(ADest, fmCreate ) do
  try
    lStream := TFileStream.Create(ASource, fmOpenRead and fmShareDenyWrite);
    try
      lStream.Seek(0, soFromBeginning);
      CopyFrom(lStream, 0);
    finally
      FreeAndNil(lStream);
    end;
  finally
    Free;
  end;
end;
{**************************************************************************}
procedure MoveFile(ASource, ADest: UTF8String);
begin
  if FileExists(ADest) { *Converted from FileExists* } then
    DeleteFile(ADest);
  SysUtils.RenameFile(ASource,ADest); { *Converted from RenameFile* }
end;
{**************************************************************************}
procedure DeleteFile(AFileName: UTF8String);
var
 lFile: PChar;
begin
  lFile := PChar(UTF8Encode(AFileName));
  try
    {$IFDEF MSWINDOWS}Windows.{$ELSE}SysUtils.{$ENDIF}DeleteFile(lFile);
  except
  end;
end;
{**************************************************************************}
function FileDate(AFileName: UTF8String): TDateTime;
begin
  result := FileDateToDateTime(FileAge(AFileName) { *Converted from FileAge* });
end;
{**************************************************************************}
function CreatePidFile(AName: UTF8String): Boolean;
var
 lPid: string;
begin
  result := true;
  
  {$IFNDEF LINUX}
  Exit;
  {$ENDIF}

  try
    ForceDirectories('/var/run/kelare/');
  except
  end;

  try
    if FileExists('/var/run/kelare/'+AName+'.pid') { *Converted from FileExists* } then
    try
      with TFileStream.Create('/var/run/kelare/'+AName+'.pid', fmOpenRead) do
      try
        lPid := '';
        SetLength(lPid, Size);
        Read(lPid[1], Size);
        lPid := Trim(lPid);
        if (lPid <> '') then
          if DirectoryExists('/proc/'+Trim(lPid)) { *Converted from DirectoryExists* } then
          begin
            result := false;
            Exit;
          end;
      finally
        Free;
      end;

      DeleteFile('/var/run/kelare/'+AName+'.pid');
    except
    end;


    with TFileStream.Create('/var/run/kelare/'+AName+'.pid', fmCreate) do
    try
      lPid := IntToStr(GetProcessId) + sLineBreak;
      Write(lPid[1], Length(lPid));
    finally
      Free;
    end;
  except
  end;
end;
{**************************************************************************}
{$IFDEF MSWINDOWS}
function CurrentMemoryUsage: Cardinal;
var
 pmc: TProcessMemoryCounters;
begin
  result := 0;
  pmc.cb := SizeOf(pmc) ;
  if GetProcessMemoryInfo(GetCurrentProcess, @pmc, SizeOf(pmc)) then
    Result := pmc.WorkingSetSize
  else
    RaiseLastOSError;
end;
{$ELSE}
function CurrentMemoryUsage: Cardinal;
{$IFDEF FPC}
var
 i: Integer;
 lValue: string;
{$ELSE}
var
 lUsage: TRUsage;
{$ENDIF}
begin
  result := 0;
  {$IFDEF FPC}
  result := 0;
  Exit;

  //result := GetHeapStatus.TotalAllocated div 1024;
  try
    with TStringList.Create do
    try
      if FileExists('/proc/'+IntToStr(fpgetpid)+'/status') { *Converted from FileExists* } then
        LoadFromFile('/proc/'+IntToStr(fpgetpid)+'/status');
      for i:=0 to Count-1 do
        if Pos('VmSize:', Strings[i]) > 0 then
        begin
          lValue := Trim(Copy(Strings[i], 8, MAXINT));
          lValue := Trim(Copy(lValue, 1, Length(lValue) -3));
          result := StrToIntDef(lValue, 0);
          Exit;
        end;
    finally
      Free;
    end;
  except
    on E:Exception do
      WriteLn(E.Message);
  end;

  {$ELSE}
  if getrusage(RUSAGE_SELF, lUsage) = 0 then
    result := lUsage.ru_maxrss; 
 {$ENDIF}
end;
{$ENDIF}
{**************************************************************************}
{$IFDEF MSWINDOWS}
procedure CurrentMemoryUsageEx(var AReal, AVirtual: Cardinal);
var
 pmc: TProcessMemoryCounters;
begin
  AReal := 0;
  AVirtual := 0;
  pmc.cb := SizeOf(pmc) ;
  if GetProcessMemoryInfo(GetCurrentProcess, @pmc, SizeOf(pmc)) then
  begin
    AReal := pmc.WorkingSetSize;
    AVirtual := pmc.PagefileUsage; //Not virtual memory, but at least not empty ;-)
  end
  else
    RaiseLastOSError;
end;
{$ELSE}
{**************************************************************************}
function GetFilesDirectory(const APath: UTF8String): TStringList;
var
{$IFDEF MSWINDOWS}
 lRec: TSearchRec;
 lRes: Integer;
{$ELSE}
{$IFDEF FPC}
 lRec: PDIR;
 lValue: PDirent;
{$ELSE}
 lRec: PDirectoryStream;
 lValue: PDirent64;
{$ENDIF}
{$ENDIF}
begin
  result := TStringList.Create;
  {$IFDEF MSWINDOWS}
  lRes := FindFirst(APath + '*.*', faAnyFile, lRec);
  if lRes = 0 then
  begin
    while lRes = 0 do
    begin
      if (lRec.Name <> '.') and (lRec.Name <> '..') then
        result.Add(lRec.Name);
      lRes := FindNext(lRec);
    end;
    FindClose(lRec);
  end;
  {$ELSE}
    {$IFDEF FPC}
    lRec := fpopendir(PChar(APath));
    if lRec <> nil then
    begin
      lValue := fpreaddir(lRec^);
      while lValue <> nil do
      begin
        if (Trim(lValue.d_name) <> '.') and (Trim(lValue.d_name) <> '..') then
          result.Add(lValue.d_name);
        lValue := fpreaddir(lRec^);
      end;
      fpclosedir(lRec^);
    end;
    {$ELSE}
    lRec := opendir(PChar(APath));
    if lRec <> nil then
    begin
      lValue := readdir64(lRec);
      while lValue <> nil do
      begin
        if (Trim(lValue.d_name) <> '.') and (Trim(lValue.d_name) <> '..') then
          result.Add(lValue.d_name);
        lValue := readdir64(lRec);
      end;
    end;
    closedir(lRec);
    {$ENDIF}
  {$ENDIF}
end;
{**************************************************************************}
procedure CurrentMemoryUsageEx(var AReal, AVirtual: Cardinal);
{$IFDEF FPC}
var
 i: Integer;
 lValue: string;
{$ELSE}
var
 lUsage: TRUsage;
{$ENDIF}
begin
  AReal := 0;
  AVirtual := 0;
  {$IFDEF FPC}

  //result := GetHeapStatus.TotalAllocated div 1024;
  try
    with TStringList.Create do
    try
      if FileExists('/proc/'+IntToStr(fpgetpid)+'/status') { *Converted from FileExists* } then
        LoadFromFile('/proc/'+IntToStr(fpgetpid)+'/status');
      for i:=0 to Count-1 do
        if Pos('VmSize:', Strings[i]) > 0 then // Mod 20230316 was VmHWM
        begin
          lValue := Trim(Copy(Strings[i], 8, MAXINT));
          lValue := Trim(Copy(lValue, 1, Length(lValue) -3));
          AReal := StrToIntDef(lValue, 0);
          Exit;
        end else if Pos('VmSize:', Strings[i]) > 0 then
        begin
          lValue := Trim(Copy(Strings[i], 8, MAXINT));
          lValue := Trim(Copy(lValue, 1, Length(lValue) -3));
          AVirtual := StrToIntDef(lValue, 0);
        end;
    finally
      Free;
    end;
  except
    on E:Exception do
      WriteLn(E.Message);
  end;

  {$ELSE}
  if getrusage(RUSAGE_SELF, lUsage) = 0 then
    result := lUsage.ru_maxrss; 
 {$ENDIF}
end;
{$ENDIF}
{**************************************************************************}
function PosChar(AValue: char; AString: UTF8String): Integer;
var
 i: Integer;
begin
  result := 0;
  for i:=1 to Length(AString) do
    if AString[i] = AValue then
    begin
      result := i;
      Exit;
    end;
end;
{**************************************************************************}
function ShellExecAndWait(AFileName: UTF8String): Boolean;
{$IFDEF LINUX}
var
 lRes: Integer;
{$ENDIF}
begin
  lRes := 0;
  AddToLog(llDebug, 'Execute: '+AFileName);
  {$IFDEF LINUX}
{$IFDEF FPC}fpchmod(PChar(AFileName), $777);{$ELSE}Libc.chmod(PChar(AFileName), $777);{$ENDIF};
{$IFDEF FPC}
{$WARNINGS OFF}
  with TProcess.Create(nil) do
  try
    //Executable := AFilename;
    CommandLine := AFilename;
    Options := [poNoConsole, poStderrToOutPut, poWaitOnExit];
    Execute;
  finally
    Free;
  end;
{$WARNINGS ON}

{$ELSE}lRes := Libc.system(PChar(AFileName));{$ENDIF};
  AddToLog(llDebug, 'Execute returned '+IntToStr(lRes));
  result := lRes = 0;
  {$ELSE}
  result := ShellExecute(0, 'open', PChar(AFileName), '', '', SW_HIDE) > 32;
  {$ENDIF}
end;
{**************************************************************************}
(*function ShellExec(AFileName: UTF8String): Boolean;
{$IFDEF LINUX}
var
 lRes: Integer;
{$ENDIF}
begin
  AddToLog(llDebug, 'Execute: '+AFileName);
  {$IFDEF LINUX}
{$IFDEF FPC}fpchmod(PChar(AFileName), $777);{$ELSE}Libc.chmod(PChar(AFileName), $777);{$ENDIF};
{$IFDEF FPC}lRes := fpsystem(PChar(AFileName));{$ELSE}lRes := Libc.system(PChar(AFileName));{$ENDIF};
  AddToLog(llDebug, 'Execute returned '+IntToStr(lRes));
  result := lRes = 0;
  {$ELSE}
  result := ShellExecute(0, 'open', PChar(AFileName), '', '', SW_HIDE) > 32;
  {$ENDIF}
end;*)
{**************************************************************************}
function ExecuteAndWait(AFileName: UTF8String): Boolean;
{$IFDEF LINUX}
var
 lRes: Integer;
{$IFDEF FPC}
 lWait: Integer;
{$ENDIF}
{$ENDIF}
begin
  {$IFDEF LINUX}
  {$IFDEF FPC}
  lWait := 0;
  fpchmod(PChar(AFileName), $777);
  lRes := fpsystem(PChar(AFileName));
  fpwait(lWait);
  {$ELSE}
  Libc.chmod(PChar(AFileName), $777);
  lRes := Libc.system(PChar(AFileName));
  wait(nil);
  {$ENDIF}
  result := lRes = 0;
  {$ELSE}
  result := ShellExecute(0, 'open', PChar(AFileName), '', '', SW_HIDE) > 32;
  {$ENDIF}
end;
{**************************************************************************}
function ShellExec(AFileName: UTF8String): Boolean;
{$IFDEF LINUX}
var
 lRes: Integer;
{$ENDIF}
begin
  lRes := 0;
  AddToLog(llDebug, 'Execute: '+AFileName);
  {$IFDEF LINUX}
{$IFDEF FPC}fpchmod(PChar(AFileName), $777);{$ELSE}Libc.chmod(PChar(AFileName), $777);{$ENDIF};
{$IFDEF FPC}
{$WARNINGS OFF}
  with TProcess.Create(nil) do
  try
    //Executable := AFilename;
    CommandLine := AFilename;
    Options := [poNoConsole, poStderrToOutPut];
    Execute;
  finally
    Free;
  end;
{$WARNINGS ON}

{$ELSE}lRes := Libc.system(PChar(AFileName));{$ENDIF};
  AddToLog(llDebug, 'Execute returned '+IntToStr(lRes));
  result := lRes = 0;
  {$ELSE}
  result := ShellExecute(0, 'open', PChar(AFileName), '', '', SW_HIDE) > 32;
  {$ENDIF}
end;
{**************************************************************************}
function ProcessRunning(AId: Integer): Boolean;
begin
  AddToLog(llDebug, 'Check Process '+IntToStr(AId));
  {$IFDEF LINUX}
  {$IFDEF FPC}       
  result := fpkill(AId, 0) = 0;
  {$ELSE}
  result := kill(AId, 0) = 0;
  {$ENDIF}
  if result then
  try
    with TStringList.Create do
    try
      LoadFromFile('/proc/'+IntToStr(AId)+'/status');
      while Count > 0 do
      begin
        if Pos('State: ', Strings[0]) > 0 then
        begin
          if (Pos('(dead)', Strings[0]) > 0) or
            (Pos('(stopped)', Strings[0]) > 0) then
          begin
            AddToLog(llWarning, 'ProcessRunning => Dead/Stopped Process '+Text);
            result := false;
            {$IFDEF FPC}
            fpkill(AId, 9); //Die bitch, die, don't sleap!
            {$ELSE}
            kill(AId, 9); //Die bitch, die, don't sleap!
            {$ENDIF}
          end;
        end;
        Delete(0);
      end;
    finally
      Free;
    end;
  except
  end
  else
    AddToLog(llWarning, 'ProcessRunning => Process disappeared: '+IntToStr(AId));
  {$ELSE}
  result := false;
  {$ENDIF}
end;
{**************************************************************************}
function GetPath(AId: Integer): UTF8string;
{$IFDEF LINUX}
var
 lPath: string;
{$ENDIF}
begin
  {$IFDEF LINUX}
  lPath := '';
  SetLength(lPath, 255);
  {$IFDEF FPC}
  lPath := fpreadlink('/proc/'+IntToStr(AId)+'/exe');
  {$ELSE}
  realpath(PChar('/proc/'+IntToStr(AId)+'/exe'), PChar(lPath));
  SetLength(lPath, Pos(#0, lPath)-1);
  {$ENDIF}
  result := lPath;
  {$ELSE}
  result := '';
  {$ENDIF}

  AddToLog(llDebug, 'Get path '+IntToStr(AId)+' = '+result);
end;
{**************************************************************************}
procedure KillProcess(AId: Integer);
{$IFDEF FPC}
var
 lDate: TDateTime;
 lPid: Integer;
{$ENDIF}
begin
  {$IFDEF LINUX}
  {$IFDEF FPC}
  fpkill(AId, 9); //Die bitch, die, don't sleap!
  lDate := Now;
  lPid := 0;
  while (lPid = 0) and (SecondsBetween(Now, lDate) < 5) do
    lPid := fpwaitpid(AId, nil, WNOHANG); //Collect zombie
  {$ELSE}
  kill(AId, 9); //Die bitch, die, don't sleap!
  {$ENDIF}
  {$ENDIF}
end;
{**************************************************************************}
procedure CleanZombies;
var
 lPid: Integer;
begin
  repeat
    lPid := fpwaitpid(-1, nil, WNOHANG);
  until lPid <= 0;
end;
{**************************************************************************}

end.
