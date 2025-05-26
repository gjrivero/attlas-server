unit uLib.Logger;

interface

uses
  System.SysUtils, System.Classes, System.IOUtils, System.DateUtils, System.SyncObjs;

type
  TLogLevel = (logNone, logFatal, logCritical, logError, logWarning, logInfo, logDebug, logSpam);

  TLogger = class
  private
    class var FLogFile: TextFile;
    class var FLogFileName: string;
    class var FLogOpened: Boolean;
    class var FCurrentLogLevel: TLogLevel;
    class var FLogToConsole: Boolean;
    class var FLoggingEnabled: Boolean; // Master switch

    class procedure WriteLineToLog(const AFullMessage: string); static;
    class function LogLevelToPrefix(ALevel: TLogLevel): string; static;
    class function ShouldLog(ALevel: TLogLevel): Boolean; static;
  public
    class var FCriticalSection: TCriticalSection;

    class constructor CreateModule; // Renamed from Create for clarity as class constructor
    class destructor DestroyModule;  // Renamed from Destroy for clarity as class destructor

    class procedure Initialize(const AFileName: string;
                               AMinLevel: TLogLevel = logInfo;
                               AEnableConsole: Boolean = False;
                               AEnableFile: Boolean = True); static;
    class procedure FinalizeLogger; static; // Renamed from Finalize

    class procedure SetMinLogLevel(AMinLevel: TLogLevel); static;
    class procedure EnableConsoleOutput(AEnable: Boolean); static;
    class procedure EnableFileOutput(AEnable: Boolean); static; // To enable/disable file logging part
    class procedure SetLoggingEnabled(AEnable: Boolean); static; // Master switch

    class procedure Log(ALevel: TLogLevel; const AMsg: string); overload; static;
    class procedure Log(ALevel: TLogLevel; const AFmt: string; const AArgs: array of const); overload; static;

    // Convenience methods
    class procedure Fatal(const AMsg: string); overload; static;
    class procedure FatalFmt(const AFmt: string; const AArgs: array of const); overload; static;
    class procedure Critical(const AMsg: string); overload; static;
    class procedure CriticalFmt(const AFmt: string; const AArgs: array of const); overload; static;
    class procedure Error(const AMsg: string); overload; static;
    class procedure ErrorFmt(const AFmt: string; const AArgs: array of const); overload; static;
    class procedure Warning(const AMsg: string); overload; static;
    class procedure WarningFmt(const AFmt: string; const AArgs: array of const); overload; static;
    class procedure Info(const AMsg: string); overload; static;
    class procedure InfoFmt(const AFmt: string; const AArgs: array of const); overload; static;
    class procedure Debug(const AMsg: string); overload; static;
    class procedure DebugFmt(const AFmt: string; const AArgs: array of const); overload; static;
    class procedure Spam(const AMsg: string); overload; static;
    class procedure SpamFmt(const AFmt: string; const AArgs: array of const); overload; static;
    class procedure LogException(E: Exception; ALevel: TLogLevel = logError; const AContextMsg: string = ''); static;
  end;

// Global procedures for backward compatibility and easier calling
function GetCurrentLogLevel(): TLogLevel;
procedure InitializeLog(const FileName: string; AMinLevel: TLogLevel = logInfo; ALogToConsole: Boolean = False; AEnableFile: Boolean = True);
procedure FinalizeLog;
procedure SetLogLevel(AMinLevel: TLogLevel);
procedure LogMessage(const Message: string; Level: TLogLevel = logInfo); // Kept for compatibility
procedure LogMessageFmt(Level: TLogLevel; const AFmt: string; const AArgs: array of const); // New helper
procedure LogException(const E: Exception; Level: TLogLevel = logError; const AContextMsg: string = '');

implementation

uses
  System.Rtti,
  System.StrUtils,

  uLib.Utils;

{ TLogger }

class constructor TLogger.CreateModule;
begin
  FCriticalSection := TCriticalSection.Create;
  FLogOpened := False;
  FLogFileName := '';
  FCurrentLogLevel := logInfo; // Default log level
  FLogToConsole := False;
  {$IFDEF DEBUG}
  FLoggingEnabled := True;
  {$ELSE}
  FLoggingEnabled := False; // By default, disable logging in release, can be overridden
  {$ENDIF}
end;

class destructor TLogger.DestroyModule;
begin
  FinalizeLogger; // Ensure log is finalized when module unloads
  FreeAndNil(FCriticalSection);
end;

class procedure TLogger.Initialize(const AFileName: string; AMinLevel: TLogLevel = logInfo; AEnableConsole: Boolean = False; AEnableFile: Boolean = True);
var
  LogDir: string;
  FileExisted: Boolean;
begin
  if not FLoggingEnabled and not AEnableFile and not AEnableConsole then
  begin
    // If master switch is off, and neither file nor console is explicitly enabled now, do nothing.
    Exit;
  end;

  FCriticalSection.Acquire;
  try
    if FLogOpened and AEnableFile then // If already open and want to keep file logging, finalize first
    begin
      try
        WriteLn(FLogFile, FormatDateTime('yyyy-mm-dd"T"hh:nn:ss.zzz"Z"', NowUTC) + ' [INFO] Log re-initializing...');
        CloseFile(FLogFile);
      except
        on E: Exception do WriteLn('Error closing previous log file: ' + E.Message); // Console as last resort
      end;
      FLogOpened := False;
    end;

    FLogFileName := AFileName;
    FCurrentLogLevel := AMinLevel;
    FLogToConsole := AEnableConsole;
    FLoggingEnabled := True; // Enable master switch if Initialize is called

    if AEnableFile and (FLogFileName <> '') then
    begin
      try
        LogDir := TPath.GetDirectoryName(FLogFileName);
        if (LogDir <> '') and (not TDirectory.Exists(LogDir)) then
          ForceDirectories(LogDir);
      except
        on E: Exception do
        begin
          // Cannot create directory, try logging to console if enabled
          if FLogToConsole then
            WriteLn(FormatDateTime('yyyy-mm-dd"T"hh:nn:ss.zzz"Z"', NowUTC) +
              ' [ERROR] [Logger] Error creating log directory "' + LogDir + '": ' + E.Message);
          FLogOpened := False; // Cannot open file
          // Do not exit, console logging might still work
        end;
      end;

      if TFile.Exists(FLogFileName) then
          FileExisted := True
      else
          FileExisted := False;

      AssignFile(FLogFile, FLogFileName);
      {$I-} // Disable I/O checking
      if FileExisted then
          Append(FLogFile) // Append if file exists
      else
          Rewrite(FLogFile); // Create new if file does not exist
      {$I+} // Re-enable I/O checking

      if IOResult = 0 then
      begin
        FLogOpened := True;
        WriteLineToLog(FormatDateTime('yyyy-mm-dd"T"hh:nn:ss.zzz"Z"', NowUTC) + ' [INFO] [Logger] Log (re)initialized. Level: ' + LogLevelToPrefix(FCurrentLogLevel) +
          '. Console: ' + BoolToStr(FLogToConsole, True) + '. File: ' + FLogFileName);
      end
      else
      begin
        FLogOpened := False;
        if FLogToConsole then
          WriteLn(FormatDateTime('yyyy-mm-dd"T"hh:nn:ss.zzz"Z"', NowUTC) +
            ' [ERROR] [Logger] Failed to open/create log file: ' + FLogFileName + '. IOResult: ' + IntToStr(IOResult));
      end;
    end
    else if not AEnableFile then // File logging explicitly disabled
    begin
        FLogOpened := False; // Ensure it's marked as not open
        FLogFileName := '';
        if FLogToConsole then
             WriteLn(FormatDateTime('yyyy-mm-dd"T"hh:nn:ss.zzz"Z"', NowUTC) + ' [INFO] [Logger] File logging disabled. Console logging enabled. Level: ' + LogLevelToPrefix(FCurrentLogLevel));
    end;

  finally
    FCriticalSection.Release;
  end;
end;

class procedure TLogger.FinalizeLogger;
begin
  FCriticalSection.Acquire;
  try
    if FLogOpened then
    begin
      try
        WriteLineToLog(FormatDateTime('yyyy-mm-dd"T"hh:nn:ss.zzz"Z"', NowUTC) + ' [INFO] [Logger] Log finalized.');
        CloseFile(FLogFile);
      except
        on E: Exception do
          if FLogToConsole then
            WriteLn('Error closing log file during FinalizeLogger: ' + E.Message);
      end;
      FLogOpened := False;
    end;
  finally
    FCriticalSection.Release;
  end;
end;

class procedure TLogger.SetMinLogLevel(AMinLevel: TLogLevel);
begin
  FCriticalSection.Acquire;
  try
    FCurrentLogLevel := AMinLevel;
    Log(logInfo, '[Logger] Log level set to: ' + LogLevelToPrefix(AMinLevel));
  finally
    FCriticalSection.Release;
  end;
end;

class procedure TLogger.EnableConsoleOutput(AEnable: Boolean);
begin
  FCriticalSection.Acquire;
  try
    FLogToConsole := AEnable;
    Log(logInfo, '[Logger] Console output ' + IfThen(AEnable, 'enabled', 'disabled') + '.');
  finally
    FCriticalSection.Release;
  end;
end;

class procedure TLogger.EnableFileOutput(AEnable: Boolean);
begin
  FCriticalSection.Acquire;
  try
    if not AEnable and FLogOpened then // Disabling file output
    begin
      Log(logInfo, '[Logger] File output disabled. Closing log file if open.');
      FinalizeLogger; // This will close the file
    end
    else if AEnable and not FLogOpened and (FLogFileName <> '') then // Enabling file output and file was previously set
    begin
       Log(logInfo, '[Logger] File output enabled. Re-initializing log file: ' + FLogFileName);
       Initialize(FLogFileName, FCurrentLogLevel, FLogToConsole, True); // Re-initialize file part
    end
    else if AEnable and (FLogFileName = '') then
       Log(logWarning, '[Logger] Cannot enable file output: Log file name not set. Call Initialize first.');

  finally
    FCriticalSection.Release;
  end;
end;

class procedure TLogger.SetLoggingEnabled(AEnable: Boolean);
begin
  FCriticalSection.Acquire;
  try
    FLoggingEnabled := AEnable;
    // Log this action itself, but only if enabling or if it was already enabled
    if FLoggingEnabled or (not AEnable and FLogOpened) then // Avoid logging "disabled" if it was already disabled and file closed
        Log(logInfo, '[Logger] Logging master switch ' + IfThen(AEnable, 'ENABLED', 'DISABLED') + '.');

    if not FLoggingEnabled and FLogOpened then // If disabling and file is open, close it
    begin
      FinalizeLogger;
    end;
  finally
    FCriticalSection.Release;
  end;
end;

class function TLogger.LogLevelToPrefix(ALevel: TLogLevel): string;
begin
  case ALevel of
    logNone: Result := 'NONE';
    logFatal: Result := 'FATAL';
    logCritical: Result := 'CRITICAL';
    logError: Result := 'ERROR';
    logWarning: Result := 'WARNING';
    logInfo: Result := 'INFO';
    logDebug: Result := 'DEBUG';
    logSpam: Result := 'SPAM';
  else
    Result := 'UNKNOWN';
  end;
end;

class function TLogger.ShouldLog(ALevel: TLogLevel): Boolean;
begin
  Result := FLoggingEnabled and (ALevel <> logNone) and (Ord(ALevel) <= Ord(FCurrentLogLevel));
end;

class procedure TLogger.WriteLineToLog(const AFullMessage: string);
begin
  // Este método asume que FCriticalSection ya está adquirido por el llamador (Log)
  if FLogOpened then
  begin
    try
      WriteLn(FLogFile, AFullMessage);
      Flush(FLogFile); // Asegurar que se escriba inmediatamente al disco
    except
      on E: Exception do
      begin
        // Error escribiendo al archivo. Intentar consola como fallback.
        FLogOpened := False; // Marcar como no abierto para evitar intentos repetidos que fallen.
        if FLogToConsole then
        begin
          WriteLn(FormatDateTime('yyyy-mm-dd"T"hh:nn:ss.zzz"Z"', NowUTC) +
            ' [ERROR] [Logger] FAILED TO WRITE TO LOGFILE. Original message: ' + AFullMessage);
          WriteLn('[ERROR] [Logger] File logging disabled due to error: ' + E.Message);
        end;
      end;
    end;
  end;

  if FLogToConsole then
  begin
    try
      WriteLn(AFullMessage); // Escribir a la consola
    except
      // Ignorar errores de escritura en consola (raro, pero por si acaso)
    end;
  end;
end;

class procedure TLogger.Log(ALevel: TLogLevel; const AMsg: string);
var
  FullMsg: string;
begin
  if not ShouldLog(ALevel) then Exit;

  FCriticalSection.Acquire;
  try
    FullMsg := FormatDateTime('yyyy-mm-dd"T"hh:nn:ss.zzz"Z"', NowUTC) + // ISO8601 con milisegundos y Z para UTC
               ' [' + LogLevelToPrefix(ALevel) + '] ' + AMsg;
    WriteLineToLog(FullMsg);
  finally
    FCriticalSection.Release;
  end;
end;

class procedure TLogger.Log(ALevel: TLogLevel; const AFmt: string; const AArgs: array of const);
begin
  if not ShouldLog(ALevel) then Exit;
  // Format se hace antes del lock para no mantener el lock durante el formateo
  Log(ALevel, Format(AFmt, AArgs));
end;

class procedure TLogger.Fatal(const AMsg: string);
begin
  Log(logFatal, AMsg);
end;

class procedure TLogger.FatalFmt(const AFmt: string; const AArgs: array of const);
begin
  Log(logFatal, AFmt, AArgs);
end;

class procedure TLogger.Critical(const AMsg: string);
begin
  Log(logCritical, AMsg);
end;

class procedure TLogger.CriticalFmt(const AFmt: string; const AArgs: array of const);
begin
  Log(logCritical, AFmt, AArgs);
end;

class procedure TLogger.Error(const AMsg: string);
begin
  Log(logError, AMsg);
end;

class procedure TLogger.ErrorFmt(const AFmt: string; const AArgs: array of const);
begin
  Log(logError, AFmt, AArgs);
end;

class procedure TLogger.Warning(const AMsg: string);
begin
  Log(logWarning, AMsg);
end;

class procedure TLogger.WarningFmt(const AFmt: string; const AArgs: array of const);
begin
  Log(logWarning, AFmt, AArgs);
end;

class procedure TLogger.Info(const AMsg: string);
begin
  Log(logInfo, AMsg);
end;

class procedure TLogger.InfoFmt(const AFmt: string; const AArgs: array of const);
begin
  Log(logInfo, AFmt, AArgs);
end;

class procedure TLogger.Debug(const AMsg: string);
begin
  Log(logDebug, AMsg);
end;

class procedure TLogger.DebugFmt(const AFmt: string; const AArgs: array of const);
begin
  Log(logDebug, AFmt, AArgs);
end;

class procedure TLogger.Spam(const AMsg: string);
begin
  Log(logSpam, AMsg);
end;

class procedure TLogger.SpamFmt(const AFmt: string; const AArgs: array of const);
begin
  Log(logSpam, AFmt, AArgs);
end;

class procedure TLogger.LogException(E: Exception; ALevel: TLogLevel = logError; const AContextMsg: string = '');
var
  Msg: string;
  LStackTrace: string;
begin
  if not ShouldLog(ALevel) then Exit;

  Msg := Format('Exception: %s - Message: %s', [E.ClassName, E.Message]);
  if AContextMsg <> '' then
    Msg := AContextMsg + '. ' + Msg;

  // Obtener StackTrace (Delphi XE7+ o con JCL/madExcept)
  // Esta es una forma básica, puede que necesites una librería para stack traces más detallados en release.
  // ReportMemoryLeaksOnShutdown := True; // Debe estar en el .dpr para que esto funcione bien.
  LStackTrace := '';
  {$IF CompilerVersion >= 28.0} // XE7+
  // LStackTrace := E.StackTrace; // E.StackTrace puede ser vacío si no hay info de debug o no se configuró bien
  // Una forma más robusta de obtener el stack trace podría ser necesaria.
  // Por ahora, solo logueamos el mensaje.
  // Para un stack trace simple (direcciones):
  // LStackTrace := ExceptAddrStackTraceStr(ExceptAddr); // Necesitaría el ExceptAddr
  {$ENDIF}
  // Si se quiere un stack trace más completo, se necesitarían herramientas como JclDebug o madExcept.

  if LStackTrace <> '' then
    Msg := Msg + sLineBreak + 'StackTrace:' + sLineBreak + LStackTrace;

  Log(ALevel, Msg);
end;

// --- Global Procedures (Delegates) ---
procedure InitializeLog(const FileName: string; AMinLevel: TLogLevel = logInfo; ALogToConsole: Boolean = False; AEnableFile: Boolean = True);
begin
  TLogger.Initialize(FileName, AMinLevel, ALogToConsole, AEnableFile);
end;

procedure FinalizeLog;
begin
  TLogger.FinalizeLogger;
end;

procedure SetLogLevel(AMinLevel: TLogLevel);
begin
  TLogger.SetMinLogLevel(AMinLevel);
end;

function GetCurrentLogLevel(): TLogLevel;
begin
  result:=TLogger.FCurrentLogLevel;
end;

procedure LogMessage(const Message: string; Level: TLogLevel = logInfo);
begin
  TLogger.Log(Level, Message); // Llama al TLogger.Log(ALevel, AMsg)
end;

procedure LogMessageFmt(Level: TLogLevel; const AFmt: string; const AArgs: array of const);
begin
  TLogger.Log(Level, AFmt, AArgs);
end;

procedure LogException(const E: Exception; Level: TLogLevel = logError; const AContextMsg: string = '');
begin
  TLogger.LogException(E, Level, AContextMsg);
end;

initialization
  // TLogger.CreateModule se llama automáticamente
finalization
  // TLogger.DestroyModule se llama automáticamente
end.

