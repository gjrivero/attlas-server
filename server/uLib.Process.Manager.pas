unit uLib.Process.Manager;

interface

uses
  System.Classes, System.SysUtils, System.SyncObjs,
  System.Generics.Collections
  {$IFDEF MSWINDOWS}
  ,Winapi.Windows // Correct unit name
  {$ENDIF}
  {$IFDEF LINUX}
  ,Posix.Signal // For Linux signals
  {$ENDIF};

type
  TShutdownHandler = reference to procedure;

  TProcessManager = class
  private
    FShutdownEvent: TEvent;
    FHandlers: TList<TShutdownHandler>;
    FGracefulTimeoutSeconds: Integer; // Renamed for clarity, in seconds
    FIsShuttingDown: Boolean;
    FLock: TCriticalSection; // To protect FHandlers and FIsShuttingDown

    procedure SetupSignalHandlers;
    procedure ExecuteShutdownHandlers;
  public
    constructor Create(AGracefulTimeoutSeconds: Integer = 30);
    destructor Destroy; override;

    procedure RegisterShutdownHandler(AHandler: TShutdownHandler);
    function WaitForShutdownSignal: Boolean;
    procedure RequestProgrammaticShutdown;
    property IsShuttingDown: Boolean read FIsShuttingDown;
  end;

var
  GProcessManager: TProcessManager; // Global instance

implementation

uses
  uLib.Logger; // Assuming uLib.Logger is available for LogMessage

{$IFDEF MSWINDOWS}
// Windows Console Control Handler
function ConsoleCtrlHandler(CtrlType: DWORD): BOOL; stdcall;
begin
  case CtrlType of
    CTRL_C_EVENT, CTRL_BREAK_EVENT, CTRL_CLOSE_EVENT, CTRL_LOGOFF_EVENT, CTRL_SHUTDOWN_EVENT:
    begin
      if Assigned(GProcessManager) then // Access global instance
         GProcessManager.RequestProgrammaticShutdown;
      Result := True; // Signal handled
    end;
  else
    Result := False; // Signal not handled by us
  end;
end;
{$ENDIF}

{$IFDEF LINUX}
// Linux Signal Handler
procedure LinuxSignalHandler(SignalNo: Integer); cdecl;
begin
  if SignalNo in [SIGINT, SIGQUIT, SIGTERM] then
  begin
    if Assigned(ProcessManager) then // Access global instance
      ProcessManager.RequestProgrammaticShutdown;
  end;
end;
{$ENDIF}

constructor TProcessManager.Create(AGracefulTimeoutSeconds: Integer);
begin
  inherited Create;
  FShutdownEvent := TEvent.Create(nil, True, False, ''); // ManualReset=True, InitialState=False
  FHandlers := TList<TShutdownHandler>.Create;
  FGracefulTimeoutSeconds := AGracefulTimeoutSeconds;
  FIsShuttingDown := False;
  FLock := TCriticalSection.Create;
  SetupSignalHandlers; // Setup OS signal handlers
  LogMessage('TProcessManager created.', logInfo);
end;

destructor TProcessManager.Destroy;
begin
  LogMessage('TProcessManager destroying...', logDebug);
  FreeAndNil(FShutdownEvent);
  FreeAndNil(FHandlers);
  FreeAndNil(FLock);
  LogMessage('TProcessManager destroyed.', logInfo);
  inherited;
end;

procedure TProcessManager.SetupSignalHandlers;
begin
{$IFDEF MSWINDOWS}
  SetConsoleCtrlHandler(@ConsoleCtrlHandler, True);
  LogMessage('Windows console control handler installed by TProcessManager.', logDebug);
{$ENDIF}
{$IFDEF LINUX}
  Posix.Signal.Signal(SIGINT, @LinuxSignalHandler);
  Posix.Signal.Signal(SIGQUIT, @LinuxSignalHandler);
  Posix.Signal.Signal(SIGTERM, @LinuxSignalHandler);
  LogMessage('Linux signal handlers (SIGINT, SIGQUIT, SIGTERM) installed by TProcessManager.', logDebug);
{$ENDIF}
end;

procedure TProcessManager.RegisterShutdownHandler(AHandler: TShutdownHandler);
begin
  FLock.Acquire;
  try
    if Assigned(AHandler) then
      FHandlers.Add(AHandler)
    else
      LogMessage('TProcessManager: Attempted to register a nil shutdown handler.', logWarning);
  finally
    FLock.Release;
  end;
end;

procedure TProcessManager.RequestProgrammaticShutdown;
begin
  FLock.Acquire;
  try
    if FIsShuttingDown then
    begin
      LogMessage('TProcessManager: Shutdown already in progress. Request ignored.', logDebug);
      Exit;
    end;
    FIsShuttingDown := True;
    LogMessage('TProcessManager: Programmatic shutdown requested. Signaling event.', logInfo);
  finally
    FLock.Release;
  end;
  FShutdownEvent.SetEvent; // Signal the event to unblock WaitForShutdownSignal
end;

function TProcessManager.WaitForShutdownSignal: Boolean;
var
  WaitResult: TWaitResult;
begin
  LogMessage('TProcessManager: Waiting for shutdown signal...', logInfo);
  // The loop with timeout allows periodic checks if FIsShuttingDown is set by other means
  // than the signal handlers directly setting FShutdownEvent (though they do now).
  // A simple FShutdownEvent.WaitFor(INFINITE) would also work if only signals trigger it.
  while not FIsShuttingDown do
  begin
    // Using FGracefulTimeoutSeconds here for the wait might be long if it's e.g. 30s.
    // A shorter timeout (e.g., 1-2 seconds) is more typical for responsiveness in such loops.
    // However, since RequestProgrammaticShutdown sets the event, this loop should exit quickly
    // once a signal is caught or RequestProgrammaticShutdown is called.
    WaitResult := FShutdownEvent.WaitFor(2000); // Wait for up to 2 seconds, or FGracefulTimeoutSeconds * 1000
    if WaitResult = wrSignaled then
    begin
      // FIsShuttingDown should have been set by RequestProgrammaticShutdown.
      Break;
    end;
    // If wrTimeout, the loop continues and FIsShuttingDown is checked.
  end;

  Result := FIsShuttingDown; // Should be true if we exited the loop due to signal or programmatic request

  if Result then
  begin
    LogMessage('TProcessManager: Shutdown signal received or shutdown initiated. Executing handlers...', logInfo);
    ExecuteShutdownHandlers;
  end
  else
    LogMessage('TProcessManager: WaitForShutdownSignal exited without shutdown request (unexpected).', logWarning);
end;

procedure TProcessManager.ExecuteShutdownHandlers;
var
  HandlerListSnapshot: TList<TShutdownHandler>;
  Handler: TShutdownHandler;
  I: Integer;
begin
  LogMessage(Format('TProcessManager: Executing %d shutdown handlers...', [FHandlers.Count]), logInfo);
  HandlerListSnapshot := TList<TShutdownHandler>.Create;
  try
    FLock.Acquire; // Lock while creating snapshot of handlers
    try
      for Handler in FHandlers do
        HandlerListSnapshot.Add(Handler);
    finally
      FLock.Release;
    end;

    // Execute handlers from the snapshot, in reverse order of registration (LIFO)
    for I := HandlerListSnapshot.Count - 1 downto 0 do
    begin
      Handler := HandlerListSnapshot[I];
      try
        LogMessage(Format('TProcessManager: Executing shutdown handler %d/%d...', [HandlerListSnapshot.Count - I, HandlerListSnapshot.Count]), logDebug);
        Handler();
      except
        on E: Exception do
          LogMessage(Format('TProcessManager: Error executing shutdown handler: %s - %s. Continuing with other handlers.', [E.ClassName, E.Message]), logError);
      end;
    end;
  finally
    HandlerListSnapshot.Free;
  end;
  LogMessage('TProcessManager: All shutdown handlers executed.', logInfo);
end;

initialization
  if not Assigned(GProcessManager) then
    GProcessManager := TProcessManager.Create;
finalization
  if Assigned(GProcessManager) then
    FreeAndNil(GProcessManager);
end.

