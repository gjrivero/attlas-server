unit uLib.Console.Signal;

interface

uses
  System.SysUtils, System.SyncObjs; // Moved System.SyncObjs to interface for TEvent visibility if needed by callers, though not strictly necessary for current API

function IsShuttingDown: Boolean;
procedure EnterInShutdownState; // Allows programmatic shutdown initiation
procedure WaitForTerminationSignal; // Blocks until a signal is received or EnterInShutdownState is called

implementation

uses
{$IF Defined(MSWINDOWS)}
  Winapi.Windows; // Correct unit name
{$ENDIF}
{$IF Defined(LINUX)}
  Posix.Signal; // For signal constants and 'signal' function or 'sigaction'
{$ENDIF}

var
  gIsShuttingDown: Boolean = False;
  gEvent: TEvent = nil; // Event for synchronization

{$IF Defined(MSWINDOWS)}
function MSWindowsConsoleCtrlHandler(CtrlType: DWORD): BOOL; stdcall;
begin
  // Handle Ctrl+C, Ctrl+Break, Close Event (e.g., closing console window), Logoff, System Shutdown
  if CtrlType in [CTRL_C_EVENT, CTRL_BREAK_EVENT, CTRL_CLOSE_EVENT, CTRL_LOGOFF_EVENT, CTRL_SHUTDOWN_EVENT] then
  begin
    // Critical section not strictly needed here as SetEvent and boolean write are mostly atomic,
    // but if more complex logic were added, it would be.
    // For this simple case, direct assignment and SetEvent are usually fine.
    EnterInShutdownState; // This will set gIsShuttingDown and call gEvent.SetEvent
    Result := True;  // Indicate that the signal has been handled
    Exit;
  end;
  Result := False; // Not handled by us, pass to the next handler in the chain
end;
{$ENDIF}

{$IF Defined(LINUX)}
procedure LinuxSignalHandler(SignalNo: Integer); cdecl; // cdecl calling convention for C library callbacks
begin
  // Handle SIGINT (Ctrl+C), SIGQUIT (Ctrl+\), and SIGTERM (kill command, systemd stop)
  if SignalNo in [SIGINT, SIGQUIT, SIGTERM] then
  begin
    EnterInShutdownState; // This will set gIsShuttingDown and call gEvent.SetEvent
    // It's generally unsafe to do much more within a signal handler.
    // Setting a flag and signaling an event for the main thread to process is the safest approach.
  end;
end;
{$ENDIF}

procedure InstallTerminationSignalHook;
begin
{$IF Defined(MSWINDOWS)}
  SetConsoleCtrlHandler(@MSWindowsConsoleCtrlHandler, True); // Add our handler to the list
{$ENDIF}
{$IF Defined(LINUX)}
  // Register handlers for common termination signals on Linux
  Posix.Signal.Signal(SIGINT, @LinuxSignalHandler);  // Interrupt from keyboard (Ctrl+C)
  Posix.Signal.Signal(SIGQUIT, @LinuxSignalHandler); // Quit from keyboard (Ctrl+\)
  Posix.Signal.Signal(SIGTERM, @LinuxSignalHandler); // Termination signal (e.g., from 'kill' or systemd)
{$ENDIF}
end;

function IsShuttingDown: Boolean;
begin
  Result := gIsShuttingDown; // Simple boolean read, typically atomic enough
end;

procedure EnterInShutdownState;
begin
  // This can be called from signal handlers or programmatically to initiate shutdown.
  if not gIsShuttingDown then // Prevent redundant actions if already shutting down
  begin
    gIsShuttingDown := True;
    if Assigned(gEvent) then // If WaitForTerminationSignal has created the event
    begin
      gEvent.SetEvent; // Signal the event to wake up the waiting thread
    end;
  end;
end;

procedure WaitForTerminationSignal;
var
  WaitResult: TWaitResult;
begin
  // This procedure is intended to be called once by the main thread.
  if Assigned(gEvent) then
  begin
    // Log this or raise an exception, as it indicates a logical error in usage.
    // For now, just exit to prevent re-entry issues.
    // ShowMessage('WaitForTerminationSignal called more than once or gEvent not properly nilled.');
    Exit;
  end;

  // Create the event. ManualReset=True (stays signaled until ResetEvent), InitialState=False.
  gEvent := TEvent.Create(nil, True, False, '');
  try
    InstallTerminationSignalHook; // Install platform-specific signal handlers

    // Main loop: waits for either gEvent to be signaled (by a signal handler or EnterInShutdownState)
    // or for gIsShuttingDown to become true (if set by EnterInShutdownState before gEvent is signaled).
    // The timeout on WaitFor allows periodic checks of gIsShuttingDown.
    while not gIsShuttingDown do
    begin
      WaitResult := gEvent.WaitFor(2000); // Wait for up to 2 seconds
      if WaitResult = TWaitResult.wrSignaled then
      begin
        // Event was signaled. gIsShuttingDown should have been set by the signaler.
        // The loop condition 'not gIsShuttingDown' will handle exiting.
        Break; // Exit the wait loop; proceed to check gIsShuttingDown
      end;
      // If wrTimeout, the loop continues, and 'not gIsShuttingDown' is checked again.
      // This handles the case where EnterInShutdownState was called, setting gIsShuttingDown,
      // but gEvent.SetEvent might not have been called if gEvent was nil at that moment,
      // or if the signal handler ran before WaitForTerminationSignal fully initialized gEvent.
    end;
    // At this point, gIsShuttingDown is True, or the loop was broken by a signal.
  finally
    FreeAndNil(gEvent); // Clean up the event object
    // Signal handlers are typically uninstalled by the OS when the process exits.
    // Explicitly unhooking can be done but is often not necessary at final termination.
  end;
end;

initialization
  gIsShuttingDown := False;
  gEvent := nil; // Ensure gEvent is nil at program start
finalization
  // gEvent should be freed by WaitForTerminationSignal.
  // If WaitForTerminationSignal was never called, gEvent is still nil.
  // If an exception occurred after gEvent was created but before its finally block,
  // it could leak. However, WaitForTerminationSignal is typically a top-level blocking call.
end.

