unit uLib.Win.Server;

interface

uses
  System.SysUtils, System.Classes, System.SyncObjs, System.JSON,
  System.DateUtils, System.Generics.Collections, System.Threading,
  IdHTTPServer, IdContext, IdSchedulerOfThreadPool, IdScheduler, IdSocketHandle,
  IdSchedulerOfThreadDefault, IdTCPConnection,  IdCustomHTTPServer, IdGlobal,
  Winapi.Windows,

  uLib.Server.Types,
  uLib.Server.Base,
  uLib.Logger;

type
  TWindowsWebServer = class(TServerBase)
  private
    FMonitorThread: TThread;
    FStopMonitorEvent: TEvent;

    procedure MonitorResourcesProc;
    procedure CleanupInactiveConnections; // Simplificado

  protected
    procedure ConfigurePlatformServerInstance; override;
    procedure CleanupServerResources; override;
    procedure PerformShutdownTasks; override;

  public
    constructor Create(AAppConfig: TJSONObject); override;
    destructor Destroy; override;

    procedure Start; override;
    procedure Stop; override;
  end;

implementation

uses
  System.StrUtils,
  System.Rtti;

{ TWindowsWebServer }

constructor TWindowsWebServer.Create(AAppConfig: TJSONObject);
begin
  inherited Create(AAppConfig);
  LogMessage('TWindowsWebServer creating...', logInfo);
  FStopMonitorEvent := TEvent.Create(nil, True, False, '');

  // El hilo de monitoreo se crea pero su utilidad para CleanupInactiveConnections
  // es limitada si se usa TerminateWaitTime de Indy.
  // Podría usarse para otras verificaciones de recursos específicos de Windows.
  if not Assigned(FMonitorThread) then
  begin
    FMonitorThread := TThread.CreateAnonymousThread(MonitorResourcesProc);
    FMonitorThread.Start; // Iniciar el hilo
    LogMessage('TWindowsWebServer: Resource monitoring thread created and started.', logDebug);
  end;
  LogMessage('TWindowsWebServer created.', logInfo);
end;

destructor TWindowsWebServer.Destroy;
begin
  LogMessage('TWindowsWebServer destroying...', logInfo);
  if Assigned(FStopMonitorEvent) then
    FStopMonitorEvent.SetEvent;

  if Assigned(FMonitorThread) then
  begin
    if not FMonitorThread.Finished then
    begin
      LogMessage('TWindowsWebServer: Waiting for monitor thread to terminate...', logDebug);
      FMonitorThread.WaitFor;
    end;
    FreeAndNil(FMonitorThread);
  end;

  FreeAndNil(FStopMonitorEvent);
  LogMessage('TWindowsWebServer destroyed.', logInfo);
  inherited;
end;

procedure TWindowsWebServer.ConfigurePlatformServerInstance;
var
  LBinding: TIdSocketHandle;
begin
  LogMessage('TWindowsWebServer: Configuring Indy server instance (port, bindings, scheduler)...', logInfo);

  HTTPServer.DefaultPort := Self.HTTPConfig.Port;

  HTTPServer.Bindings.Clear;
  LBinding := HTTPServer.Bindings.Add;
  LBinding.IP := '0.0.0.0';
  LBinding.Port := Self.HTTPConfig.Port;
  LogMessage(Format('Indy HTTP Server Binding configured: IP=%s, Port=%d', [LBinding.IP, LBinding.Port]), logDebug);

  if Self.HTTPConfig.ThreadPoolSize > 0 then
  begin
    if Assigned(HTTPServer.Scheduler) and (HTTPServer.Scheduler is TIdSchedulerOfThreadPool) then
    begin
      if TIdSchedulerOfThreadPool(HTTPServer.Scheduler).PoolSize <> Self.HTTPConfig.ThreadPoolSize then
      begin
        TIdSchedulerOfThreadPool(HTTPServer.Scheduler).PoolSize := Self.HTTPConfig.ThreadPoolSize;
        LogMessage(Format('Indy ThreadPool existing scheduler updated. New PoolSize: %d', [Self.HTTPConfig.ThreadPoolSize]), logDebug);
      end;
    end
    else
    begin
      if Assigned(HTTPServer.Scheduler) then FreeAndNil(HTTPServer.Scheduler);
      var NewScheduler := TIdSchedulerOfThreadPool.Create(HTTPServer);
      NewScheduler.PoolSize := Self.HTTPConfig.ThreadPoolSize;
      HTTPServer.Scheduler := NewScheduler;
      LogMessage(Format('Indy ThreadPool scheduler created and assigned. PoolSize: %d', [Self.HTTPConfig.ThreadPoolSize]), logDebug);
    end;
  end
  else
  begin
    if Assigned(HTTPServer.Scheduler) and not (HTTPServer.Scheduler is TIdSchedulerOfThreadDefault) then
    begin
      FreeAndNil(HTTPServer.Scheduler);
      LogMessage('Indy ThreadPool scheduler removed to use default thread-per-connection.', logDebug);
    end;
  end;

  // Asegúrate de que TerminateWaitTime se establezca en TServerBase.ApplyIndyBaseSettings
  // usando FHTTPServerConfig.ConnectionTimeout. Si esa propiedad es para el timeout de inactividad.
  // Ejemplo: HTTPServer.TerminateWaitTime := Self.HTTPConfig.ConnectionTimeout; (si > 0)

  LogMessage(Format('TWindowsWebServer: Indy Server Instance configured using HTTPConfig. Port=%d, PoolSize=%d.',
    [Self.HTTPConfig.Port, Self.HTTPConfig.ThreadPoolSize]), logInfo);
end;

procedure TWindowsWebServer.Start;
begin
  if IsRunning then
  begin
    LogMessage('TWindowsWebServer.Start: Server is already running.', logInfo);
    Exit;
  end;

  LogMessage('TWindowsWebServer: Starting server process...', logInfo);
  ServerState := ssStarting;
  try
    ConfigurePlatformServerInstance;

    LogMessage(Format('TWindowsWebServer: Activating Indy HTTP Server on port %d...', [HTTPServer.DefaultPort]), logInfo);
    HTTPServer.Active := True;
    ServerState := ssRunning;

    if Assigned(FMonitorThread) and FMonitorThread.Suspended then // Should not be suspended if Start logic is correct
       FMonitorThread.Resume
    else if not Assigned(FMonitorThread) or FMonitorThread.Finished then // If it needs to be recreated
    begin
        FStopMonitorEvent.ResetEvent;
        FMonitorThread := TThread.CreateAnonymousThread(MonitorResourcesProc);
        FMonitorThread.Start;
        LogMessage('TWindowsWebServer: Monitor thread (re)started.', logDebug);
    end;

    LogMessage(Format('TWindowsWebServer started successfully. Listening on port %d.',
      [HTTPServer.DefaultPort]), logInfo);
  except
    on E: Exception do
    begin
      ServerState := ssError;
      LogMessage(Format('TWindowsWebServer: Failed to start server: %s - %s', [E.ClassName, E.Message]), logFatal);
      raise;
    end;
  end;
end;

procedure TWindowsWebServer.Stop;
begin
  if (ServerState = ssStopped) or (ServerState = ssStopping) then
  begin
    LogMessage(Format('TWindowsWebServer.Stop: Server is already %s.', [TRttiEnumerationType.GetName<TServerState>(ServerState)]), logInfo);
    Exit;
  end;

  LogMessage('TWindowsWebServer: Stopping server process...', logInfo);

  if Assigned(FStopMonitorEvent) then
    FStopMonitorEvent.SetEvent;

  ServerState := ssStopping;
  try
    PerformShutdownTasks;

    if Assigned(HTTPServer) and HTTPServer.Active then
    begin
      LogMessage('TWindowsWebServer: Deactivating Indy HTTP Server...', logInfo);
      HTTPServer.Active := False;
    end;

    if Assigned(FMonitorThread) and (not FMonitorThread.Finished) then
    begin
      LogMessage('TWindowsWebServer: Waiting for monitor thread to complete after server stop...', logDebug);
      FMonitorThread.WaitFor;
    end;

    ServerState := ssStopped;
    LogMessage('TWindowsWebServer stopped successfully.', logInfo);
  except
    on E: Exception do
    begin
      ServerState := ssError;
      LogMessage(Format('TWindowsWebServer: Error stopping server: %s - %s', [E.ClassName, E.Message]), logError);
    end;
  end;
end;

procedure TWindowsWebServer.CleanupServerResources;
begin
  LogMessage('TWindowsWebServer: Cleaning up platform-specific resources...', logDebug);
  if Assigned(FStopMonitorEvent) then FStopMonitorEvent.SetEvent;
  if Assigned(FMonitorThread) then
  begin
    if not FMonitorThread.Finished then FMonitorThread.WaitFor;
    FreeAndNil(FMonitorThread);
  end;
  inherited;
end;

procedure TWindowsWebServer.PerformShutdownTasks;
begin
  inherited PerformShutdownTasks;
  LogMessage('TWindowsWebServer: Platform-specific pre-stop shutdown tasks completed.', logDebug);
end;

procedure TWindowsWebServer.MonitorResourcesProc;
const
  CHECK_INTERVAL_MS = 30000; // Check every 30 seconds
begin
  LogMessage('TWindowsWebServer: MonitorResourcesProc thread started.', logInfo);
  try
    while True do
    begin
      if FStopMonitorEvent.WaitFor(CHECK_INTERVAL_MS) = TWaitResult.wrSignaled then
      begin
        LogMessage('TWindowsWebServer: MonitorResourcesProc received stop signal.', logInfo);
        Break;
      end;

      if ServerState = ssRunning then
      begin
        try
          // High connection count check (using HTTPConfig from TServerBase)
          if Assigned(HTTPServer) and Assigned(HTTPServer.Contexts) and (Self.HTTPConfig.MaxConnections > 0) then
          begin
            var LContextCount := HTTPServer.Contexts.LockList.Count;
            HTTPServer.Contexts.UnlockList;
            if (LContextCount > (Self.HTTPConfig.MaxConnections * 0.9)) then // Example: 90% threshold
            begin
              LogMessage(Format('TWindowsWebServer Monitor: High connection count detected (%d / %d).',
                [LContextCount, Self.HTTPConfig.MaxConnections]), logWarning);
              // CleanupInactiveConnections could be called here if it did more than just time-based,
              // but MaxConnections is already handled by Indy. This is more for logging or other actions.
            end;
          end;

          // CleanupInactiveConnections; // Call the simplified or removed version
                                      // Given TerminateWaitTime, this custom call might be redundant.
                                      // If kept, ensure its logic is sound.
        except
          on E: Exception do
            LogMessage('TWindowsWebServer Monitor: Error during resource check: ' + E.Message, logError);
        end;
      end
      else if ServerState <> ssStarting then // If not running and not in process of starting
      begin
         LogMessage(Format('TWindowsWebServer: MonitorResourcesProc detected server state is %s (not Running). Exiting loop.',
           [TRttiEnumerationType.GetName<TServerState>(ServerState)]), logInfo);
         Break;
      end;
    end;
  except
    on E: Exception do
      LogMessage(Format('TWindowsWebServer: Unhandled exception in MonitorResourcesProc: %s - %s. Thread terminating.', [E.ClassName, E.Message]), logCritical);
  end;
  LogMessage('TWindowsWebServer: MonitorResourcesProc thread finished.', logInfo);
end;

procedure TWindowsWebServer.CleanupInactiveConnections;
// var
//   Contexts: TIdContextList;
//   ToRemove: TList<TIdContext>;
//   Context: TIdContext;
begin
  // RECOMENDACIÓN: La gestión de timeouts de inactividad de conexión se maneja de forma más
  // efectiva y estándar en Indy a través de la propiedad TIdHTTPServer.TerminateWaitTime.
  // Esta propiedad debe configurarse en TServerBase.ApplyIndyBaseSettings usando
  // el valor de Self.HTTPConfig.ConnectionTimeout (asegurándose que ConnectionTimeout
  // en TServerHTTPConfig realmente represente el timeout de INACTIVIDAD en milisegundos).

  // Si TerminateWaitTime está configurado, esta función personalizada es probablemente redundante
  // para la limpieza basada en tiempo de inactividad.

  // Si aún se desea una lógica aquí (ej. para forzar cierres bajo condiciones específicas
  // no cubiertas por TerminateWaitTime, o para logging detallado de conexiones cerradas),
  // se debe implementar con cuidado.
  // El acceso a "LastActivityTime" no es directo.

  LogMessage('TWindowsWebServer.CleanupInactiveConnections called. Consider using TIdHTTPServer.TerminateWaitTime for idle timeout management.', logDebug);

  // Ejemplo de lógica muy simple si se quisiera mantener algún tipo de barrido,
  // aunque su utilidad es cuestionable si TerminateWaitTime está activo:
  {
  if not Assigned(HTTPServer) or not Assigned(HTTPServer.Contexts) then
    Exit;

  ToRemove := TList<TIdContext>.Create;
  try
    Contexts := HTTPServer.Contexts.LockList;
    try
      for Context in Contexts do
      begin
        // ADVERTENCIA: Esta es una lógica muy simplista y potencialmente problemática.
        // No hay una forma fácil de obtener "LastActivityTime".
        // Solo como ejemplo, si quisiéramos desconectar contextos que ya no están "Connected"
        // según Indy (aunque esto puede ser engañoso para half-open).
        if Assigned(Context.Connection) and (not Context.Connection.Connected) then
        begin
          LogMessage(Format('TWindowsWebServer: Context %s (Peer: %s) found as not connected during cleanup sweep.',
            [Context.SessionID, Context.Binding.PeerIP]), logInfo);
          ToRemove.Add(Context);
        end;
      end;
    finally
      HTTPServer.Contexts.UnlockList;
    end;

    if ToRemove.Count > 0 then
    begin
      LogMessage(Format('TWindowsWebServer: Attempting to disconnect %d contexts marked during cleanup sweep.', [ToRemove.Count]), logInfo);
      for Context in ToRemove do
      begin
        try
          Context.Connection.Disconnect;
        except
          on E: Exception do
            LogMessage(Format('TWindowsWebServer: Error disconnecting context %s during cleanup: %s', [Context.SessionID, E.Message]), logError);
        end;
      end;
    end;
  finally
    ToRemove.Free;
  end;
  }
end;

end.


unit uLib.Win.Server;

interface

uses
  System.SysUtils, System.Classes, System.SyncObjs, System.JSON,
  System.DateUtils, System.Generics.Collections, System.Threading, // Added System.Threading
  IdHTTPServer, IdContext, IdSchedulerOfThreadPool, IdSocketHandle,
  IdCustomHTTPServer, IdGlobal, IdScheduler, IdSchedulerOfThreadDefault,
  IdTCPConnection,
  Winapi.Windows, // Kept for now, might be removable if no Windows-specific API calls remain

  uLib.Server.Types,
  uLib.Server.Base,
  uLib.Logger; // Added uLib.Logger

type
  TWindowsWebServer = class(TServerBase)
  private
    // FScheduler: TIdSchedulerOfThreadPool; // No longer needed here, TServerBase.HTTPServer.Scheduler is used
    FMonitorThread: TThread; // Thread for monitoring resources
    FStopMonitorEvent: TEvent; // Event to signal monitor thread to stop

    // procedure ConfigureThreadPool; // Logic moved to ConfigurePlatformServerInstance
    procedure MonitorResourcesProc; // Procedure for the FMonitorThread
    procedure CleanupInactiveConnections;
    // function ValidateRequest(AContext: TIdContext): Boolean; // This logic can be part of TServerBase or middlewares

  protected
    // procedure ConfigureServer; override; // Logic moved to ConfigurePlatformServerInstance
    procedure ConfigurePlatformServerInstance; override; // Correctly override and implement
    // procedure HandleError(AContext: TIdContext; AResponse: TIdHTTPResponseInfo; AException: Exception); override; // Inherited from TServerBase
    procedure CleanupServerResources; override; // Renamed from CleanupResources for consistency
    procedure PerformShutdownTasks; override; // For tasks before Indy stops

  public
    constructor Create(AAppConfig: TJSONObject); override; // Changed AConfig to AAppConfig for consistency
    destructor Destroy; override;

    procedure Start; override;
    procedure Stop; override;

    // property MaxConcurrentConnections: Integer read FMaxConcurrentConnections write FMaxConcurrentConnections; // Removed, use HTTPConfig.MaxConnections
  end;

implementation

uses
  System.StrUtils, // For IfThen, SameText
  System.Rtti,
  uLib.Base;     // For GetInt, GetStr, GetBool

{ TWindowsWebServer }

constructor TWindowsWebServer.Create(AAppConfig: TJSONObject);
begin
  // 1. Call TServerBase constructor first.
  //    This clones AAppConfig to Self.AppConfig, creates HTTPServer,
  //    loads Self.HTTPConfig, applies base Indy settings, configures SSL,
  //    and initializes framework components (middlewares, controllers).
  inherited Create(AAppConfig); // Pass AAppConfig, not a sub-object

  LogMessage('TWindowsWebServer creating...', logInfo);

  FStopMonitorEvent := TEvent.Create(nil, True, False, ''); // ManualReset=True, InitialState=False

  // FMaxConcurrentConnections is now handled by Self.HTTPConfig.MaxConnections in TServerBase

  // Start monitoring thread if configured or deemed necessary
  // For now, let's assume it's always started for Windows, can be made configurable.
  if not Assigned(FMonitorThread) then // Check if already created (e.g., if Create is called multiple times, though unlikely for server)
  begin
    FMonitorThread := TThread.CreateAnonymousThread(MonitorResourcesProc);
    // (FMonitorThread as TThread).FreeOnTerminate := True; // Set FreeOnTerminate if the thread object itself should be freed
    LogMessage('TWindowsWebServer: Resource monitoring thread created.', logDebug);
  end;
  LogMessage('TWindowsWebServer created.', logInfo);
end;

destructor TWindowsWebServer.Destroy;
begin
  LogMessage('TWindowsWebServer destroying...', logInfo);
  // Stop should have been called by TServerManager or shutdown flow.
  // Ensure monitor thread is signaled to stop and waited for if not FreeOnTerminate.
  if Assigned(FStopMonitorEvent) then
    FStopMonitorEvent.SetEvent; // Signal monitor thread to terminate

  if Assigned(FMonitorThread) then
  begin
    if not FMonitorThread.Finished then // Check if it's still running
    begin
      LogMessage('TWindowsWebServer: Waiting for monitor thread to terminate...', logDebug);
      // FMonitorThread.WaitFor; // Wait for the thread to finish if not FreeOnTerminate
      // If FreeOnTerminate is True, just setting the event should be enough,
      // but explicit WaitFor can be safer if there's cleanup in the thread proc.
      // For this example, assuming the loop in MonitorResourcesProc checks FStopMonitorEvent.Wait(0)
      // and exits, then FreeOnTerminate handles the TThread object.
      // If not using FreeOnTerminate, then WaitFor and then FreeAndNil(FMonitorThread).
    end;
    // If FreeOnTerminate is false, uncomment:
    // FMonitorThread.WaitFor;
    // FreeAndNil(FMonitorThread);
  end;

  FreeAndNil(FStopMonitorEvent);
  // FScheduler is managed by HTTPServer (TIdHTTPServer takes ownership)
  // CleanupServerResources is called from inherited Destroy of TServerBase.
  LogMessage('TWindowsWebServer destroyed.', logInfo);
  inherited;
end;

// This method is now responsible for Indy's port, bindings, and scheduler.
procedure TWindowsWebServer.ConfigurePlatformServerInstance;
var
  LBinding: TIdSocketHandle;
begin
  LogMessage('TWindowsWebServer: Configuring Indy server instance (port, bindings, scheduler)...', logInfo);

  // HTTPServer is the TIdHTTPServer from TServerBase
  // Self.HTTPConfig is TServerHTTPConfig, populated by TServerBase.LoadAndPopulateHTTPConfig

  HTTPServer.DefaultPort := Self.HTTPConfig.Port;

  HTTPServer.Bindings.Clear;
  LBinding := HTTPServer.Bindings.Add;
  LBinding.IP := '0.0.0.0'; // Listen on all interfaces by default for a server
  LBinding.Port := Self.HTTPConfig.Port;
  LogMessage(Format('Indy HTTP Server Binding configured: IP=%s, Port=%d', [LBinding.IP, LBinding.Port]), logDebug);

  // Configure the Scheduler (Thread Pool) of Indy
  // TServerBase.ApplyIndyBaseSettings might have already set some scheduler defaults.
  // This ensures the specific ThreadPoolSize from config is applied.
  if Self.HTTPConfig.ThreadPoolSize > 0 then
  begin
    if Assigned(HTTPServer.Scheduler) and (HTTPServer.Scheduler is TIdSchedulerOfThreadPool) then
    begin
      if TIdSchedulerOfThreadPool(HTTPServer.Scheduler).PoolSize <> Self.HTTPConfig.ThreadPoolSize then
      begin
        TIdSchedulerOfThreadPool(HTTPServer.Scheduler).PoolSize := Self.HTTPConfig.ThreadPoolSize;
        LogMessage(Format('Indy ThreadPool existing scheduler updated. New PoolSize: %d', [Self.HTTPConfig.ThreadPoolSize]), logDebug);
      end;
    end
    else // No scheduler or not a TIdSchedulerOfThreadPool
    begin
      if Assigned(HTTPServer.Scheduler) then // Free existing different scheduler
         FreeAndNil(HTTPServer.Scheduler);

      var NewScheduler := TIdSchedulerOfThreadPool.Create(HTTPServer); // HTTPServer is owner
      NewScheduler.PoolSize := Self.HTTPConfig.ThreadPoolSize;
      HTTPServer.Scheduler := NewScheduler;
      LogMessage(Format('Indy ThreadPool scheduler created and assigned. PoolSize: %d', [Self.HTTPConfig.ThreadPoolSize]), logDebug);
    end;
  end
  else // HTTPConfig.ThreadPoolSize = 0, use default Indy scheduler (TIdSchedulerThreadDefault)
  begin
    if Assigned(HTTPServer.Scheduler) and not (HTTPServer.Scheduler is TIdSchedulerOfThreadDefault) then
    begin
      FreeAndNil(HTTPServer.Scheduler); // Remove custom scheduler to revert to default
      LogMessage('Indy ThreadPool scheduler removed to use default thread-per-connection.', logDebug);
    end;
  end;

  LogMessage(Format('TWindowsWebServer: Indy Server Instance configured using HTTPConfig. Port=%d, PoolSize=%d.',
    [Self.HTTPConfig.Port, Self.HTTPConfig.ThreadPoolSize]), logInfo);
end;

procedure TWindowsWebServer.Start;
begin
  if IsRunning then // IsRunning is from TServerBase
  begin
    LogMessage('TWindowsWebServer.Start: Server is already running.', logInfo);
    Exit;
  end;

  LogMessage('TWindowsWebServer: Starting server process...', logInfo);
  ServerState := ssStarting; // Property from TServerBase
  try
    // 1. (Re)Configure the Indy server instance (port, pool, etc.)
    //    This ensures Indy settings are fresh from HTTPConfig before activation.
    ConfigurePlatformServerInstance;

    // 2. Activate the Indy server
    LogMessage(Format('TWindowsWebServer: Activating Indy HTTP Server on port %d...', [HTTPServer.DefaultPort]), logInfo);
    HTTPServer.Active := True;
    ServerState := ssRunning; // Mark as running AFTER Active is true

    // Start the monitor thread if it's not already running (e.g., if Start is called after a Stop without full destruction)
    if Assigned(FMonitorThread) and FMonitorThread.Suspended then // Or check if Finished if not using Terminate
    begin
       // If thread was created but not started, or if it was stopped and needs restart (complex)
       // For simplicity, let's assume Create starts it, and Stop signals it.
       // If Start can be called multiple times, thread management needs more robustness.
       // For now, assuming FMonitorThread.Start is called in Create or here if needed.
       // If FMonitorThread is managed with FreeOnTerminate=false, it might need to be recreated.
       // If FreeOnTerminate=true, it would be nil here if previously finished.
       // Let's assume FMonitorThread is created in constructor and started there or here.
       if FMonitorThread.Suspended then FMonitorThread.Resume; // Or Start if not started
       // Or, if it can finish and be nil:
       // if not Assigned(FMonitorThread) or FMonitorThread.Finished then
       // begin
       //   FStopMonitorEvent.ResetEvent; // Ensure event is not set from previous run
       //   FMonitorThread := TThread.CreateAnonymousThread(MonitorResourcesProc);
       //   FMonitorThread.Start;
       // end;
    end
    else if Assigned(FMonitorThread) and not FMonitorThread.Suspended then // if it's already running
    begin
        // It should be running if server state was not ssRunning
    end else if not Assigned(FMonitorThread) then // If for some reason it was not created
    begin
        FStopMonitorEvent.ResetEvent;
        FMonitorThread := TThread.CreateAnonymousThread(MonitorResourcesProc);
        // (FMonitorThread as TThread).FreeOnTerminate := True; // Set this if TThread object should self-free
        FMonitorThread.Start;
        LogMessage('TWindowsWebServer: Monitor thread (re)started.', logDebug);
    end;


    LogMessage(Format('TWindowsWebServer started successfully. Listening on port %d.',
      [HTTPServer.DefaultPort]), logInfo);

  except
    on E: Exception do
    begin
      ServerState := ssError;
      LogMessage(Format('TWindowsWebServer: Failed to start server: %s - %s', [E.ClassName, E.Message]), logFatal);
      // PerformShutdownTasks might be relevant here too if partial start occurred
      raise; // Re-raise for TServerManager to handle
    end;
  end;
end;

procedure TWindowsWebServer.Stop;
begin
  if (ServerState = ssStopped) or (ServerState = ssStopping) then
  begin
    LogMessage(Format('TWindowsWebServer.Stop: Server is already %s.', [TRttiEnumerationType.GetName<TServerState>(ServerState)]), logInfo);
    Exit;
  end;

  LogMessage('TWindowsWebServer: Stopping server process...', logInfo);

  // Signal monitor thread to stop BEFORE stopping Indy server
  if Assigned(FStopMonitorEvent) then
    FStopMonitorEvent.SetEvent;

  ServerState := ssStopping;
  try
    PerformShutdownTasks; // Wait for active Indy contexts (from TServerBase, uses HTTPConfig.ShutdownGracePeriodSeconds)

    if Assigned(HTTPServer) and HTTPServer.Active then
    begin
      LogMessage('TWindowsWebServer: Deactivating Indy HTTP Server...', logInfo);
      HTTPServer.Active := False;
    end;

    // Wait for monitor thread to finish if it was running
    if Assigned(FMonitorThread) and (not FMonitorThread.Finished) then
    begin
      LogMessage('TWindowsWebServer: Waiting for monitor thread to complete after server stop...', logDebug);
      // FMonitorThread.WaitFor; // Wait for it to exit its loop
      // If FreeOnTerminate is false, you would FreeAndNil(FMonitorThread) here.
      // If FreeOnTerminate is true, it will free itself.
    end;

    ServerState := ssStopped;
    LogMessage('TWindowsWebServer stopped successfully.', logInfo);
  except
    on E: Exception do
    begin
      ServerState := ssError; // Or some other state indicating stop failure
      LogMessage(Format('TWindowsWebServer: Error stopping server: %s - %s', [E.ClassName, E.Message]), logError);
      // Potentially re-raise or handle further
    end;
  end;
end;

// Handles cleanup of resources specific to TWindowsWebServer before TServerBase.Destroy
procedure TWindowsWebServer.CleanupServerResources;
begin
  LogMessage('TWindowsWebServer: Cleaning up platform-specific resources...', logDebug);
  // Signal and wait for monitor thread if not already handled in Stop or destructor
  if Assigned(FStopMonitorEvent) then FStopMonitorEvent.SetEvent;
  if Assigned(FMonitorThread) then
  begin
    // if not FMonitorThread.FreeOnTerminate then FMonitorThread.WaitFor; // Ensure it finishes
    // FreeAndNil(FMonitorThread); // Only if FreeOnTerminate is false
  end;
  // FStopMonitorEvent is freed in destructor.
  // FScheduler is owned by HTTPServer.
  inherited; // Calls TServerBase.CleanupFrameworkResources (middlewares, etc.)
end;

// This method is called by TServerBase.Stop or by this class's Stop method.
// It handles tasks that must be done *before* TIdHTTPServer is deactivated.
procedure TWindowsWebServer.PerformShutdownTasks;
begin
  // Call inherited to perform TServerBase's shutdown tasks (like waiting for Indy contexts)
  inherited PerformShutdownTasks;

  LogMessage('TWindowsWebServer: Performing platform-specific pre-stop shutdown tasks...', logDebug);
  // Add any Windows-specific tasks here that need to run before Indy stops.
  // For example, unregistering from a Windows service manager if applicable.
  // Currently, none are defined beyond what TServerBase does.
  LogMessage('TWindowsWebServer: Platform-specific pre-stop shutdown tasks completed.', logDebug);
end;

// --- Resource Monitoring and Connection Cleanup ---
// This logic is kept but needs careful consideration if it's truly needed
// given Indy's own timeout mechanisms.
procedure TWindowsWebServer.MonitorResourcesProc;
const
  CHECK_INTERVAL_MS = 15000; // Check every 15 seconds (increased from 5s)
begin
  LogMessage('TWindowsWebServer: MonitorResourcesProc thread started.', logInfo);
  try
    while ServerState = ssRunning do // Loop while server is supposed to be running
    begin
      // Wait for stop signal or timeout
      if FStopMonitorEvent.WaitFor(CHECK_INTERVAL_MS) = TWaitResult.wrSignaled then
      begin
        LogMessage('TWindowsWebServer: MonitorResourcesProc received stop signal.', logInfo);
        Break; // Exit loop if stop event is signaled
      end;

      // If timed out (event not signaled), perform checks
      if ServerState = ssRunning then // Double check state after wait
      begin
        try
          // Check for high connection count (example logic)
          if Assigned(HTTPServer) and Assigned(HTTPServer.Contexts) then
          begin
            var LContextCount := HTTPServer.Contexts.LockList.Count;
            HTTPServer.Contexts.UnlockList; // Unlock immediately after getting count

            // Using HTTPConfig.MaxConnections for the threshold logic
            if (Self.HTTPConfig.MaxConnections > 0) and (LContextCount > (Self.HTTPConfig.MaxConnections * 0.8)) then
            begin
              LogMessage(Format('TWindowsWebServer Monitor: High connection count detected (%d / %d). Consider cleanup.',
                [LContextCount, Self.HTTPConfig.MaxConnections]), logWarning);
              CleanupInactiveConnections;
            end;
          end;
          // Add other resource checks here if needed (memory, CPU - platform-specific)

        except
          on E: Exception do
            LogMessage('TWindowsWebServer Monitor: Error during resource check: ' + E.Message, logError);
        end;
      end
      else // ServerState changed while waiting
      begin
         LogMessage(Format('TWindowsWebServer: MonitorResourcesProc detected server state changed to %s. Exiting loop.',
           [TRttiEnumerationType.GetName<TServerState>(ServerState)]), logInfo);
         Break;
      end;
    end;
  except
    on E: Exception do
      LogMessage(Format('TWindowsWebServer: Unhandled exception in MonitorResourcesProc: %s - %s. Thread terminating.', [E.ClassName, E.Message]), logCritical);
  end;
  LogMessage('TWindowsWebServer: MonitorResourcesProc thread finished.', logInfo);
end;

procedure TWindowsWebServer.CleanupInactiveConnections;
var
  Contexts: TIdContextList;
  LBinding: TIdSocketHandle;
  ToRemove: TList<TIdContext>; // Store contexts to remove to avoid modifying list while iterating
  Context: TIdContext;
  InactiveTimeoutMinutes: Integer; // Example: configurable timeout
  SessionID: String;
  NowTime: TDateTime;
begin
  LogMessage('TWindowsWebServer: Attempting to cleanup inactive connections...', logDebug);
  if not Assigned(HTTPServer) or not Assigned(HTTPServer.Contexts) then
  begin
    LogMessage('TWindowsWebServer.CleanupInactiveConnections: HTTPServer or Contexts not available.', logWarning);
    Exit;
  end;
  // Example: Get timeout from config, default to 30 minutes
  // This should ideally come from Self.HTTPConfig or a specific config for this feature.
  InactiveTimeoutMinutes := GetInt(Self.ApplicationConfig.GetValue<TJSONObject>('server'),
                                       'cleanupInactiveConnectionTimeoutMinutes', 30);
  NowTime := Now; // Get current time once
  ToRemove := TList<TIdContext>.Create;
  try
    Contexts := HTTPServer.Contexts.LockList; // Lock the context list
    try
      for Context in Contexts do
      begin
        // Check if connection is still physically connected and when it was last active.
        // TIdContext.Connection.LastActivityTime is not a standard Indy property.
        // TIdTCPConnection has ConnectTime and TerminateWaitTime.
        // For more precise last activity, you might need a custom IOHandler or context tracking.
        // This example assumes a hypothetical LastActivityTime or uses Connection.Binding.ConnectTime
        // as a proxy if no activity tracking is in place.
        // A simple check: if a connection has been open for a long time without specific activity
        // tracking, it's hard to determine true inactivity without custom logic.
        // Indy's TerminateWaitTime on TIdCustomTCPServer is for idle timeout before server closes it.

        // Using a placeholder for last activity. In a real scenario, this needs proper tracking.
        // For now, let's use a simplified check based on TerminateWaitTime if available,
        // or just log that this feature needs more robust activity tracking.
        // The original code had this part commented out.

        // If using Indy's built-in idle timeout (TerminateWaitTime), this custom cleanup might be redundant.
        // If TerminateWaitTime is set on TIdHTTPServer, Indy handles idle disconnections.
        // This custom logic is only useful if TerminateWaitTime is not used or finer control is needed.

        // Let's assume a simple check: if the connection has been established for longer than InactiveTimeoutMinutes
        // AND there's no other activity metric. This is a very basic heuristic.
        // Note: IdContext.Connection is TIdTCPConnection. It has ConnectTime.
        if Assigned(Context.Connection) then
        begin
          // This logic is illustrative. Proper inactivity detection is complex.
          // Consider if Indy's TerminateWaitTime is sufficient.
          SessionID := Integer(Context.Binding.Handle).ToString;
          LBinding:= Context.Connection.Socket.Binding;

          { TODO : TERMINAR ESTO }
          if MinutesBetween(NowTime, LBinding.ConnectTime)>InactiveTimeoutMinutes then
          begin
            // Further check: is the connection still actually connected?
            // (Context.Connection.Connected might not be reliable if client just disappeared)
            // A small read/write attempt or a PING might be needed for true validation,
            // but that's too complex for a generic cleanup here.
            LogMessage(Format('TWindowsWebServer: Context %s (Peer: %s) connected for > %d mins. Marking for potential cleanup.',
              [ SessionID, Context.Binding.PeerIP, InactiveTimeoutMinutes]), logDebug);
            ToRemove.Add(Context);
          end;
        end;
      end;
    finally
      HTTPServer.Contexts.UnlockList;
    end;

    // Disconnect contexts marked for removal
    if ToRemove.Count > 0 then
    begin
      LogMessage(Format('TWindowsWebServer: Found %d potentially inactive connections to cleanup.', [ToRemove.Count]), logInfo);
      for Context in ToRemove do
      begin
        try
          LogMessage(Format('TWindowsWebServer: Disconnecting context %s (Peer: %s) due to inactivity.',
            [SessionID, Context.Binding.PeerIP]), logInfo);
          Context.Connection.Disconnect; // Request disconnection
        except
          on E: Exception do
            LogMessage(Format('TWindowsWebServer: Error disconnecting context %s: %s', [SessionID, E.Message]), logError);
        end;
      end;
    end
    else
      LogMessage('TWindowsWebServer: No connections marked for inactivity cleanup in this cycle.', logDebug);

  finally
    ToRemove.Free;
  end;
end;

end.
