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
  protected
    procedure ConfigurePlatformServerInstance; override;
    procedure CleanupServerResources; override;
    procedure PerformShutdownTasks; override;

  public
    procedure ReloadConfiguration(ANewAppConfig: TJSONObject);
    constructor Create(AAppConfig: TJSONObject); override;
    destructor Destroy; override;

    procedure Start; override;
    procedure Stop; override;
  end;

implementation

uses
  System.StrUtils,
  System.Math,
  System.Rtti;

{ TWindowsWebServer }

constructor TWindowsWebServer.Create(AAppConfig: TJSONObject);
begin
  inherited Create(AAppConfig);
  LogMessage('TWindowsWebServer creating...', logInfo);
  FStopMonitorEvent := TEvent.Create(nil, True, False, '');
  FMonitorThread:=Nil;
  if not Assigned(FMonitorThread) then
  begin
    FMonitorThread := TThread.CreateAnonymousThread(MonitorResourcesProc);
    FMonitorThread.FreeOnTerminate := False; // Asegurar control manual de la liberación
    FMonitorThread.Start;
    LogMessage('TWindowsWebServer: Resource monitoring thread created and started.', logDebug);
  end;
  LogMessage('TWindowsWebServer created.', logInfo);
end;

destructor TWindowsWebServer.Destroy;
begin
  LogMessage('TWindowsWebServer destroying...', logInfo);

  // 1. Señalar al thread de monitoreo que termine
  if Assigned(FStopMonitorEvent) then
    FStopMonitorEvent.SetEvent;

  // 2. Esperar a que el thread de monitoreo termine y luego liberarlo
  if Assigned(FMonitorThread) then
  begin
    if not FMonitorThread.Finished then // Verificar si realmente necesita esperar
    begin
      LogMessage('TWindowsWebServer: Waiting for monitor thread to terminate...', logDebug);
      FMonitorThread.WaitFor; // Esperar a que MonitorResourcesProc complete su ejecución
    end;
    LogMessage('TWindowsWebServer: Monitor thread finished or was already finished. Freeing FMonitorThread.', logDebug);
    FreeAndNil(FMonitorThread); // Liberar el objeto TThread
  end;

  // 3. Liberar otros recursos propios de TWindowsWebServer
  if Assigned(FStopMonitorEvent) then // Volver a verificar por si se liberó en otro lado (poco probable aquí)
    FreeAndNil(FStopMonitorEvent);

  LogMessage('TWindowsWebServer specific resources freed.', logInfo);

  // 4. Llamar al destructor de la clase base COMO ÚLTIMO PASO
  inherited Destroy;

  LogMessage('TWindowsWebServer fully destroyed.', logInfo);
end;

procedure TWindowsWebServer.ReloadConfiguration(ANewAppConfig: TJSONObject);
var
  WasRunning: Boolean;
  SuccessfullyReconfiguredInternals: Boolean;
begin
  LogMessage('TWindowsWebServer: Reloading configuration using provided ANewAppConfig...', logInfo);
  SuccessfullyReconfiguredInternals := False;
  WasRunning := IsRunning; // IsRunning es de TServerBase

  if WasRunning then
  begin
    LogMessage('TWindowsWebServer.ReloadConfiguration: Stopping server to apply configuration changes.', logInfo);
    Self.Stop; // Llama al Stop de TWindowsWebServer
  end;

  try
    // PASO 1: Actualizar FAppConfig en TServerBase con la nueva configuración.
    Self.InternalRefreshAppConfig(ANewAppConfig); // Método protegido de TServerBase

    // PASO 2: Repoblar FHTTPServerConfig desde el FAppConfig recién actualizado.
    Self.LoadAndPopulateHTTPConfig; // Método de TServerBase

    // PASO 3: Reaplica configuraciones base de Indy al objeto FServerInstance.
    Self.ApplyIndyBaseSettings; // Método de TServerBase

    // PASO 4: Reconfigura el IOHandler SSL basado en el nuevo FHTTPServerConfig.
    Self.ConfigureSSLFromConfig; // Método de TServerBase

    // NOTA: ConfigurePlatformServerInstance (que establece puerto, bindings, scheduler)
    // será llamado por Self.Start() si el servidor se reinicia.
    LogMessage('TWindowsWebServer: Internal configuration state refreshed from ANewAppConfig.', logInfo);
    SuccessfullyReconfiguredInternals := True;

  except
    on E: Exception do
    begin
      LogMessage(Format('TWindowsWebServer: CRITICAL ERROR during internal configuration refresh: %s - %s. ' +
        'Server was stopped (if previously running) and will NOT be restarted due to this error. ' +
        'Manual intervention likely required.',
        [E.ClassName, E.Message]), logFatal);
      // El servidor permanece detenido.
      raise; // Relanzar para que cualquier gestor superior se entere.
    end;
  end;

  if SuccessfullyReconfiguredInternals and WasRunning then
  begin
    LogMessage('TWindowsWebServer.ReloadConfiguration: Attempting to restart server with new configuration...', logInfo);
    Self.Start; // Start llamará a ConfigurePlatformServerInstance con la nueva configuración.
    if IsRunning then
      LogMessage('TWindowsWebServer.ReloadConfiguration: Server restarted successfully with new configuration.', logInfo)
    else
      LogMessage('TWindowsWebServer.ReloadConfiguration: FAILED to restart server after configuration reload. The server is stopped. Check logs.', logError);
  end
  else if SuccessfullyReconfiguredInternals and not WasRunning then
  begin
     LogMessage('TWindowsWebServer.ReloadConfiguration: Server was not running. Configuration has been updated internally. Call Start() explicitly if needed.', logInfo);
  end;
end;

procedure TWindowsWebServer.ConfigurePlatformServerInstance;
var
  LHTTPConfig: TServerHTTPConfig; // Usar la configuración ya parseada en TServerBase
begin
  LogMessage('TWindowsWebServer: Configuring Indy server instance (port, bindings, scheduler)...', logInfo);
  LHTTPConfig := Self.HTTPConfig; // Acceder a la propiedad de TServerBase

  HTTPServerInstance.DefaultPort := LHTTPConfig.Port;
  HTTPServerInstance.Bindings.Clear;

  var Binding := HTTPServerInstance.Bindings.Add;
  Binding.IP := '0.0.0.0'; // Escuchar en todas las interfaces por defecto
  Binding.Port := LHTTPConfig.Port;
  LogMessage(Format('Indy HTTP Server Binding configured: IP=%s, Port=%d', [Binding.IP, Binding.Port]), logDebug);

  if Assigned(HTTPServerInstance.Scheduler) and (HTTPServerInstance.Scheduler is TIdSchedulerOfThreadPool) then
  begin
    // Ya existe un ThreadPool, verificar si el tamaño necesita actualizarse
    if TIdSchedulerOfThreadPool(HTTPServerInstance.Scheduler).PoolSize <> LHTTPConfig.ThreadPoolSize then
    begin
      if LHTTPConfig.ThreadPoolSize > 0 then // Solo actualizar si el nuevo tamaño es válido para un pool
      begin
        TIdSchedulerOfThreadPool(HTTPServerInstance.Scheduler).PoolSize := LHTTPConfig.ThreadPoolSize;
        LogMessage(Format('Indy ThreadPool existing scheduler updated. New PoolSize: %d', [LHTTPConfig.ThreadPoolSize]), logDebug);
      end
      else // El nuevo tamaño configurado es <= 0, pero ya teníamos un ThreadPool. Revertir al default de Indy.
      begin
        var OldPoolSize := TIdSchedulerOfThreadPool(HTTPServerInstance.Scheduler).PoolSize; // ← Capturar ANTES de liberar
        FreeAndNil(HTTPServerInstance.Scheduler);
        LogMessage(Format('Indy ThreadPool scheduler removed due to configured ThreadPoolSize <= 0 (was %d). Using default thread-per-connection.',
            [OldPoolSize]), logInfo); // ← Usar valor capturado
      end;
    end
    // else: el tamaño es el mismo, no hacer nada.
  end
  else if LHTTPConfig.ThreadPoolSize > 0 then // No es TIdSchedulerOfThreadPool (o no hay scheduler) Y se desea un pool
  begin
    if Assigned(HTTPServerInstance.Scheduler) then // Liberar cualquier scheduler existente que no sea el de Pool
       FreeAndNil(HTTPServerInstance.Scheduler);

    var Scheduler := TIdSchedulerOfThreadPool.Create(HTTPServerInstance);
    Scheduler.PoolSize := LHTTPConfig.ThreadPoolSize;
    HTTPServerInstance.Scheduler := Scheduler; // TIdHTTPServer toma posesión
    LogMessage(Format('Indy ThreadPool scheduler created and assigned. PoolSize: %d', [LHTTPConfig.ThreadPoolSize]), logDebug);
  end
  else // LHTTPConfig.ThreadPoolSize <= 0, usar default de Indy (TIdSchedulerThreadDefault)
  begin
    if Assigned(HTTPServerInstance.Scheduler) and not (HTTPServerInstance.Scheduler is TIdSchedulerOfThreadDefault) then
    begin
      FreeAndNil(HTTPServerInstance.Scheduler); // Liberar scheduler custom para usar el default de Indy
      LogMessage('Indy ThreadPool scheduler (or other custom scheduler) removed to use default thread-per-connection (ThreadPoolSize <= 0).', logInfo);
    end
    else if not Assigned(HTTPServerInstance.Scheduler) then
    begin
      // --- INICIO: Logging explícito para el caso de default ---
      LogMessage('Indy Scheduler: ThreadPoolSize <= 0 and no custom scheduler assigned. Indy will use default (TIdSchedulerThreadDefault - thread-per-connection).', logInfo);
      // --- FIN: Logging explícito ---
    end
    // Si ya es TIdSchedulerThreadDefault, no es necesario hacer nada.
  end;

  LogMessage(Format('TWindowsWebServer: Indy Server Instance configured using HTTPConfig. Port=%d, Effective ThreadPoolSize (if pool used): %d. Using %s scheduler.',
    [LHTTPConfig.Port,
     IfThen(LHTTPConfig.ThreadPoolSize > 0, LHTTPConfig.ThreadPoolSize, 0), // Mostrar 0 si no se usa pool
     IfThen(LHTTPConfig.ThreadPoolSize > 0, 'TIdSchedulerOfThreadPool', 'TIdSchedulerThreadDefault (Indy default)') ]), logInfo);
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

    LogMessage(Format('TWindowsWebServer: Activating Indy HTTP Server on port %d...', [HTTPServerInstance.DefaultPort]), logInfo);
    HTTPServerInstance.Active := True;
    ServerState := ssRunning;

    // Gestión limpia del monitor thread
    if not Assigned(FMonitorThread) or FMonitorThread.Finished then
    begin
      // Limpiar thread anterior si terminó
      if Assigned(FMonitorThread) and FMonitorThread.Finished then
        FreeAndNil(FMonitorThread);

      // Resetear evento antes de crear nuevo thread
      FStopMonitorEvent.ResetEvent;

      // Crear y iniciar nuevo thread
      FMonitorThread := TThread.CreateAnonymousThread(MonitorResourcesProc);
      FMonitorThread.Start;
      LogMessage('TWindowsWebServer: Monitor thread started.', logDebug);
    end
    else
    begin
      LogMessage('TWindowsWebServer: Monitor thread already running.', logDebug);
    end;

    LogMessage(Format('TWindowsWebServer started successfully. Listening on port %d.',
      [HTTPServerInstance.DefaultPort]), logInfo);
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

    if Assigned(HTTPServerInstance) and HTTPServerInstance.Active then
    begin
      LogMessage('TWindowsWebServer: Deactivating Indy HTTP Server...', logInfo);
      HTTPServerInstance.Active := False;
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
  CHECK_INTERVAL_MS = 30000; // Revisar cada 30 segundos (ajustado desde 15000)
var
  LCurrentServerState: TServerState;
begin
  LogMessage('TWindowsWebServer: MonitorResourcesProc thread started.', logInfo);
  try
    while True do // Se romperá con FStopMonitorEvent o cambio de estado
    begin
      if FStopMonitorEvent.WaitFor(CHECK_INTERVAL_MS) = TWaitResult.wrSignaled then
      begin
        LogMessage('TWindowsWebServer: MonitorResourcesProc received stop signal. Exiting.', logInfo);
        Break;
      end;

      // Verificar el estado del servidor ANTES de realizar las comprobaciones
      // para asegurar que el hilo termine si el servidor se detiene por otra razón.
      LCurrentServerState := GetServerStateThreadSafe; // ← Usar método thread-safe de TServerBase
      if LCurrentServerState <> ssRunning then
        begin
          LogMessage(Format('TWindowsWebServer: MonitorResourcesProc detected server state is %s (not Running). Exiting loop.',
            [TRttiEnumerationType.GetName<TServerState>(LCurrentServerState)]), logInfo); // ← Usar variable local
          Break;
        end;
      // Si llegamos aquí, ServerState era ssRunning al momento de la verificación.
      try
        // Comprobación de alta cantidad de conexiones (ejemplo)
        if Assigned(HTTPServerInstance) and Assigned(HTTPServerInstance.Contexts) and (Self.HTTPConfig.MaxConnections > 0) then
        begin
          var LContextList := HTTPServerInstance.Contexts.LockList;
          try
            var LContextCount := LContextList.Count;

            if (LContextCount > (Self.HTTPConfig.MaxConnections * 0.9)) then // Umbral del 90%
            begin
              LogMessage(Format('TWindowsWebServer Monitor: High connection count detected (%d / %d).',
                [LContextCount, Self.HTTPConfig.MaxConnections]), logWarning);
              // Se podrían realizar otras acciones o solo loguear.
            end;
          finally
            HTTPServerInstance.Contexts.UnlockList; // ← Garantizar que siempre se libere
          end;
        end;
        // Aquí se podrían añadir otras verificaciones de recursos específicas de Windows si es necesario
        // ej. uso de memoria del proceso, descriptores de sistema, etc.
      except
        on E: Exception do
          LogMessage('TWindowsWebServer Monitor: Error during resource check: ' + E.Message, logError);
      end;
    end; // Fin while
  except
    on E: Exception do
      LogMessage(Format('TWindowsWebServer: Unhandled exception in MonitorResourcesProc: %s - %s. Thread terminating.', [E.ClassName, E.Message]), logCritical);
  end;
  LogMessage('TWindowsWebServer: MonitorResourcesProc thread finished.', logInfo);
end;

end.


