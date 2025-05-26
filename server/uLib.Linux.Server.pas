unit uLib.Linux.Server;

interface

uses
  System.SysUtils, System.Classes, System.JSON,
  Posix.Base, Posix.Signal, Posix.Unistd, Posix.Fcntl, // Added Posix.Fcntl for open
  IdHTTPWebBrokerBridge, IdSchedulerOfThreadPool, // IdSchedulerOfThreadPool para configurar el pool de Indy
  uLib.Server.Base, // Hereda de TServerBase
  uLib.Server.Types,  // Para TServerState, EConfigurationError, TServerHTTPConfig
  uLib.Logger;      // Para LogMessage

type
  TLinuxWebServer = class(TServerBase)
  private
    FPIDFilePath: string; // Path completo al archivo PID

    procedure CreatePIDFile;
    procedure RemovePIDFile;
    // procedure MonitorSystemResources; // Comentado por dependencias faltantes (Metrics, GetOpenFileCount)
    // function GetProcessMemoryInfo: Int64;

  protected
    // Implementación de métodos abstractos de TServerBase
    procedure ConfigurePlatformServerInstance; override;
    procedure CleanupServerResources; override; // Para limpieza específica de Linux si es necesaria además de TServerBase
    procedure PerformShutdownTasks; override;   // Para tareas antes de que Indy se detenga

    // Funcionalidad específica de Linux
    procedure DaemonizeProcess;

  public
    constructor Create(AAppConfig: TJSONObject); override; // Constructor compatible con TServerBase
    destructor Destroy; override;

    procedure Start; override;
    procedure Stop; override;
    procedure ReloadConfiguration; // Para recargar la configuración en caliente (reaplica al TIdHTTPServer)
  end;

implementation

uses
  System.IOUtils, // Para TPath, TFile
  System.StrUtils, // Para IfThen, SameText, Format
  uLib.Base;     // Para GetStr, GetInt, GetBool (asumido)

{ TLinuxWebServer }

constructor TLinuxWebServer.Create(AAppConfig: TJSONObject);
var
  ServerConfigSection: TJSONObject;
  BasePathConfig, PIDFileNameFromConfig: string;
begin
  // 1. Llamar al constructor de TServerBase PRIMERO.
  //    Esto clona AAppConfig a Self.AppConfig (propiedad de TServerBase),
  //    crea FServerInstance (TIdHTTPServer), llama a LoadAndPopulateHTTPConfig (que puebla Self.HTTPConfig),
  //    llama a ApplyIndyBaseSettings, configura SSL si es necesario, y llama a InitializeFrameworkComponents.
  inherited Create(AAppConfig);

  LogMessage('TLinuxWebServer creating using provided AAppConfig...', logInfo);

  // 2. Obtener configuraciones específicas de Linux de Self.HTTPConfig (que fue poblado desde AppConfig.server)
  //    o directamente de Self.AppConfig si son configuraciones de plataforma más allá de HTTP.
  //    FPIDFilePath se construye usando Self.HTTPConfig.BasePath y Self.HTTPConfig.PIDFile.
  if Assigned(Self.HTTPConfig) then // HTTPConfig es TServerHTTPConfig de TServerBase
  begin
    // Self.HTTPConfig.BasePath y Self.HTTPConfig.PIDFile ya fueron inicializados
    // por TServerBase.LoadAndPopulateHTTPConfig con defaults o valores de "server" en config.json.
    if Self.HTTPConfig.BasePath.Trim <> '' then
      FPIDFilePath := TPath.Combine(Self.HTTPConfig.BasePath, Self.HTTPConfig.PIDFile)
    else // Si BasePath no se pudo determinar o está vacío, usar directorio de la app
      FPIDFilePath := TPath.Combine(ExtractFilePath(ParamStr(0)), Self.HTTPConfig.PIDFile);
    LogMessage(Format('TLinuxWebServer: PIDFile path determined from HTTPConfig: "%s"', [FPIDFilePath]), logDebug);
  end
  else
  begin
    // Esto no debería ocurrir si TServerBase.Create funcionó correctamente.
    LogMessage('TLinuxWebServer.Create: Self.HTTPConfig is not assigned. Using default PID path.', logError);
    FPIDFilePath := TPath.Combine(ExtractFilePath(ParamStr(0)), 'server.pid');
  end;

  // El manejo de señales de terminación (SIGINT, SIGTERM) es centralizado por TProcessManager.
  LogMessage(Format('TLinuxWebServer created. PID file will be: %s', [FPIDFilePath]), logInfo);
end;

destructor TLinuxWebServer.Destroy;
begin
  LogMessage('TLinuxWebServer destroying...', logInfo);
  // Stop ya debería haber sido llamado por TServerManager o el flujo de apagado.
  // CleanupServerResources se llama desde inherited Destroy de TServerBase.
  // Aquí solo recursos específicos de TLinuxWebServer que no se limpian en CleanupServerResources.
  // RemovePIDFile se llama en CleanupServerResources ahora.
  LogMessage('TLinuxWebServer destroyed.', logInfo);
  inherited;
end;

procedure TLinuxWebServer.ConfigurePlatformServerInstance;
var
  LHTTPConfig: TServerHTTPConfig; // Usar la configuración ya parseada en TServerBase
begin
  LogMessage('TLinuxWebServer: Configuring Indy server instance (port, bindings, scheduler)...', logInfo);
  LHTTPConfig := Self.HTTPConfig; // Acceder a la propiedad de TServerBase

  // HTTPServerInstance es el TIdHTTPServer de TServerBase
  HTTPServerInstance.DefaultPort := LHTTPConfig.Port;

  // Limpiar bindings existentes antes de añadir uno nuevo o actualizar
  // Esto es más robusto si la configuración de puerto cambia.
  HTTPServerInstance.Bindings.Clear;
  var Binding := HTTPServerInstance.Bindings.Add;
  Binding.IP := '0.0.0.0'; // Escuchar en todas las interfaces por defecto
  Binding.Port := LHTTPConfig.Port;
  LogMessage(Format('Indy HTTP Server Binding configured: IP=%s, Port=%d', [Binding.IP, Binding.Port]), logDebug);

  // Configurar el Scheduler (Pool de Threads) de Indy
  if Assigned(HTTPServerInstance.Scheduler) and (HTTPServerInstance.Scheduler is TIdSchedulerOfThreadPool) then
  begin
    if TIdSchedulerOfThreadPool(HTTPServerInstance.Scheduler).PoolSize <> LHTTPConfig.ThreadPoolSize then
    begin
      TIdSchedulerOfThreadPool(HTTPServerInstance.Scheduler).PoolSize := LHTTPConfig.ThreadPoolSize;
      LogMessage(Format('Indy ThreadPool existing scheduler updated. New PoolSize: %d', [LHTTPConfig.ThreadPoolSize]), logDebug);
    end;
  end
  else if LHTTPConfig.ThreadPoolSize > 0 then
  begin
    if Assigned(HTTPServerInstance.Scheduler) then // Si existe un scheduler pero no es el de Pool
       FreeAndNil(HTTPServerInstance.Scheduler);

    var Scheduler := TIdSchedulerOfThreadPool.Create(HTTPServerInstance);
    Scheduler.PoolSize := LHTTPConfig.ThreadPoolSize;
    HTTPServerInstance.Scheduler := Scheduler; // TIdHTTPServer toma posesión
    LogMessage(Format('Indy ThreadPool scheduler created and assigned. PoolSize: %d', [LHTTPConfig.ThreadPoolSize]), logDebug);
  end
  else // LHTTPConfig.ThreadPoolSize = 0, usar default de Indy (TIdSchedulerThreadDefault)
  begin
    if Assigned(HTTPServerInstance.Scheduler) and not (HTTPServerInstance.Scheduler is TIdSchedulerThreadDefault) then
    begin
      FreeAndNil(HTTPServerInstance.Scheduler);
      LogMessage('Indy ThreadPool scheduler removed to use default thread-per-connection.', logDebug);
    end;
  end;

  LogMessage(Format('TLinuxWebServer: Indy Server Instance configured using TServerHTTPConfig. Port=%d, PoolSize=%d.',
    [LHTTPConfig.Port, LHTTPConfig.ThreadPoolSize]), logInfo);
end;

procedure TLinuxWebServer.Start;
begin
  if IsRunning then // IsRunning es de TServerBase
  begin
    LogMessage('TLinuxWebServer.Start: Server is already running.', logInfo);
    Exit;
  end;

  LogMessage('TLinuxWebServer: Starting server process...', logInfo);
  ServerState := ssStarting; // Propiedad de TServerBase
  try
    // 1. Demonizar si está configurado (solo en Linux)
    {$IFDEF LINUX}
    if Self.HTTPConfig.Daemonize then // Usar FHTTPServerConfig de TServerBase
    begin
      LogMessage('TLinuxWebServer: Daemonizing process...', logInfo);
      DaemonizeProcess; // El padre terminará aquí si la demonización es exitosa
    end;
    {$ENDIF}

    // 2. (Re)Configurar la instancia del servidor Indy (puerto, pool, etc.)
    //    Esto asegura que la configuración de Indy esté actualizada antes de activar.
    ConfigurePlatformServerInstance;

    // 3. Crear archivo PID (después de demonizar, si aplica, para tener el PID del demonio)
    CreatePIDFile;

    // 4. Activar el servidor Indy
    LogMessage(Format('TLinuxWebServer: Activating Indy HTTP Server on port %d...', [HTTPServerInstance.DefaultPort]), logInfo);
    HTTPServerInstance.Active := True;
    ServerState := ssRunning; // Marcar como corriendo DESPUÉS de que Active sea true

    LogMessage(Format('TLinuxWebServer started successfully. Listening on port %d. PID: %d',
      [HTTPServerInstance.DefaultPort, getpid]), logInfo);

  except
    on E: Exception do
    begin
      ServerState := ssError;
      LogMessage(Format('TLinuxWebServer: Failed to start server: %s - %s', [E.ClassName, E.Message]), logFatal);
      RemovePIDFile;
      raise;
    end;
  end;
end;

procedure TLinuxWebServer.Stop;
begin
  if (ServerState = ssStopped) or (ServerState = ssStopping) then
  begin
    LogMessage(Format('TLinuxWebServer.Stop: Server is already %s.', [TRttiEnumerationType.GetName<TServerState>(ServerState)]), logInfo);
    Exit;
  end;

  LogMessage('TLinuxWebServer: Stopping server process...', logInfo);
  ServerState := ssStopping;
  try
    PerformShutdownTasks; // Esperar conexiones activas

    if Assigned(HTTPServerInstance) and HTTPServerInstance.Active then
    begin
      LogMessage('TLinuxWebServer: Deactivating Indy HTTP Server...', logInfo);
      HTTPServerInstance.Active := False;
    end;

    // RemovePIDFile se llama en CleanupServerResources, que es llamado por el destructor de TServerBase,
    // o podría llamarse explícitamente aquí si se quiere asegurar antes de que el objeto se destruya.
    // Por consistencia con CleanupServerResources, se podría dejar allí.
    // Sin embargo, para un Stop explícito, es bueno limpiar el PID.
    RemovePIDFile;

    ServerState := ssStopped;
    LogMessage('TLinuxWebServer stopped successfully.', logInfo);
  except
    on E: Exception do
    begin
      ServerState := ssError;
      LogMessage(Format('TLinuxWebServer: Error stopping server: %s - %s', [E.ClassName, E.Message]), logError);
    end;
  end;
end;

procedure TLinuxWebServer.ReloadConfiguration;
begin
  LogMessage('TLinuxWebServer: Reloading configuration for active server instance...', logInfo);
  if ServerState <> ssRunning then
  begin
    LogMessage('TLinuxWebServer.ReloadConfiguration: Server is not running. Cannot reload.', logWarning);
    Exit;
  end;

  // Asumimos que Self.AppConfig (el TJSONObject en TServerBase) ya ha sido
  // actualizado por TServerManager.UpdateConfiguration(NewConfig).
  // TServerBase.LoadAndPopulateHTTPConfig debe ser llamado para actualizar Self.HTTPConfig.
  // Luego, ConfigurePlatformServerInstance reaplica al servidor Indy.
  try
    LogMessage('TLinuxWebServer: Re-populating HTTPConfig and re-applying to Indy server instance...', logDebug);

    // 1. Repoblar FHTTPServerConfig desde el FAppConfig (que TServerManager actualizó)
    //    TServerBase necesita un método para esto, o se duplica la lógica aquí.
    //    Mejor: TServerBase debería tener un método protegido para recargar su FHTTPServerConfig desde FAppConfig.
    //    Por ahora, asumimos que TServerManager.UpdateConfiguration -> InitializeServerInstance (que recrea TLinuxWebServer) es el flujo principal.
    //    Si esta ReloadConfiguration es para una recarga "más ligera" sin recrear TLinuxWebServer:
    Self.LoadAndPopulateHTTPConfig; // Método de TServerBase para re-leer de Self.AppConfig a Self.HTTPConfig
    Self.ApplyIndyBaseSettings;     // Reaplica settings base de Indy
    if Self.HTTPConfig.SSLEnabled then // Reconfigurar SSL
        Self.ConfigureSSLFromConfig
    else if Assigned(Self.FSSLIOHandler) then // Si SSL estaba y ahora no, quitarlo
    begin
        if Assigned(Self.HTTPServerInstance) and (Self.HTTPServerInstance.IOHandler = Self.FSSLIOHandler) then
            Self.HTTPServerInstance.IOHandler := nil;
        FreeAndNil(Self.FSSLIOHandler);
        Self.HTTPConfig.SSLEnabled := False; // Asegurar que el flag en config refleje el estado
    end;

    // 2. Reaplica la configuración específica de la plataforma (puerto, bindings, scheduler)
    ConfigurePlatformServerInstance;

    LogMessage('TLinuxWebServer: Configuration re-applied to Indy server instance.', logInfo);
  except
    on E: Exception do
      LogMessage(Format('TLinuxWebServer: Error reloading/re-applying configuration: %s - %s', [E.ClassName, E.Message]), logError);
  end;
end;

procedure TLinuxWebServer.CreatePIDFile;
begin
  if FPIDFilePath.IsEmpty then
  begin
    LogMessage('CreatePIDFile: PID file path is empty. Cannot create PID file.', logWarning);
    Exit;
  end;
  try
    TFile.WriteAllText(FPIDFilePath, IntToStr(getpid), TEncoding.ASCII);
    LogMessage(Format('PID file created: %s (PID: %d)', [FPIDFilePath, getpid]), logDebug);
  except
    on E: Exception do
      LogMessage(Format('Error creating PID file "%s": %s - %s', [FPIDFilePath, E.ClassName, E.Message]), logError);
  end;
end;

procedure TLinuxWebServer.RemovePIDFile;
begin
  if FPIDFilePath.IsEmpty then Exit;

  if TFile.Exists(FPIDFilePath) then
  begin
    try
      TFile.Delete(FPIDFilePath);
      LogMessage(Format('PID file removed: %s', [FPIDFilePath]), logDebug);
    except
      on E: Exception do
        LogMessage(Format('Error removing PID file "%s": %s - %s', [FPIDFilePath, E.ClassName, E.Message]), logError);
    end;
  end
  else
    LogMessage(Format('RemovePIDFile: PID file not found at "%s". Nothing to remove.', [FPIDFilePath]), logSpam);
end;

procedure TLinuxWebServer.DaemonizeProcess;
{$IFDEF LINUX}
var
  pid: pid_t;
  FileDesc: Integer;
  SigActionRec: TSigActionRec; // De Posix.Signal
{$ENDIF}
begin
{$IFDEF LINUX}
  LogMessage('Attempting to daemonize process...', logInfo);
  pid := Posix.Unistd.fork;
  if pid < 0 then
    raise EServerStartError.Create('Daemonize: First fork failed: ' + SysErrorMessage(Posix.Base.GetErrno));
  if pid > 0 then // Proceso padre original
  begin
    LogMessage(Format('Daemonize: First fork successful. Parent (PID %d) exiting.', [getpid]), logDebug);
    System.SysUtils.Terminate; // Terminar el padre limpiamente
  end;
  // En el primer hijo

  if Posix.Unistd.setsid < 0 then // Crear nueva sesión
    raise EServerStartError.Create('Daemonize: setsid failed: ' + SysErrorMessage(Posix.Base.GetErrno));
  LogMessage(Format('Daemonize: New session created (Child PID %d).', [getpid]), logDebug);

  // Ignorar SIGHUP
  FillChar(SigActionRec, SizeOf(TSigActionRec), 0);
  SigActionRec.sa_handler_ptr := Pointer(SIG_IGN);
  if Posix.Signal.sigaction(SIGHUP, @SigActionRec, nil) <> 0 then
     LogMessage('Daemonize: Failed to set SIGHUP to SIG_IGN: ' + SysErrorMessage(Posix.Base.GetErrno), logWarning);

  pid := Posix.Unistd.fork; // Segundo fork
  if pid < 0 then
    raise EServerStartError.Create('Daemonize: Second fork failed: ' + SysErrorMessage(Posix.Base.GetErrno));
  if pid > 0 then // Primer hijo
  begin
    LogMessage('Daemonize: Second fork successful. Intermediate parent exiting.', logDebug);
    System.SysUtils.Terminate;
  end;
  // En el demonio (segundo hijo)

  if Posix.Unistd.chdir('/') <> 0 then // Cambiar a directorio raíz
    LogMessage('Daemonize: Failed to change directory to root: ' + SysErrorMessage(Posix.Base.GetErrno), logWarning);

  Posix.Unistd.umask(0); // Limpiar umask

  LogMessage('Daemonize: Closing standard file descriptors and redirecting to /dev/null.', logDebug);
  FileDesc := Posix.Fcntl.open('/dev/null', O_RDWR); // O_RDWR de Posix.Fcntl
  if FileDesc <> -1 then
  begin
    Posix.Unistd.dup2(FileDesc, STDIN_FILENO);  // stdin (0)
    Posix.Unistd.dup2(FileDesc, STDOUT_FILENO); // stdout (1)
    Posix.Unistd.dup2(FileDesc, STDERR_FILENO);  // stderr (2)
    if FileDesc > STDERR_FILENO then // No cerrar si FileDesc es uno de los std handles
       Posix.Unistd.Close(FileDesc);
  end
  else
    LogMessage('Daemonize: Failed to open /dev/null for redirection: ' + SysErrorMessage(Posix.Base.GetErrno), logWarning);

  LogMessage('Process daemonized successfully.', logInfo);
{$ELSE}
  LogMessage('DaemonizeProcess called on non-Linux platform. Operation skipped.', logWarning);
{$ENDIF}
end;

procedure TLinuxWebServer.PerformShutdownTasks;
var
  ServerConfigSection: TJSONObject; // No se usa aquí, se usa Self.HTTPConfig
  MaxWaitSeconds: Integer;
  StartTime: TDateTime;
  ActiveContextsCount: Integer;
begin
  LogMessage('TLinuxWebServer: Performing pre-stop shutdown tasks...', logDebug);

  MaxWaitSeconds := Self.HTTPConfig.ShutdownGracePeriodSeconds; // De TServerHTTPConfig

  if Assigned(HTTPServerInstance) and Assigned(HTTPServerInstance.Contexts) then
  begin
    StartTime := NowUTC;
    ActiveContextsCount := HTTPServerInstance.Contexts.LockList.Count;
    LogMessage(Format('TLinuxWebServer: %d active Indy contexts at shutdown initiation.', [ActiveContextsCount]), logInfo);

    while (ActiveContextsCount > 0) and (SecondsBetween(NowUTC, StartTime) < MaxWaitSeconds) do
    begin
      LogMessage(Format('TLinuxWebServer: Waiting for %d active Indy contexts to close (remaining time: %d sec)...',
        [ActiveContextsCount, MaxWaitSeconds - Abs(SecondsBetween(NowUTC, StartTime))]), logInfo); // Usar Abs por si hay desfase de reloj
      Sleep(1000);
      ActiveContextsCount := HTTPServerInstance.Contexts.LockList.Count;
    end;

    ActiveContextsCount := HTTPServerInstance.Contexts.LockList.Count;
    if ActiveContextsCount > 0 then
      LogMessage(Format('TLinuxWebServer: Shutdown grace period ended. %d active Indy contexts may be cut off.', [ActiveContextsCount]), logWarning)
    else
      LogMessage('TLinuxWebServer: All active Indy contexts closed gracefully within grace period.', logInfo);
  end;
  LogMessage('TLinuxWebServer: Pre-stop shutdown tasks completed.', logDebug);
end;

procedure TLinuxWebServer.CleanupServerResources;
begin
  LogMessage('TLinuxWebServer: Cleaning up platform-specific resources (PID file)...', logDebug);
  RemovePIDFile;
  inherited; // Llama a TServerBase.CleanupFrameworkResources
end;

end.

