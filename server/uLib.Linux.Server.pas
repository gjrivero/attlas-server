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

    function GetConfiguredMaxFileDescriptors: Integer;
    procedure CreatePIDFile;
    procedure RemovePIDFile;
    procedure SetDaemonEnvironmentVariables;
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
    procedure ReloadConfiguration(ANewAppConfig: TJSONObject);
  end;

implementation

uses
  System.IOUtils, // Para TPath, TFile
  System.StrUtils, // Para IfThen, SameText, Format
  System.Math,
  uLib.Utils;     // Para GetStr, GetInt, GetBool (asumido)

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
  LogMessage(Format('TLinuxWebServer: Indy Server Instance configured using HTTPConfig. Port=%d, Effective ThreadPoolSize (if pool used): %d. Using %s scheduler.',
   [LHTTPConfig.Port,
   IfThen(LHTTPConfig.ThreadPoolSize > 0, LHTTPConfig.ThreadPoolSize, 0),
   IfThen(LHTTPConfig.ThreadPoolSize > 0, 'TIdSchedulerOfThreadPool', 'TIdSchedulerThreadDefault (Indy default)') ]), logInfo);
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

procedure TLinuxWebServer.ReloadConfiguration(ANewAppConfig: TJSONObject);
var
  WasRunning: Boolean;
  SuccessfullyReconfiguredInternals: Boolean;
begin
  LogMessage('TLinuxWebServer: Reloading configuration using provided ANewAppConfig...', logInfo);
  SuccessfullyReconfiguredInternals := False;
  WasRunning := IsRunning;

  if WasRunning then
  begin
    LogMessage('TLinuxWebServer.ReloadConfiguration: Stopping server to apply configuration changes.', logInfo);
    Self.Stop; // Detiene el servidor Indy y limpia el PID. ServerState debería pasar a ssStopped.
  end;

  try
    // PASO 1: Actualizar FAppConfig en TServerBase con la nueva configuración.
    // Esto actualiza la fuente principal de donde se leerán otras configuraciones.
    Self.InternalRefreshAppConfig(ANewAppConfig);

    // PASO 2: Repoblar FHTTPServerConfig (puerto, timeouts, rutas SSL, etc.)
    // desde el FAppConfig recién actualizado.
    Self.LoadAndPopulateHTTPConfig;

    // PASO 3: Reaplica configuraciones base de Indy (KeepAlive, ServerSoftware, MaxConnections,
    // TerminateWaitTime) al objeto FServerInstance existente.
    Self.ApplyIndyBaseSettings;

    // PASO 4: Reconfigura el IOHandler SSL basado en el nuevo FHTTPServerConfig.
    // Esto puede crear/destruir/reasignar FSSLIOHandler en FServerInstance.IOHandler.
    Self.ConfigureSSLFromConfig;

    // NOTA: ConfigurePlatformServerInstance (que establece puerto, bindings, scheduler)
    // NO se llama aquí directamente. Será llamado por Self.Start() si el servidor se reinicia.
    // Si el servidor no estaba corriendo, estos cambios en FHTTPServerConfig
    // (ej. puerto, threadPoolSize) se aplicarán la próxima vez que se llame a Start().

    LogMessage('TLinuxWebServer: Internal configuration state (FAppConfig, FHTTPServerConfig, Indy base settings, SSL IOHandler) refreshed from ANewAppConfig.', logInfo);
    SuccessfullyReconfiguredInternals := True;

  except
    on E: Exception do
    begin
      // Si falla cualquier paso de la reconfiguración interna, el estado puede ser inconsistente.
      // FAppConfig y FHTTPServerConfig pueden tener los nuevos valores (potencialmente problemáticos).
      LogMessage(Format('TLinuxWebServer: CRITICAL ERROR during internal configuration refresh: %s - %s. ' +
        'Server was stopped (if previously running) and will NOT be restarted due to this error. ' +
        'The internal configuration might reflect the new (problematic) settings. Manual intervention likely required.',
        [E.ClassName, E.Message]), logFatal);
      // El servidor permanece detenido (si se detuvo). ServerState debería ser ssStopped o ssError.
      raise; // Relanzar para que cualquier gestor superior se entere del fallo crítico.
    end;
  end;

  // Solo intentar reiniciar si la reconfiguración interna fue exitosa Y el servidor estaba corriendo antes.
  if SuccessfullyReconfiguredInternals and WasRunning then
  begin
    LogMessage('TLinuxWebServer.ReloadConfiguration: Attempting to restart server with new configuration...', logInfo);
    Self.Start; // Start llamará a ConfigurePlatformServerInstance, que usará el FHTTPServerConfig actualizado
                // para configurar puerto, bindings, scheduler, etc.
    if IsRunning then
      LogMessage('TLinuxWebServer.ReloadConfiguration: Server restarted successfully with new configuration.', logInfo)
    else
      LogMessage('TLinuxWebServer.ReloadConfiguration: FAILED to restart server after configuration reload. The server is stopped. Check logs for errors from Start() method itself (e.g., port in use, scheduler error).', logError);
  end
  else if SuccessfullyReconfiguredInternals and not WasRunning then
  begin
     LogMessage('TLinuxWebServer.ReloadConfiguration: Server was not running. Configuration has been updated internally. Call Start() explicitly if needed to apply all changes (like port bindings).', logInfo);
  end
  // Si SuccessfullyReconfiguredInternals es false, ya se logueó un error fatal y se relanzó la excepción.
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
  SigActionRec: TSigActionRec;
  ConfigMgr: TConfigManager;
  WorkingDir, DevNullPath, DaemonPath: string;
{$ENDIF}
begin
{$IFDEF LINUX}
  LogMessage('Attempting to daemonize process...', logInfo);

  // Obtener configuración de daemonización
  try
    ConfigMgr := TConfigManager.GetInstance;
    if Assigned(ConfigMgr) and Assigned(ConfigMgr.ConfigData) then
    begin
      WorkingDir := TJSONHelper.GetString(ConfigMgr.ConfigData, 'server.daemon.workingDirectory', '/');
      DevNullPath := TJSONHelper.GetString(ConfigMgr.ConfigData, 'server.daemon.nullDevice', '/dev/null');
      DaemonPath := TJSONHelper.GetString(ConfigMgr.ConfigData, 'server.daemon.path', '/bin:/usr/bin:/usr/local/bin');
    end
    else
    begin
      // Valores por defecto si no hay configuración
      WorkingDir := '/';
      DevNullPath := '/dev/null';
      DaemonPath := '/bin:/usr/bin:/usr/local/bin';
      LogMessage('No daemon configuration found, using default values', logWarning);
    end;
  except
    on E: Exception do
    begin
      LogMessage(Format('Error reading daemon configuration: %s. Using defaults.', [E.Message]), logWarning);
      WorkingDir := '/';
      DevNullPath := '/dev/null';
      DaemonPath := '/bin:/usr/bin:/usr/local/bin';
    end;
  end;

  // Validar configuración
  if not TDirectory.Exists(WorkingDir) then
  begin
    LogMessage(Format('Configured working directory "%s" does not exist, using root', [WorkingDir]), logWarning);
    WorkingDir := '/';
  end;

  if not TFile.Exists(DevNullPath) then
  begin
    LogMessage(Format('Configured null device "%s" does not exist, using /dev/null', [DevNullPath]), logWarning);
    DevNullPath := '/dev/null';
  end;

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

  // Cambiar al directorio de trabajo configurado
  if Posix.Unistd.chdir(PAnsiChar(AnsiString(WorkingDir))) <> 0 then
    LogMessage(Format('Daemonize: Failed to change directory to "%s": %s',
      [WorkingDir, SysErrorMessage(Posix.Base.GetErrno)]), logWarning);

  Posix.Unistd.umask(0); // Limpiar umask

  LogMessage(Format('Daemonize: Closing standard file descriptors and redirecting to %s.', [DevNullPath]), logDebug);
  FileDesc := Posix.Fcntl.open(PAnsiChar(AnsiString(DevNullPath)), O_RDWR);
  if FileDesc <> -1 then
  begin
    Posix.Unistd.dup2(FileDesc, STDIN_FILENO);  // stdin (0)
    Posix.Unistd.dup2(FileDesc, STDOUT_FILENO); // stdout (1)
    Posix.Unistd.dup2(FileDesc, STDERR_FILENO);  // stderr (2)
    if FileDesc > STDERR_FILENO then // No cerrar si FileDesc es uno de los std handles
       Posix.Unistd.Close(FileDesc);
  end
  else
    LogMessage(Format('Daemonize: Failed to open %s for redirection: %s',
      [DevNullPath, SysErrorMessage(Posix.Base.GetErrno)]), logWarning);

  // Cerrar otros descriptores de archivo heredados del padre
  var MaxFD := GetConfiguredMaxFileDescriptors;
  for var fd := 3 to MaxFD do
  begin
    try
      Posix.Unistd.Close(fd); // Intentar cerrar cada descriptor
    except
      // Ignorar errores - algunos FDs pueden no estar abiertos
    end;
  end;

  // Establecer variables de entorno limpias para el daemon
  if Posix.Stdlib.setenv('PATH', PAnsiChar(AnsiString(DaemonPath)), 1) <> 0 then
    LogMessage(Format('Daemonize: Failed to set PATH to "%s"', [DaemonPath]), logWarning);

  Posix.Stdlib.unsetenv('HOME');
  Posix.Stdlib.unsetenv('USER');
  Posix.Stdlib.unsetenv('LOGNAME');

  // Establecer variables específicas del daemon si están configuradas
  SetDaemonEnvironmentVariables;

  LogMessage('Process daemonized successfully with configured settings.', logInfo);
{$ELSE}
  LogMessage('DaemonizeProcess called on non-Linux platform. Operation skipped.', logWarning);
{$ENDIF}
end;

{$IFDEF LINUX}
function TLinuxWebServer.GetConfiguredMaxFileDescriptors: Integer;
var
  ConfigMgr: TConfigManager;
begin
  Result := 256; // Default

  try
    ConfigMgr := TConfigManager.GetInstance;
    if Assigned(ConfigMgr) and Assigned(ConfigMgr.ConfigData) then
    begin
      Result := TJSONHelper.GetInteger(ConfigMgr.ConfigData, 'server.monitoring.maxFileDescriptors', Result);

      // Validar rango razonable
      if Result < 64 then
        Result := 64
      else if Result > 65536 then
        Result := 65536;
    end;
  except
    on E: Exception do
    begin
      LogMessage(Format('Error reading maxFileDescriptors configuration: %s', [E.Message]), logWarning);
    end;
  end;
end;

procedure TLinuxWebServer.SetDaemonEnvironmentVariables;
var
  ConfigMgr: TConfigManager;
  EnvVars: TJSONObject;
  EnvPair: TJSONPair;
  i: Integer;
begin
  try
    ConfigMgr := TConfigManager.GetInstance;
    if Assigned(ConfigMgr) and Assigned(ConfigMgr.ConfigData) then
    begin
      EnvVars := TJSONHelper.GetJSONObject(ConfigMgr.ConfigData, 'server.daemon.environment');
      if Assigned(EnvVars) then
      begin
        for i := 0 to EnvVars.Count - 1 do
        begin
          EnvPair := EnvVars.Pairs[i];
          if Assigned(EnvPair) and Assigned(EnvPair.JsonValue) then
          begin
            var EnvName := EnvPair.JsonString.Value;
            var EnvValue := EnvPair.JsonValue.Value;

            if Posix.Stdlib.setenv(PAnsiChar(AnsiString(EnvName)), PAnsiChar(AnsiString(EnvValue)), 1) = 0 then
              LogMessage(Format('Daemon environment: Set %s=%s', [EnvName, EnvValue]), logDebug)
            else
              LogMessage(Format('Daemon environment: Failed to set %s=%s', [EnvName, EnvValue]), logWarning);
          end;
        end;
      end;
    end;
  except
    on E: Exception do
    begin
      LogMessage(Format('Error setting daemon environment variables: %s', [E.Message]), logWarning);
    end;
  end;
end;
{$ENDIF}

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

