program AttlasServer;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.IOUtils,
  System.JSON,
  System.Rtti,
  uLib.Logger,
  uLib.Utils,
  uController.System,
  uController.Customers,
  uGlobalDefinitions in 'uGlobalDefinitions.pas',
  uLib.Config.Manager in 'server\uLib.Config.Manager.pas',
  uLib.Process.Manager in 'server\uLib.Process.Manager.pas',
  uLib.Server.Manager in 'server\uLib.Server.Manager.pas',
  uLib.Database.Pool in 'database\uLib.Database.Pool.pas',
  uLib.Database.Types in 'database\uLib.Database.Types.pas',
  uLib.Controller.Base in 'controllers\uLib.Controller.Base.pas',
  uLib.Routes in 'server\uLib.Routes.pas',
  uLib.Server.Base in 'server\uLib.Server.Base.pas',
  uLib.Session.Manager in 'server\uLib.Session.Manager.pas';

{$R *.res}

var
  GAppBasePath: String; // Ruta base de la aplicación
  GDBPoolManager: TDBConnectionPoolManager;
  GWebServerManager: TServerManager;

function StringToLogLevel(const S: string; DefaultLevel: TLogLevel): TLogLevel;
var
  TempLogLevelStr: string;
begin
  Result := DefaultLevel;
  TempLogLevelStr := LowerCase(Trim(S));
  if TempLogLevelStr = 'none' then Result := logNone
  else if TempLogLevelStr = 'fatal' then Result := logFatal
  else if TempLogLevelStr = 'critical' then Result := logCritical
  else if TempLogLevelStr = 'error' then Result := logError
  else if TempLogLevelStr = 'warning' then Result := logWarning
  else if TempLogLevelStr = 'info' then Result := logInfo
  else if TempLogLevelStr = 'debug' then Result := logDebug
  else if TempLogLevelStr = 'spam' then Result := logSpam
  else
    LogMessage('Warning: Invalid logLevel string "' + S + '" encountered during parse. Using default.', logWarning); // Log si el string no es reconocido
end;

procedure InitializeAndRunApplication;
var
  LAppConfig: TJSONObject;
  LDBPoolConfigsArray: TJSONArray;
  // LDBMonitorInstance: IDBMonitor; // Opcional, si se implementa un monitor de BD
  AppSettings: TJSONObject;
  LogLevelStr: string;
  ParsedLogLevel: TLogLevel;
begin
  LAppConfig := nil;
  // LDBMonitorInstance := nil;

  try
    // 1. Inicializar el Logger lo antes posible
    // El nombre del archivo de log y el nivel podrían venir de una config mínima inicial o defaults.
    InitializeLog(GAppBasePath + 'mercadosaint_server.log', logDebug, True, True); // Nivel Debug para desarrollo, salida a consola y archivo
    LogMessage('Application starting. Base path: ' + GAppBasePath, logInfo);

    // 2. Inicializar el Gestor de Configuración
    // TConfigManager.GetInstance se asegura que se cree el Singleton
    // y si se le pasa un path, lo usa para Initialize si aún no tiene uno.
    GConfigManager := TConfigManager.GetInstance(GAppBasePath); // GAppBasePath debe tener el trailing path delimiter
    if GConfigManager.ConfigFilePath.IsEmpty then
    begin
      LogMessage('CRITICAL: Configuration file path not set in TConfigManager. Application cannot start.', logFatal);
      Exit; // No se puede continuar sin configuración
    end;

    LAppConfig := GConfigManager.GetGlobalConfigClone; // Obtener un clon de la configuración cargada
    if (not Assigned(LAppConfig)) or (LAppConfig.Count = 0) then
    begin
      LogMessage('CRITICAL: Failed to load application configuration from ' + GConfigManager.ConfigFilePath + '. Application cannot start.', logFatal);
      Exit;
    end;
    LogMessage('Application configuration loaded successfully from: ' + GConfigManager.ConfigFilePath, logInfo);

    // (Opcional pero recomendado) Configurar el nivel de log desde el archivo de configuración
    if LAppConfig.TryGetValue('application', AppSettings) and Assigned(AppSettings) then
    begin
      LogLevelStr := TJSONHelper.GetString(AppSettings, 'logLevel', 'Info'); // Default a Info si no está en config
      ParsedLogLevel := StringToLogLevel(LogLevelStr, logInfo); // Usar logInfo como default si el parseo dentro de StringToLogLevel falla
      if ParsedLogLevel <> GetCurrentLogLevel then // GetLogLevel sería una función en uLib.Logger para obtener el nivel actual
      begin
        SetLogLevel(ParsedLogLevel); // TRttiEnumerationType.GetValue<TLogLevel>('log' + LogLevelStr)); // Línea original
        LogMessage('Log level set from configuration: ' + LogLevelStr + ' (Effective: ' + TRttiEnumerationType.GetName<TLogLevel>(ParsedLogLevel) +')', logInfo);
      end;
    end
    else
      LogMessage('Warning: "application" section not found in config for log level. Using default.', logWarning);

    // 3. Inicializar el Gestor de Señales de Proceso
    // Asume que 'ProcessManager' es la instancia global creada en la unit uLib.Process.Manager
    // o que TProcessManager.GetInstance existe y funciona como Singleton.
    // El constructor de TProcessManager (llamado por GetInstance o en su unit) ya llama a SetupSignalHandlers.
    //GProcessManager := TProcessManager.Create(); // Usar GetInstance si es el patrón implementado
    //LogMessage('ProcessManager instance obtained/initialized.', logInfo);

    // 4. Inicializar el Pool de Conexiones de Base de Datos
    LogMessage('Initializing Database Connection Pool Manager...', logInfo);
    GDBPoolManager := TDBConnectionPoolManager.GetInstance;
    // GDBPoolManager.Monitor := LDBMonitorInstance; // Asignar monitor si se usa

    if LAppConfig.TryGetValue('databasePools', LDBPoolConfigsArray) then
    begin
      if LDBPoolConfigsArray.Count > 0 then
      begin
        // Pasar el monitor si se tiene uno: GDBPoolManager.ConfigurePoolsFromJSONArray(LDBPoolConfigsArray, LDBMonitorInstance);
        GDBPoolManager.ConfigurePoolsFromJSONArray(LDBPoolConfigsArray, nil);
        LogMessage(Format('Database Connection Pool Manager configured with %d pool(s).', [LDBPoolConfigsArray.Count]), logInfo);
      end
      else
        LogMessage('No database pool configurations found in "databasePools" array. No pools initialized.', logWarning);
    end
    else
      LogMessage('Key "databasePools" (TJSONArray) not found in application configuration. No database pools will be configured.', logWarning);

    // 5. Inicializar el Gestor del Servidor Web
    LogMessage('Initializing Web ServerManager instance...', logInfo);
    GWebServerManager := TServerManager.GetInstance;
    // TServerManager.GetInstance puede tomar AConfigBaseDirForInit.
    // Si TConfigManager ya está inicializado (como lo está aquí), TServerManager usará esa instancia.
    // TServerManager.EnsureConfigIsLoaded (llamado por StartServer o UpdateConfiguration)
    // obtendrá la configuración de GConfigManager.

    // 6. Registrar Manejadores de Apagado
    // Deben registrarse ANTES de iniciar los servicios principales que necesitan ser apagados.
    GProcessManager.RegisterShutdownHandler(
      procedure
      begin
        LogMessage('Shutdown signal received. Initiating graceful shutdown of services...', logInfo);
        WriteLn('Graceful shutdown initiated...'); // Output para el usuario en consola

        if Assigned(GWebServerManager) then
        begin
          LogMessage('Stopping Web Server...', logInfo);
          GWebServerManager.StopServer; // StopServer es un Boolean, pero aquí solo se llama
          LogMessage('Web Server stopped.', logInfo);
        end
        else
          LogMessage('Web Server Manager not assigned at shutdown.', logWarning);

        if Assigned(GDBPoolManager) then
        begin
          LogMessage('Shutting down Database Connection Pools...', logInfo);
          GDBPoolManager.ShutdownAllPools;
          LogMessage('Database Connection Pools shut down.', logInfo);
        end
        else
          LogMessage('Database Pool Manager not assigned at shutdown.', logWarning);

        LogMessage('All services stopped by shutdown handler.', logInfo);
      end
    );
    LogMessage('Shutdown handlers registered.', logInfo);

    // 7. Iniciar el Servidor Web
    // StartServer en TServerManager debería usar el FConfig interno,
    // que se carga mediante EnsureConfigIsLoaded (que usa TConfigManager).
    // Pasar GAppBasePath a StartServer asegura que TConfigManager (y por ende TServerManager)
    // tenga el path correcto si no se inicializó antes explícitamente con path.
    WriteLn('Attempting to start web server...');
    if GWebServerManager.StartServer(GAppBasePath) then
    begin
      LogMessage('Web Server started successfully and is running.', logInfo);
      WriteLn('Web Server started successfully. Press Ctrl+C or send termination signal to exit.');
      // 8. Esperar señal de apagado (ej. Ctrl+C)
      // WaitForShutdownSignal en TProcessManager maneja la espera y la ejecución de handlers.
      GProcessManager.WaitForShutdownSignal; // Esta función es bloqueante
    end
    else
    begin
      LogMessage('Failed to start the web server. Check previous logs for errors.', logFatal);
      WriteLn('FATAL: Failed to start the web server. Check logs.');
      // Considerar si se debe llamar a los handlers de apagado aquí también para limpiar lo que se haya inicializado.
      // GProcessManager.RequestProgrammaticShutdown; // Podría ser una opción.
    end;

  finally
    // La finalización de Singletons (TConfigManager, TDBConnectionPoolManager, TServerManager, TProcessManager)
    // se maneja en sus respectivas secciones 'finalization' o class destructors.
    // LAppConfig es un clon, se libera aquí.
    FreeAndNil(LAppConfig);
    LogMessage('InitializeAndRunApplication finished execution path.', logDebug);
  end;
end;

begin
  GAppBasePath := IncludeTrailingPathDelimiter(ExtractFilePath(ParamStr(0)));

  try
    ReportMemoryLeaksOnShutdown := True; // Útil para depuración
    WriteLn('Initializing MercadoSaint Server Application...');

    InitializeAndRunApplication;

    LogMessage('Application shutdown sequence complete.', logInfo);
    WriteLn('Application shutdown complete.');
    ExitCode := 0; // Salida exitosa

  except
    on E: Exception do
    begin
      // Si el logger ya está inicializado, usarlo. Sino, consola.
      // FCriticalSection en TLogger es una variable de clase, no de instancia.
      // Se puede verificar si fue asignada (lo que ocurre en TLogger.CreateModule).
      if Assigned(TLogger.FCriticalSection) then
        LogMessage(Format('FATAL UNHANDLED EXCEPTION: %s - %s', [E.ClassName, E.Message]), logFatal)
      else
        WriteLn(Format('FATAL UNHANDLED EXCEPTION (Logger not fully initialized): %s - %s', [E.ClassName, E.Message]));
      ExitCode := 1; // Salida con error
    end;
  end;

  // La finalización de los Singletons (TConfigManager, TDBConnectionPoolManager, TServerManager, TProcessManager, TLogger)
  // se maneja automáticamente por Delphi al descargar las units, a través de sus bloques 'finalization' o class destructors.
  // No es necesario llamar a FinalizeLog explícitamente aquí si TLogger.DestroyModule lo hace.
end.

