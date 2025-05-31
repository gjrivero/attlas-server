program AttlasServer;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.StrUtils,
  System.IOUtils,
  System.JSON,
  System.Rtti,
  System.Classes,
  System.Generics.Collections,
  uLib.Logger,
  uLib.Utils,
  uController.System,
  uController.Customers,
  uGlobalDefinitions in 'uGlobalDefinitions.pas',
  uLib.Database.Types in 'database\uLib.Database.Types.pas',
  uLib.Controller.Base in 'controllers\uLib.Controller.Base.pas',
  uLib.Routes in 'server\uLib.Routes.pas',
  uLib.Server.Base in 'server\uLib.Server.Base.pas',
  uLib.Config.Manager in 'server\uLib.Config.Manager.pas',
  uLib.Process.Manager in 'server\uLib.Process.Manager.pas',
  uLib.Server.Manager in 'server\uLib.Server.Manager.pas',
  uLib.Session.Manager in 'server\uLib.Session.Manager.pas',
  uLib.Database.Connection in 'database\uLib.Database.Connection.pas',
  uLib.Database.Pool in 'database\uLib.Database.Pool.pas',
  uLib.Server.Types in 'server\uLib.Server.Types.pas';

{$R *.res}

var
  GAppBasePath: String; // Ruta base de la aplicación
  GDBPoolManager: TDBConnectionPoolManager;
  GWebServerManager: TServerManager;


procedure ValidateProductionSecurity(const SecurityVars: TDictionary<string,string>);
begin
  if SecurityVars['${JWT_SECRET}'].Length < 32 then
    raise EConfigurationError.Create('JWT_SECRET must be at least 32 characters in production');

  // Validar que password no contenga palabras comunes
  var Password := SecurityVars['${DB_PASSWORD}'].ToLower;
  if ContainsText(Password, 'password') or
     ContainsText(Password, 'admin') or
     ContainsText(Password, 'master') or
     ContainsText(Password, 'key') then
    raise EConfigurationError.Create('DB_PASSWORD contains weak/common terms in production');

  LogMessage('Production security validation passed', logInfo);
end;

procedure SetEnviromentVars(var AConfig: TJSONObject);

  function SetDefaultValues(AVariable, ADefault: String): String;
  var
    aValue: String;
  begin
    aValue:=GetEnvironmentVariable(AVariable);
    If AValue='' then
       AValue:=ADefault;
    Result:=AValue;
  end;

  function SetCriticalValue(AVariable, ADefault: String; AIsProduction: Boolean = False): String;
  var
    EnvValue: String;
  begin
    EnvValue := GetEnvironmentVariable(AVariable);

    if EnvValue.Trim = '' then
    begin
      if AIsProduction then
      begin
        LogMessage(Format('SECURITY ERROR: Critical variable "%s" not set in production environment', [AVariable]), logFatal);
        raise EConfigurationError.CreateFmt('Critical environment variable "%s" must be set in production', [AVariable]);
      end
      else
      begin
        LogMessage(Format('WARNING: Using default value for "%s". This should not happen in production!', [AVariable]), logWarning);
        Result := ADefault;
      end;
    end
    else
    begin
      // Validar que no sea el valor default conocido en producción
      if AIsProduction and SameText(EnvValue, ADefault) then
      begin
        LogMessage(Format('SECURITY ERROR: Variable "%s" is using default/weak value in production', [AVariable]), logFatal);
        raise EConfigurationError.CreateFmt('Variable "%s" cannot use default value in production environment', [AVariable]);
      end;
      Result := EnvValue;
    end;
  end;

var
  NewConfig: TJSONObject;
  aPairs: TDictionary<string,string>;
  key, sJSON: String;
  IsProduction: Boolean;
begin
  aPairs:=TDictionary<string,string>.Create;
  try
    // Detectar si estamos en producción (por variable de entorno o configuración)
    IsProduction := SameText(GetEnvironmentVariable('ENVIRONMENT'), 'PRODUCTION') or
                   SameText(GetEnvironmentVariable('APP_ENV'), 'PROD');

    // Variables normales (pueden tener defaults)
    aPairs.Add('${DB_HOST}',SetDefaultValues('DB_HOST','172.27.37.121'));
    aPairs.Add('${DB_NAME}',SetDefaultValues('DB_NAME','centropago'));
    aPairs.Add('${DB_USER}',SetDefaultValues('DB_USER','gjrivero'));

    // Variables críticas (NO deben usar defaults en producción)
    aPairs.Add('${DB_PASSWORD}', SetCriticalValue('DB_PASSWORD', 'Master_Key.', IsProduction));
    aPairs.Add('${JWT_SECRET}', SetCriticalValue('JWT_SECRET',
         'e73994119adda8c9a1322f39b6730f5ba32f924cedb089cb99cf0a62eaf1a3'+
         'b96126ec9f819c3efa3cc516bafcda43c8f2ccc10c7e8c9bec9e65002cbd22d20e', IsProduction));
    aPairs.Add('${PASSWORD_SALT}', SetCriticalValue('PASSWORD_SALT', 'd7Q2mX9VzR1LpF6K', IsProduction));

    // Validaciones adicionales para producción
    if IsProduction then
    begin
      ValidateProductionSecurity(aPairs);
    end;
    sJSON:=AConfig.ToJSON;
    for key in Apairs.Keys do
      begin
        sJSON := ReplaceText(sJSON, Key, aPairs[Key]);
      end;
    NewConfig := TJSONObject.ParseJSONValue(sJSON) as TJSONObject;
    if not Assigned(NewConfig) then
      raise Exception.Create('Failed to parse processed JSON');

    FreeAndNil(AConfig);
    AConfig := NewConfig;
  finally
    aPairs.free;
  end;
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
    SetEnviromentVars(LAppConfig);
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
          if GWebServerManager.StopServer then
            LogMessage('Web Server stopped successfully.', logInfo)
          else
            LogMessage('Web Server stop returned false.', logWarning);
        end
        else
          LogMessage('Web Server Manager not assigned at shutdown.', logWarning);

        if Assigned(GDBPoolManager) then
        begin
          LogMessage('Shutting down Database Connection Pools...', logInfo);
          try
            GDBPoolManager.ShutdownAllPools;
            LogMessage('Database Connection Pools shut down.', logInfo);
          except
            on E: Exception do
              LogMessage(Format('Error shutting down DB pools: %s', [E.Message]), logError);
          end;
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
  // Inicializar variables globales
  GDBPoolManager := nil;
  GWebServerManager := nil;
  GAppBasePath := IncludeTrailingPathDelimiter(ExtractFilePath(ParamStr(0)));
  try
    ReportMemoryLeaksOnShutdown := True;
    WriteLn('Initializing MercadoSaint Server Application...');
    try
      InitializeAndRunApplication;
      LogMessage('Application shutdown sequence complete.', logInfo);
      WriteLn('Application shutdown complete.');
      ExitCode := 0;
    except
      on E: EConfigurationError do
      begin
        WriteLn(Format('CONFIGURATION ERROR: %s', [E.Message]));
        try
          LogMessage(Format('Configuration error during startup: %s', [E.Message]), logFatal);
        except
          // Logger puede no estar disponible
        end;
        ExitCode := 2; // Exit code específico para errores de configuración
      end;
      on E: EServerStartError do
      begin
        WriteLn(Format('SERVER START ERROR: %s', [E.Message]));
        try
          LogMessage(Format('Server start error: %s', [E.Message]), logFatal);
        except
          // Logger puede no estar disponible
        end;
        ExitCode := 3; // Exit code específico para errores de servidor
      end;
      on E: Exception do
      begin
        WriteLn(Format('FATAL UNHANDLED EXCEPTION: %s - %s', [E.ClassName, E.Message]));
        try
          LogMessage(Format('FATAL UNHANDLED EXCEPTION: %s - %s', [E.ClassName, E.Message]), logFatal);
        except
          // Logger puede no estar disponible
        end;
        ExitCode := 1; // Error general
      end;
    end;

  finally
    // Cleanup de emergencia si algo falló
    try
      if Assigned(GWebServerManager) then
      begin
        try
          GWebServerManager.StopServer;
        except
          // Silenciar errores en cleanup
        end;
      end;

      if Assigned(GDBPoolManager) then
      begin
        try
          GDBPoolManager.ShutdownAllPools;
        except
          // Silenciar errores en cleanup
        end;
      end;
    except
      // Silenciar cualquier error de cleanup
    end;
  end;
end.

