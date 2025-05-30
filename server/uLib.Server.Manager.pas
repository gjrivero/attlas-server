unit uLib.Server.Manager;

interface

uses
  System.SysUtils, System.Classes, System.JSON, System.SyncObjs,
  System.Generics.Collections,
  uLib.Server.Types,
  uLib.Logger,
  uLib.Config.Manager // Para obtener la configuración global
  {$IFDEF MSWINDOWS}
  , uLib.Win.Server
  {$ELSE}
  , uLib.Linux.Server
  {$ENDIF};

type
  {$IFDEF MSWINDOWS}
  TPlatformSpecificServer = class(TWindowsWebServer);
  {$ELSE}
  TPlatformSpecificServer = class(TLinuxWebServer);
  {$ENDIF}

  TServerManager = class
  private
    class var FInstance: TServerManager;
    class var FSingletonLock: TCriticalSection;

    FServer: TPlatformSpecificServer;
    FInternalLock: TCriticalSection;
    FConfig: TJSONObject;

    class constructor CreateClassLock; // Renombrado
    class destructor DestroyClassLock;  // Renombrado

    constructor CreateInternal;
    function GetServerInstance: TPlatformSpecificServer;
    function GetConfigInstance: TJSONObject;
    procedure InitializeServerInstance(AAppConfig: TJSONObject); // Ahora toma el config a usar
    procedure EnsureConfigIsLoaded(const AConfigBaseDirForInit: string = ''); // Helper para cargar FConfig desde TConfigManager
    procedure ValidateSSLConfigurationAtStartup;
  public
    destructor Destroy; override;
    class function GetInstance(const AConfigBaseDirForInit: string = ''): TServerManager; // Permite inicializar ConfigManager

    function StartServer(const AConfigBaseDirForInit: string = ''): Boolean; // Permite inicializar ConfigManager si es necesario
    function StopServer: Boolean;
    function RestartServer(const AConfigBaseDirForInit: string = ''): Boolean;

    // UpdateConfiguration: Actualiza FConfig con un nuevo TJSONObject y reinicializa el servidor.
    // La recarga desde archivo es manejada por TConfigManager.ReloadConfiguration,
    // luego se puede llamar a UpdateConfiguration con TConfigManager.GetGlobalConfigClone.
    procedure UpdateConfiguration(const ANewAppConfigJSON: TJSONObject);
    function GetConfigurationClone: TJSONObject; // Renombrado

    function GetServerStatus: TServerStats;
    function GetActiveConnectionsInfo: TArray<TConnectionInfo>;

    property ServerInstance: TPlatformSpecificServer read GetServerInstance;
    property CurrentAppConfig: TJSONObject read GetConfigInstance;
  end;

implementation

uses
  System.IOUtils,
  System.Rtti,
  System.DateUtils,
  uLib.Server.Base,

  Ulib.Utils;

{ TServerManager }

function TServerManager.GetServerInstance: TPlatformSpecificServer;
begin
  FInternalLock.Acquire;
  try
    Result := FServer;
  finally
    FInternalLock.Release;
  end;
end;

function TServerManager.GetConfigInstance: TJSONObject;
begin
  FInternalLock.Acquire;
  try
    Result := FConfig;
  finally
    FInternalLock.Release;
  end;
end;

class constructor TServerManager.CreateClassLock;
begin
  if not Assigned(FSingletonLock) then
    FSingletonLock := TCriticalSection.Create;
end;

class destructor TServerManager.DestroyClassLock;
begin
  FreeAndNil(FSingletonLock);
end;

constructor TServerManager.CreateInternal;
begin
  inherited Create;
  FInternalLock := TCriticalSection.Create;
  FConfig := nil; // Se cargará mediante EnsureConfigIsLoaded o UpdateConfiguration
  FServer := nil;
  LogMessage('TServerManager instance (CreateInternal) created.', logInfo);
end;

destructor TServerManager.Destroy;
begin
  LogMessage('TServerManager instance destroying...', logDebug);
  if Assigned(FServer) then
  begin
    try
      if FServer.IsRunning then
        FServer.Stop;
    except
      on E: Exception do
        LogMessage(Format('Error stopping server during TServerManager.Destroy: %s - %s', [E.ClassName, E.Message]), logError);
    end;
    FreeAndNil(FServer);
  end;
  FreeAndNil(FConfig);
  FreeAndNil(FInternalLock);
  LogMessage('TServerManager instance destroyed.', logInfo);
  inherited;
end;

class function TServerManager.GetInstance(const AConfigBaseDirForInit: string = ''): TServerManager;
begin
  if not Assigned(FInstance) then
  begin
    if not Assigned(FSingletonLock) then
    begin
      LogMessage('CRITICAL: TServerManager.FSingletonLock is nil in GetInstance!', logFatal);
      FSingletonLock := TCriticalSection.Create;
      if not Assigned(FSingletonLock) then
        raise Exception.Create('TServerManager SingletonLock could not be initialized.');
    end;
    FSingletonLock.Acquire;
    try
      if not Assigned(FInstance) then
        FInstance := TServerManager.CreateInternal;
    finally
      FSingletonLock.Release;
    end;
  end;
  if AConfigBaseDirForInit.Trim <> '' then
  begin
    FSingletonLock.Acquire;
    try
      FInstance.EnsureConfigIsLoaded(AConfigBaseDirForInit);
    finally
      FSingletonLock.Release;
    end;
  end;
  Result := FInstance;
end;

procedure TServerManager.EnsureConfigIsLoaded(const AConfigBaseDirForInit: string = '');
begin
  FInternalLock.Acquire;
  try
    if AConfigBaseDirForInit.Trim <> '' then
      TConfigManager.GetInstance(AConfigBaseDirForInit);
    var ConfigMgr := TConfigManager.GetInstance;
    if (ConfigMgr.ConfigFilePath.Trim = '') and (AConfigBaseDirForInit.Trim = '') then
    begin
        LogMessage('TServerManager.EnsureConfigIsLoaded: ConfigManager path not set and no AConfigBaseDirForInit provided. Cannot load configuration.', logWarning);
        if not Assigned(FConfig) then FConfig := TJSONObject.Create; // Asegurar que FConfig no sea nil
        Exit;
    end;

    FreeAndNil(FConfig); // Liberar el FConfig anterior
    FConfig := ConfigMgr.GetGlobalConfigClone; // Obtener un clon de la configuración global actual
    if (not Assigned(FConfig)) or (FConfig.Count = 0) then
    begin
      LogMessage('TServerManager.EnsureConfigIsLoaded: Failed to load global configuration from TConfigManager or configuration is empty.', logError);
      if not Assigned(FConfig) then FConfig := TJSONObject.Create; // Asegurar que FConfig no sea nil
    end
    else
      LogMessage('TServerManager: FConfig loaded/updated from TConfigManager.', logDebug);
  finally
    FInternalLock.Release;
  end;
end;

procedure TServerManager.InitializeServerInstance(AAppConfig: TJSONObject);
begin
  // Este método asume que AAppConfig es un TJSONObject válido.
  // Debe ser llamado mientras FInternalLock está adquirido.
  if Assigned(FServer) then
  begin
    LogMessage('TServerManager: Server instance already exists. Stopping and recreating...', logWarning);
    if FServer.IsRunning then
      FServer.Stop;
    FreeAndNil(FServer);
  end;

  LogMessage('TServerManager: Initializing platform-specific server instance...', logInfo);
  if (not Assigned(AAppConfig)) or (AAppConfig.Count = 0) then
  begin
    LogMessage('TServerManager.InitializeServerInstance: Provided AAppConfig is nil or empty. Cannot initialize server.', logError);
    raise EConfigurationError.Create('Application configuration is missing or empty for server initialization.');
  end;

  try
    // TPlatformSpecificServer (TLinuxWebServer/TWindowsWebServer) debe tener Create(AAppConfig: TJSONObject)
    // que llame a inherited Create(AAppConfig) de TServerBase.
    FServer := TPlatformSpecificServer.Create(AAppConfig);
    LogMessage(Format('TServerManager: %s instance created.', [FServer.ClassName]), logInfo);
  except
    on E: Exception do
    begin
      LogMessage(Format('TServerManager: Failed to create platform-specific server instance: %s - %s', [E.ClassName, E.Message]), logFatal);
      FServer := nil;
      raise;
    end;
  end;
  if not Assigned(FServer) then // Seguridad adicional
    raise EServerStartError.Create('TServerManager: Failed to create platform-specific server instance (FServer is nil).');
end;


procedure TServerManager.UpdateConfiguration(const ANewAppConfigJSON: TJSONObject);
var
  NewConfig: TJSONObject;
begin
  FInternalLock.Acquire;
  try
    LogMessage('TServerManager: Updating server configuration with provided TJSONObject...', logInfo);
    if not Assigned(ANewAppConfigJSON) then
    begin
      LogMessage('TServerManager.UpdateConfiguration: Provided JSON configuration is nil. Configuration not updated.', logError);
      raise EConfigurationError.Create('Cannot update configuration with a nil JSON object.');
    end;

    // Primero clonar y validar la nueva configuración
    NewConfig := ANewAppConfigJSON.Clone as TJSONObject;
    try
      // Intentar inicializar el servidor con la nueva configuración
      // ANTES de liberar la configuración actual
      InitializeServerInstance(NewConfig);

      // Solo si InitializeServerInstance fue exitoso, reemplazar FConfig
      FreeAndNil(FConfig);
      FConfig := NewConfig;
      NewConfig := nil; // Transferir ownership

      LogMessage('TServerManager: Configuration updated successfully and server re-initialized.', logInfo);
    except
      on E: Exception do
      begin
        FreeAndNil(NewConfig); // Limpiar si algo falló
        LogMessage(Format('TServerManager: CRITICAL ERROR during configuration update: %s - %s. ' +
          'Previous configuration maintained.',
          [E.ClassName, E.Message]), logFatal);
        raise;
      end;
    end;
  finally
    FInternalLock.Release;
  end;
end;

function TServerManager.GetConfigurationClone: TJSONObject;
begin
  FInternalLock.Acquire;
  try
    EnsureConfigIsLoaded; // Asegurar que FConfig esté cargado (podría ser desde un path default si no se inicializó antes)
    if Assigned(FConfig) then
      Result := FConfig.Clone as TJSONObject
    else
      Result := TJSONObject.Create; // Devolver objeto vacío si FConfig sigue siendo nil
  finally
    FInternalLock.Release;
  end;
end;

procedure TServerManager.ValidateSSLConfigurationAtStartup;
var
  SSLConfig: TJSONObject;
  SSLEnabled: Boolean;
  CertFile, KeyFile: string;
  IsProduction: Boolean;
begin
  try
    if not FConfig.TryGetValue('server', SSLConfig) then Exit;
    if not SSLConfig.TryGetValue('ssl', SSLConfig) then Exit;

    SSLEnabled := TJSONHelper.GetBoolean(SSLConfig, 'enabled', False);
    if not SSLEnabled then
    begin
      LogMessage('SSL is disabled in configuration', logDebug);
      Exit;
    end;

    IsProduction := SameText(GetEnvironmentVariable('ENVIRONMENT'), 'PRODUCTION') or
                   SameText(GetEnvironmentVariable('APP_ENV'), 'PROD');

    CertFile := TJSONHelper.GetString(SSLConfig, 'certFile', '');
    KeyFile := TJSONHelper.GetString(SSLConfig, 'keyFile', '');

    LogMessage('Validating SSL configuration at startup...', logInfo);

    if CertFile.Trim.IsEmpty or KeyFile.Trim.IsEmpty then
    begin
      var ErrorMsg := 'SSL is enabled but certificate or key file paths are empty';
      if IsProduction then
      begin
        LogMessage('CRITICAL: ' + ErrorMsg + ' in production environment', logFatal);
        raise EConfigurationError.Create(ErrorMsg + ' in production environment');
      end
      else
      begin
        LogMessage('WARNING: ' + ErrorMsg + ' in development environment. SSL will be disabled.', logWarning);
        Exit;
      end;
    end;

    // Resolver paths relativos si es necesario
    var BasePath := TJSONHelper.GetString(FConfig, 'server.basePath', '');
    if BasePath.Trim <> '' then
    begin
      if TPath.IsRelativePath(CertFile) then
        CertFile := TPath.Combine(BasePath, CertFile);
      if TPath.IsRelativePath(KeyFile) then
        KeyFile := TPath.Combine(BasePath, KeyFile);
    end;

    // Verificar existencia de archivos
    if not TFile.Exists(CertFile) then
    begin
      var ErrorMsg := Format('SSL certificate file not found: %s', [CertFile]);
      if IsProduction then
      begin
        LogMessage('CRITICAL: ' + ErrorMsg, logFatal);
        raise EConfigurationError.Create(ErrorMsg);
      end
      else
      begin
        LogMessage('WARNING: ' + ErrorMsg + '. SSL will be disabled in development.', logWarning);
        Exit;
      end;
    end;

    if not TFile.Exists(KeyFile) then
    begin
      var ErrorMsg := Format('SSL key file not found: %s', [KeyFile]);
      if IsProduction then
      begin
        LogMessage('CRITICAL: ' + ErrorMsg, logFatal);
        raise EConfigurationError.Create(ErrorMsg);
      end
      else
      begin
        LogMessage('WARNING: ' + ErrorMsg + '. SSL will be disabled in development.', logWarning);
        Exit;
      end;
    end;

    // Verificar permisos básicos
    try
      var TestStream := TFileStream.Create(CertFile, fmOpenRead or fmShareDenyNone);
      try
        if TestStream.Size = 0 then
          raise EConfigurationError.CreateFmt('SSL certificate file is empty: %s', [CertFile]);
      finally
        TestStream.Free;
      end;

      TestStream := TFileStream.Create(KeyFile, fmOpenRead or fmShareDenyNone);
      try
        if TestStream.Size = 0 then
          raise EConfigurationError.CreateFmt('SSL key file is empty: %s', [KeyFile]);
      finally
        TestStream.Free;
      end;

      LogMessage(Format('SSL configuration validation passed. Cert: %s, Key: %s', [CertFile, KeyFile]), logInfo);

    except
      on E: Exception do
      begin
        var ErrorMsg := Format('SSL file validation failed: %s', [E.Message]);
        if IsProduction then
        begin
          LogMessage('CRITICAL: ' + ErrorMsg, logFatal);
          raise EConfigurationError.Create(ErrorMsg);
        end
        else
        begin
          LogMessage('WARNING: ' + ErrorMsg + '. SSL may not work properly.', logWarning);
        end;
      end;
    end;

  except
    on E: EConfigurationError do
      raise; // Re-lanzar errores de configuración
    on E: Exception do
    begin
      LogMessage(Format('Unexpected error during SSL validation: %s', [E.Message]), logError);
      if IsProduction then
        raise EConfigurationError.CreateFmt('SSL validation failed: %s', [E.Message]);
    end;
  end;
end;

function TServerManager.StartServer(const AConfigBaseDirForInit: string = ''): Boolean;
begin
  Result := False;
  FInternalLock.Acquire;
  try
    EnsureConfigIsLoaded(AConfigBaseDirForInit); // Carga/Recarga FConfig desde TConfigManager

    if (not Assigned(FConfig)) or (FConfig.Count = 0) then
    begin
      LogMessage('TServerManager.StartServer: Cannot start server. Configuration (FConfig) is not loaded or empty.', logError);
      Exit;
    end;

    ValidateSSLConfigurationAtStartup;

    if not Assigned(FServer) then // Si el servidor no existe (ej. primera vez o después de un Stop y Free)
    begin
      LogMessage('TServerManager.StartServer: Server instance not initialized. Initializing now...', logInfo);
      InitializeServerInstance(FConfig); // Crear usando el FConfig actual
    end;

    if Assigned(FServer) then
    begin
      if not FServer.IsRunning then
      begin
        LogMessage('TServerManager: Attempting to start server instance...', logInfo);
        try
          FServer.Start;
          Result := FServer.IsRunning;
          if Result then
            LogMessage('Server started successfully by TServerManager.', logInfo)
          else
            LogMessage('TServerManager: Server Start method called, but IsRunning is false.', logError);
        except
          on E: Exception do
          begin
            LogMessage(Format('TServerManager: Exception during server start: %s - %s', [E.ClassName, E.Message]), logError);
            Result := False;
          end;
        end;
      end
      else
      begin
        LogMessage('TServerManager.StartServer: Server is already running.', logInfo);
        Result := True;
      end;
    end
    else
       LogMessage('TServerManager.StartServer: Cannot start. FServer instance is not available after initialization attempt.', logError);
  finally
    FInternalLock.Release;
  end;
end;

function TServerManager.StopServer: Boolean;
begin
  Result := False;
  FInternalLock.Acquire;
  try
    if Assigned(FServer) then
    begin
      if FServer.IsRunning then
      begin
        LogMessage('TServerManager: Attempting to stop server instance...', logInfo);
        try
          FServer.Stop;
          Result := not FServer.IsRunning;
          if Result then
            LogMessage('Server stopped successfully by TServerManager.', logInfo)
          else
            LogMessage('TServerManager: Server Stop method called, but IsRunning is true.', logError);
        except
          on E: Exception do
          begin
            LogMessage(Format('TServerManager: Exception during server stop: %s - %s', [E.ClassName, E.Message]), logError);
            Result := False;
          end;
        end;
      end
      else
      begin
        LogMessage('TServerManager.StopServer: Server is not running.', logInfo);
        Result := True;
      end;
      // Considerar si se debe liberar FServer aquí o dejarlo para el destructor/reinicio.
      // Si se libera, StartServer necesitará llamar a InitializeServerInstance siempre.
      // Por ahora, StopServer no libera FServer. InitializeServerInstance lo hace si ya existe.
    end
    else
    begin
      LogMessage('TServerManager.StopServer: Server instance (FServer) is not available.', logWarning);
      Result := True; // No hay servidor que detener
    end;
  finally
    FInternalLock.Release;
  end;
end;

function TServerManager.RestartServer(const AConfigBaseDirForInit: string = ''): Boolean;
begin
  FInternalLock.Acquire;
  try
    LogMessage('TServerManager: Attempting to restart server...', logInfo);
    if not StopServer then
    begin
      LogMessage('TServerManager.RestartServer: Failed to stop the server. Restart aborted.', logError);
      Result := False;
      Exit;
    end;
    // EnsureConfigIsLoaded y InitializeServerInstance se llamarán dentro de StartServer
    Result := StartServer(AConfigBaseDirForInit);
    if Result then
      LogMessage('Server restarted successfully by TServerManager.', logInfo)
    else
      LogMessage('TServerManager.RestartServer: Failed to start the server after stopping.', logError);
  finally
    FInternalLock.Release;
  end;
end;

function TServerManager.GetServerStatus: TServerStats;
var
  ServerJSONStats: TJSONObject;
  LStateEnum: TServerState;
  StateStr: string;
  LStartupTime: TDateTime;
begin
  FInternalLock.Acquire;
  try
    FillChar(Result, SizeOf(TServerStats), 0);
    Result.State := ssStopped;

    EnsureConfigIsLoaded; // Asegurar que FConfig esté disponible para referencias de TServerBase si es necesario

    if Assigned(FServer) then
    begin
      ServerJSONStats := FServer.GetServerStats; // De TServerBase, devuelve TJSONObject
      try
        if Assigned(ServerJSONStats) then
        begin
          StateStr := TJSONHelper.GetString(ServerJSONStats, 'state', '');
          if StateStr <> '' then
          try
            LStateEnum := TRttiEnumerationType.GetValue<TServerState>(StateStr);
            Result.State := LStateEnum;
          except
            on E: Exception do
            begin
              LogMessage(Format('GetServerStatus: Error converting state string "%s" to TServerState enum: %s', [StateStr, E.Message]), logWarning);
              Result.State := ssError;
            end;
          end
          else
            Result.State := ssUnknown;

          // TServerBase.GetServerStats devuelve 'startup_time_utc'
          var StartupTimeStr := TJSONHelper.GetString(ServerJSONStats, 'startup_time_utc', '');
          if TryStrToDateTime(StartupTimeStr, LStartupTime) then // Usar Try
             Result.StartupTimeUTC := LStartupTime
          else
            if StartupTimeStr <> '' then // Log si el parseo falló pero había un string
               LogMessage(Format('GetServerStatus: Could not parse startup_time_utc "%s" from server stats.',[StartupTimeStr]),logWarning);


          Result.ActiveConnections := TJSONHelper.GetInteger(ServerJSONStats, 'active_connections');
          Result.TotalRequests := TJSONHelper.GetInt64(ServerJSONStats, 'total_requests');
          Result.FailedRequests := TJSONHelper.GetInt64(ServerJSONStats, 'failed_requests');
          // Placeholders, ya que TServerBase.GetServerStats los tiene como placeholders
          Result.BytesSent := TJSONHelper.GetInt64(ServerJSONStats, 'bytes_sent', 0);
          Result.BytesReceived := TJSONHelper.GetInt64(ServerJSONStats, 'bytes_received', 0);
          Result.AverageResponseTimeMs := TJSONHelper.GetDouble(ServerJSONStats, 'avg_response_time_ms', 0.0); // Asumiendo GetFloat de uLib.Base
        end
        else
          LogMessage('GetServerStatus: FServer.GetServerStats returned nil.', logWarning);
      finally
        FreeAndNil(ServerJSONStats);
      end;
    end
    else
      LogMessage('GetServerStatus: FServer instance is nil. Returning default stopped state.', logDebug);
  finally
    FInternalLock.Release;
  end;
end;

function TServerManager.GetActiveConnectionsInfo: TArray<TConnectionInfo>;
begin
  FInternalLock.Acquire;
  try
    if Assigned(FServer) then
    begin
      LogMessage('GetActiveConnectionsInfo: Functionality is a placeholder and depends on TServerBase/TPlatformSpecificServer implementation.', logDebug);
      SetLength(Result, 0);
    end
    else
    begin
      SetLength(Result, 0);
      LogMessage('Cannot get active connections: Server instance not available.', logWarning);
    end;
  finally
    FInternalLock.Release;
  end;
end;

initialization
  TServerManager.FInstance := nil;
finalization
  if Assigned(TServerManager.FInstance) then
    FreeAndNil(TServerManager.FInstance);
end.

