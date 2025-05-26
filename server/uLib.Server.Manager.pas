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

    var FServer: TPlatformSpecificServer;
    FInternalLock: TCriticalSection;
    FConfig: TJSONObject; // Almacena un CLON de la configuración global obtenida de TConfigManager

    class constructor CreateClassLock; // Renombrado
    class destructor DestroyClassLock;  // Renombrado

    constructor CreateInternal;
    procedure InitializeServerInstance(AAppConfig: TJSONObject); // Ahora toma el config a usar
    procedure EnsureConfigIsLoaded(const AConfigBaseDirForInit: string = ''); // Helper para cargar FConfig desde TConfigManager
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

    property ServerInstance: TPlatformSpecificServer read FServer;
    property CurrentAppConfig: TJSONObject read FConfig; // Acceso al FConfig actual (es un clon)
  end;

implementation

uses
  System.IOUtils,
  System.Rtti,
  System.DateUtils,
  uLib.Server.Base,

  Ulib.Utils;

{ TServerManager }

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
  // Si se proporciona un path base y FConfig aún no está cargado, o si el path es diferente,
  // se podría inicializar/recargar la configuración aquí.
  // Sin embargo, es mejor que el programa principal llame a TConfigManager.Initialize primero,
  // y luego TServerManager obtenga la configuración.
  // Por ahora, GetInstance no fuerza la carga de FConfig. Eso se hará en StartServer o explícitamente.
  if AConfigBaseDirForInit.Trim <> '' then
     FInstance.EnsureConfigIsLoaded(AConfigBaseDirForInit); // Asegura que ConfigManager esté inicializado

  Result := FInstance;
end;

// Carga FConfig desde TConfigManager si aún no está cargado o si se quiere forzar una recarga.
procedure TServerManager.EnsureConfigIsLoaded(const AConfigBaseDirForInit: string = '');
begin
  FInternalLock.Acquire;
  try
    // Inicializar TConfigManager con el path si se proporciona y aún no tiene uno.
    // Esto también cargará el archivo config.json en TConfigManager.
    if AConfigBaseDirForInit.Trim <> '' then
      TConfigManager.GetInstance(AConfigBaseDirForInit); // Esto inicializará ConfigManager con el path si es necesario

    // Obtener (o recargar) FConfig desde TConfigManager
    var ConfigMgr := TConfigManager.GetInstance; // Obtener instancia (ya debería estar inicializada con path si AConfigBaseDirForInit se usó)
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
      // raise EConfigurationError.Create('Failed to load application configuration via TConfigManager.');
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

// Ya no es necesario, TConfigManager se encarga de cargar desde archivo.
// procedure TServerManager.LoadConfiguration(const AConfigFilePath: string);

procedure TServerManager.UpdateConfiguration(const ANewAppConfigJSON: TJSONObject);
begin
  FInternalLock.Acquire;
  try
    LogMessage('TServerManager: Updating server configuration with provided TJSONObject...', logInfo);
    if not Assigned(ANewAppConfigJSON) then
    begin
      LogMessage('TServerManager.UpdateConfiguration: Provided JSON configuration is nil. Configuration not updated.', logError);
      raise EConfigurationError.Create('Cannot update configuration with a nil JSON object.');
    end;

    FreeAndNil(FConfig); // Liberar el FConfig anterior
    FConfig := ANewAppConfigJSON.Clone as TJSONObject; // Clonar para tomar posesión
    LogMessage('TServerManager: FConfig updated. Server instance will be re-initialized.', logInfo);

    InitializeServerInstance(FConfig); // Recrear el servidor con la nueva configuración
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

