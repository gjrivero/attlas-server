unit uLib.Server.Base;

interface

uses
  System.SysUtils, System.Classes, System.SyncObjs,
  System.JSON, System.Rtti, System.Diagnostics,

  IdHTTPHeaderInfo,  IdHTTPServer, IdContext, IdCustomHTTPServer,
  IdGlobal,  IdHTTPWebBrokerBridge, IdSSLOpenSSL,

  uLib.Logger,
  uLib.Routes,
  uLib.Server.Types,
  uLib.Controller.Base,
  uLib.Middleware.Auth,
  uLib.Middleware.Security,
  uLib.Middleware.CORS;

type
  TServerBase = class abstract
  private
    FAppConfig: TJSONObject;
    FHTTPServerConfig: TServerHTTPConfig;

    FServerState: TServerState;
    FServerInstance: TIdHTTPServer;
    FSSLIOHandler: TIdServerIOHandlerSSLOpenSSL;
    FLock: TCriticalSection;

    FRouteManager: TRouteManager;
    FAuthMiddleware: TAuthMiddleware;
    FSecurityMiddleware: TSecurityMiddleware;
    FCORSMiddleware: TCORSMiddleware;

    FStartUpTimeUTC: TDateTime;
    FActiveConnections: Integer;
    FTotalRequests: Int64;
    FFailedRequests: Integer;

    procedure SetServerState(AValue: TServerState);
    procedure IndyServerOnExceptionHandler(AContext: TIdContext; AException: Exception);
    function IsConnectionSecure(AContext: TIdContext): Boolean;
    procedure ValidateSSLFiles(const CertPath, KeyPath, RootCertPath: string; IsProduction: Boolean);
    procedure ValidateSSLConfiguration;
  protected
    procedure InternalRefreshAppConfig(ANewAppConfig: TJSONObject);
    property ServerState: TServerState read FServerState write SetServerState;

    procedure ConfigureSSLFromConfig;
    procedure ApplyIndyBaseSettings;
    procedure LoadAndPopulateHTTPConfig;
    procedure InitializeFrameworkComponents; virtual;
    procedure ConfigurePlatformServerInstance; virtual; abstract;
    procedure CleanupServerResources; virtual;
    procedure PerformShutdownTasks; virtual;

    function GetServerStateThreadSafe: TServerState;

    procedure DoIndyConnect(AContext: TIdContext); virtual;
    procedure DoIndyDisconnect(AContext: TIdContext); virtual;
    procedure DoIndyRequest(AContext: TIdContext; ARequest: TIdHTTPRequestInfo; AResponse: TIdHTTPResponseInfo); virtual;
    procedure DoIndyErrorInRequest(AContext: TIdContext; ARequest: TIdHTTPRequestInfo; AResponse: TIdHTTPResponseInfo; AException: Exception); virtual;
  public
    constructor Create(AAppConfig: TJSONObject); virtual;
    destructor Destroy; override;

    procedure Start; virtual; abstract;
    procedure Stop; virtual; abstract;
    function IsRunning: Boolean;

    function GetServerStats: TJSONObject;

    property HTTPServerInstance: TIdHTTPServer read FServerInstance;
    property ApplicationConfig: TJSONObject read FAppConfig;
    property Router: TRouteManager read FRouteManager;
    property HTTPConfig: TServerHTTPConfig read FHTTPServerConfig;
  end;

implementation

uses
  System.DateUtils, // Para DateTimeToISO8601, NowUTC, etc.
  System.IOUtils,   // For TPath
  System.StrUtils,
  uLib.Utils;

constructor TServerBase.Create(AAppConfig: TJSONObject);
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FServerState := ssStopped;
  FActiveConnections := 0;
  FTotalRequests := 0;
  FFailedRequests := 0;

  if not Assigned(AAppConfig) then
    raise EConfigurationError.Create('Application configuration (AAppConfig) cannot be nil for TServerBase.Create.');
  FAppConfig := AAppConfig.Clone as TJSONObject;

  LogMessage('TServerBase creating instance...', logInfo);
  try
    FRouteManager := TRouteManager.Create;

    FServerInstance := TIdHTTPServer.Create(nil);
    LoadAndPopulateHTTPConfig;
    ApplyIndyBaseSettings;

    if FHTTPServerConfig.SSLEnabled then
      ConfigureSSLFromConfig;

    InitializeFrameworkComponents;

    FServerInstance.OnConnect := DoIndyConnect;
    FServerInstance.OnDisconnect := DoIndyDisconnect;
    FServerInstance.OnCommandGet := DoIndyRequest;
    FServerInstance.OnCommandOther := DoIndyRequest;
    FServerInstance.OnException := IndyServerOnExceptionHandler;

    LogMessage('TServerBase instance created. Platform-specific configuration (port, etc.) to be done by descendant.', logInfo);
  except
    on E: Exception do
    begin
      LogMessage(Format('FATAL Error initializing TServerBase: %s - %s', [E.ClassName, E.Message]), logFatal);
      FreeAndNil(FRouteManager);
      FreeAndNil(FServerInstance);
      FreeAndNil(FSSLIOHandler);
      FreeAndNil(FAppConfig);
      FreeAndNil(FLock);
      raise;
    end;
  end;
end;

destructor TServerBase.Destroy;
begin
  LogMessage('TServerBase destroying instance...', logInfo);
  CleanupServerResources;
  FreeAndNil(FRouteManager);
  FreeAndNil(FServerInstance);
  FreeAndNil(FSSLIOHandler);
  FreeAndNil(FAppConfig);
  FreeAndNil(FLock);
  LogMessage('TServerBase instance destroyed.', logInfo);
  inherited;
end;

procedure TServerBase.InternalRefreshAppConfig(ANewAppConfig: TJSONObject);
begin
  FLock.Acquire; // Proteger el acceso a FAppConfig
  try
    LogMessage('TServerBase: Refreshing internal FAppConfig with new configuration.', logDebug);
    FreeAndNil(FAppConfig); // Liberar el clon anterior
    if Assigned(ANewAppConfig) then
      FAppConfig := ANewAppConfig.Clone as TJSONObject // Clonar la nueva configuración
    else
    begin
      FAppConfig := TJSONObject.Create; // Crear uno vacío si se pasa nil
      LogMessage('TServerBase.InternalRefreshAppConfig: ANewAppConfig was nil. FAppConfig set to empty JSON object.', logWarning);
    end;
  finally
    FLock.Release;
  end;
end;

procedure TServerBase.LoadAndPopulateHTTPConfig;
var
  ServerConfigJSON: TJSONObject;
  SSLConfigJSON: TJSONObject;
begin
  LogMessage('TServerBase: Loading HTTP Server specific settings from AppConfig...', logDebug);
  FHTTPServerConfig := CreateDefaultServerHTTPConfig;

  if FAppConfig.TryGetValue('server', ServerConfigJSON) and Assigned(ServerConfigJSON) then
  begin
    FHTTPServerConfig.Port            := TJSONHelper.GetInteger(ServerConfigJSON, 'port', FHTTPServerConfig.Port);
    FHTTPServerConfig.MaxConnections  := TJSONHelper.GetInteger(ServerConfigJSON, 'maxConnections', FHTTPServerConfig.MaxConnections);
    FHTTPServerConfig.ThreadPoolSize  := TJSONHelper.GetInteger(ServerConfigJSON, 'threadPoolSize', FHTTPServerConfig.ThreadPoolSize);
    FHTTPServerConfig.ConnectionTimeout := TJSONHelper.GetInteger(ServerConfigJSON, 'connectionTimeoutMs', FHTTPServerConfig.ConnectionTimeout);
    FHTTPServerConfig.KeepAliveEnabled  := TJSONHelper.GetBoolean(ServerConfigJSON, 'keepAlive', FHTTPServerConfig.KeepAliveEnabled);
    FHTTPServerConfig.ServerName      := TJSONHelper.GetString(ServerConfigJSON, 'serverName', FHTTPServerConfig.ServerName);
    FHTTPServerConfig.BasePath        := TJSONHelper.GetString(ServerConfigJSON, 'basePath', FHTTPServerConfig.BasePath);
    FHTTPServerConfig.PIDFile         := TJSONHelper.GetString(ServerConfigJSON, 'pidFile', FHTTPServerConfig.PIDFile);
    FHTTPServerConfig.Daemonize       := TJSONHelper.GetBoolean(ServerConfigJSON, 'daemonize', FHTTPServerConfig.Daemonize);
    FHTTPServerConfig.ShutdownGracePeriodSeconds := TJSONHelper.GetInteger(ServerConfigJSON, 'shutdownGracePeriodSeconds', FHTTPServerConfig.ShutdownGracePeriodSeconds);

    if ServerConfigJSON.TryGetValue('ssl', SSLConfigJSON) and Assigned(SSLConfigJSON) then
    begin
      FHTTPServerConfig.SSLEnabled     := TJSONHelper.GetBoolean(SSLConfigJSON, 'enabled', FHTTPServerConfig.SSLEnabled);
      FHTTPServerConfig.SSLCertFile    := TJSONHelper.GetString(SSLConfigJSON, 'certFile', FHTTPServerConfig.SSLCertFile);
      FHTTPServerConfig.SSLKeyFile     := TJSONHelper.GetString(SSLConfigJSON, 'keyFile', FHTTPServerConfig.SSLKeyFile);
      FHTTPServerConfig.SSLRootCertFile := TJSONHelper.GetString(SSLConfigJSON, 'rootCertFile', FHTTPServerConfig.SSLRootCertFile);
    end;
    LogMessage('HTTP Server specific settings (FHTTPServerConfig) populated from "server" section of AppConfig.', logDebug);
  end
  else
    LogMessage('HTTP Server configuration section ("server") not found in AppConfig. Using defaults for FHTTPServerConfig.', logWarning);
end;

procedure TServerBase.ApplyIndyBaseSettings;
begin
  if not Assigned(FServerInstance) then Exit;

  FServerInstance.AutoStartSession := False;
  FServerInstance.ParseParams := True;
  FServerInstance.KeepAlive := FHTTPServerConfig.KeepAliveEnabled;
  FServerInstance.ServerSoftware := FHTTPServerConfig.ServerName;
  if FHTTPServerConfig.MaxConnections > 0 then
    FServerInstance.MaxConnections := FHTTPServerConfig.MaxConnections;
  if FHTTPServerConfig.ConnectionTimeout > 0 then // ConnectionTimeout debe ser el idle timeout en ms
     FServerInstance.TerminateWaitTime := FHTTPServerConfig.ConnectionTimeout;
  // Connection idle timeout is typically handled by IOHandler.ReadTimeout or specific logic.
  LogMessage('Base Indy settings applied from FHTTPServerConfig.', logDebug);
end;

function TServerBase.GetServerStateThreadSafe: TServerState;
begin
  FLock.Acquire;
  try
    Result := FServerState;
  finally
    FLock.Release;
  end;
end;

procedure TServerBase.SetServerState(AValue: TServerState);
var
  OldState: TServerState;
begin
  FLock.Acquire;
  try
    if FServerState <> AValue then
    begin
      OldState := FServerState;
      FServerState := AValue;
      LogMessage(Format('Server state changed from %s to %s.',
        [TRttiEnumerationType.GetName<TServerState>(OldState), TRttiEnumerationType.GetName<TServerState>(FServerState)]), logInfo);

      if (OldState <> ssRunning) and (FServerState = ssRunning) then
      begin
        FStartUpTimeUTC := NowUTC;
        LogMessage(Format('Server marked as running. Startup time (UTC): %s', [DateToISO8601(FStartUpTimeUTC)]), logInfo);
      end;
    end;
  finally
    FLock.Release;
  end;
end;

procedure TServerBase.InitializeFrameworkComponents;
var
  ServerConfigSection, CorsConfigJSON, SecurityConfigSection, AuthMiddlewareConfigJSON, JWTSettingsConfigJSON, SecMiddlewareConfigJSON: TJSONObject;
begin
  LogMessage('TServerBase: Initializing framework components (Router, Middlewares, Controllers)...', logDebug);
  try
    if Assigned(FRouteManager) then
      TControllerRegistry.InitializeAll(FRouteManager)
    else
      LogMessage('FRouteManager is nil in InitializeFrameworkComponents. Cannot initialize controllers.', logError);

    // CORS Middleware
    FCORSMiddleware := nil; // Initialize to nil
    if FAppConfig.TryGetValue('server', ServerConfigSection) and Assigned(ServerConfigSection) then
    begin
      if ServerConfigSection.TryGetValue('cors', CorsConfigJSON) and Assigned(CorsConfigJSON) then
        FCORSMiddleware := TCORSMiddleware.Create(CorsConfigJSON)
      else
        LogMessage('CORS configuration ("server.cors") not found. CORS Middleware not created.', logDebug);
    end
    else
      LogMessage('Main "server" config section for CORS not found. CORS Middleware not created.', logDebug);

    // Auth and Security Middlewares
    FAuthMiddleware := nil;
    FSecurityMiddleware := nil;
    JWTSettingsConfigJSON := nil;
    AuthMiddlewareConfigJSON := nil;
    SecMiddlewareConfigJSON := nil;

    if FAppConfig.TryGetValue('security', SecurityConfigSection) and Assigned(SecurityConfigSection) then
    begin
      // Get JWT settings for AuthMiddleware
      if SecurityConfigSection.TryGetValue('jwt', JWTSettingsConfigJSON) and Assigned(JWTSettingsConfigJSON) then
      begin
        // Get AuthMiddleware specific settings
        if SecurityConfigSection.TryGetValue('authMiddleware', AuthMiddlewareConfigJSON) and Assigned(AuthMiddlewareConfigJSON) then
        begin
          FAuthMiddleware := TAuthMiddleware.Create(JWTSettingsConfigJSON, AuthMiddlewareConfigJSON);
          LogMessage('AuthMiddleware created.', logDebug);
        end
        else
          LogMessage('AuthMiddleware configuration ("security.authMiddleware") not found. Auth Middleware not created.', logDebug);
      end
      else
        LogMessage('JWT settings ("security.jwt") not found. Auth Middleware cannot be fully configured.', logError);


      // SecurityMiddleware specific settings
      if SecurityConfigSection.TryGetValue('securityMiddleware', SecMiddlewareConfigJSON) and Assigned(SecMiddlewareConfigJSON) then
      begin
        FSecurityMiddleware := TSecurityMiddleware.Create(SecMiddlewareConfigJSON);
        LogMessage('SecurityMiddleware created.', logDebug);
      end
      else
        LogMessage('SecurityMiddleware configuration ("security.securityMiddleware") not found. Security Middleware not created.', logDebug);
    end
    else
      LogMessage('Main "security" config section not found. Auth/Security Middlewares not created.', logDebug);

    LogMessage('TServerBase: Framework components initialization attempt finished.', logDebug);
  except
    on E: Exception do
    begin
      LogMessage(Format('Error initializing framework components: %s - %s', [E.ClassName, E.Message]), logError);
      raise; // Re-raise to halt server startup if critical components fail
    end;
  end;
end;

procedure TServerBase.ConfigureSSLFromConfig;
var
 CertPath, KeyPath, RootCertPath: string;
 OriginalCertPath, OriginalKeyPath, OriginalRootCertPath: string;
 LBasePath: string;
 IsProduction: Boolean;
 NeedsReconfiguration: Boolean;
begin
 LogMessage('TServerBase: Configuring SSL from FHTTPServerConfig...', logInfo);

 // Detectar ambiente de producción
 IsProduction := SameText(GetEnvironmentVariable('ENVIRONMENT'), 'PRODUCTION') or
                SameText(GetEnvironmentVariable('APP_ENV'), 'PROD') or
                (HTTPConfig.Port = 443) or // Puerto HTTPS estándar indica producción
                (HTTPConfig.Port = 8443); // Puerto HTTPS alternativo común

 // En producción, SSL debe estar habilitado
 if IsProduction and not FHTTPServerConfig.SSLEnabled then
 begin
   LogMessage('SECURITY ERROR: SSL is disabled but server appears to be running in production environment', logFatal);
   raise EConfigurationError.Create('SSL must be enabled in production environment');
 end;

 if not FHTTPServerConfig.SSLEnabled then
 begin
   LogMessage('SSL not enabled in FHTTPServerConfig. Skipping SSL setup.', logDebug);
   if Assigned(FSSLIOHandler) then
   begin
       if Assigned(FServerInstance) and (FServerInstance.IOHandler = FSSLIOHandler) then
           FServerInstance.IOHandler := nil;
       FreeAndNil(FSSLIOHandler);
   end;
   Exit;
 end;

 OriginalCertPath := FHTTPServerConfig.SSLCertFile;
 OriginalKeyPath := FHTTPServerConfig.SSLKeyFile;
 OriginalRootCertPath := FHTTPServerConfig.SSLRootCertFile;

 CertPath := OriginalCertPath;
 KeyPath := OriginalKeyPath;
 RootCertPath := OriginalRootCertPath;

 LBasePath := Trim(FHTTPServerConfig.BasePath);

 LogMessage(Format('SSL Config: BasePath="%s", CertFile="%s", KeyFile="%s", RootCertFile="%s"',
   [LBasePath, CertPath, KeyPath, RootCertPath]), logDebug);

 // Resolve paths if they are relative, using BasePath from config
 if LBasePath <> '' then
 begin
   LogMessage(Format('Attempting to resolve SSL paths against BasePath: "%s"', [LBasePath]), logDebug);
   if TPath.IsRelativePath(CertPath) then
   begin
      CertPath := TPath.Combine(LBasePath, CertPath);
      LogMessage(Format('Resolved CertPath to: "%s"', [CertPath]), logDebug);
   end
   else
      LogMessage(Format('CertPath "%s" is absolute, not using BasePath.', [OriginalCertPath]), logDebug);

   if TPath.IsRelativePath(KeyPath) then
   begin
      KeyPath := TPath.Combine(LBasePath, KeyPath);
      LogMessage(Format('Resolved KeyPath to: "%s"', [KeyPath]), logDebug);
   end
   else
      LogMessage(Format('KeyPath "%s" is absolute, not using BasePath.', [OriginalKeyPath]), logDebug);

   if (RootCertPath.Trim <> '') then
   begin
     if TPath.IsRelativePath(RootCertPath) then
     begin
       RootCertPath := TPath.Combine(LBasePath, RootCertPath);
       LogMessage(Format('Resolved RootCertPath to: "%s"', [RootCertPath]), logDebug);
     end
     else
       LogMessage(Format('RootCertPath "%s" is absolute, not using BasePath.', [OriginalRootCertPath]), logDebug);
   end;
 end
 else
 begin
   LogMessage('SSL Config: BasePath is empty. Using SSL file paths as specified (must be absolute or relative to CWD).', logDebug);
 end;

 // Validaciones críticas de SSL
 if CertPath.IsEmpty or KeyPath.IsEmpty then
 begin
   var ErrorMsg := 'SSL CertFile or KeyFile not specified or resolved to empty. SSL cannot be enabled.';
   LogMessage(ErrorMsg, logError);

   if IsProduction then
   begin
     LogMessage('CRITICAL: SSL configuration incomplete in production environment', logFatal);
     raise EConfigurationError.Create('SSL certificates must be properly configured in production');
   end
   else
   begin
     LogMessage('WARNING: SSL disabled due to missing certificates in development environment', logWarning);
     FHTTPServerConfig.SSLEnabled := False;
     Exit;
   end;
 end;

 // Validar existencia y permisos de archivos
 try
   ValidateSSLFiles(CertPath, KeyPath, RootCertPath, IsProduction);
 except
   on E: EConfigurationError do
   begin
     if IsProduction then
       raise // Re-lanzar en producción
     else
     begin
       LogMessage(Format('SSL validation failed in development, disabling SSL: %s', [E.Message]), logWarning);
       FHTTPServerConfig.SSLEnabled := False;
       Exit;
     end;
   end;
 end;

 // Verificar si necesita reconfiguración
 NeedsReconfiguration := True;
 if Assigned(FSSLIOHandler) then
 begin
   NeedsReconfiguration :=
     (FSSLIOHandler.SSLOptions.CertFile <> CertPath) or
     (FSSLIOHandler.SSLOptions.KeyFile <> KeyPath) or
     (FSSLIOHandler.SSLOptions.RootCertFile <> RootCertPath);

   if not NeedsReconfiguration then
   begin
     LogMessage('SSL configuration unchanged. Reusing existing SSL handler.', logDebug);
     // Asegurar que esté asignado al servidor
     if FServerInstance.IOHandler <> FSSLIOHandler then
       FServerInstance.IOHandler := FSSLIOHandler;
     Exit;
   end;
 end;

 // Limpiar handler existente solo si necesita reconfiguración
 if Assigned(FServerInstance.IOHandler) then
 begin
   if FServerInstance.IOHandler = FSSLIOHandler then
     FServerInstance.IOHandler := nil
   else
     FreeAndNil(FServerInstance.IOHandler);
 end;

 FreeAndNil(FSSLIOHandler);
 FSSLIOHandler := TIdServerIOHandlerSSLOpenSSL.Create(nil);

 try
   FSSLIOHandler.SSLOptions.CertFile      := CertPath;
   FSSLIOHandler.SSLOptions.KeyFile       := KeyPath;
   FSSLIOHandler.SSLOptions.RootCertFile  := RootCertPath;
   FSSLIOHandler.SSLOptions.Method        := sslvTLSv1_2; // O sslvTLSv1_3 dependiendo de la versión de OpenSSL
   FSSLIOHandler.SSLOptions.Mode          := sslmServer;
   FSSLIOHandler.SSLOptions.VerifyMode    := []; // No requerir certificado de cliente por defecto
   FSSLIOHandler.SSLOptions.VerifyDepth   := 2;

   LogMessage(Format('SSL IOHandler Configured: CertFile=%s, KeyFile=%s, RootCertFile=%s, Method=TLSv1.2 (o superior preferido)',
     [FSSLIOHandler.SSLOptions.CertFile, FSSLIOHandler.SSLOptions.KeyFile, FSSLIOHandler.SSLOptions.RootCertFile]), logInfo);

   FServerInstance.IOHandler := FSSLIOHandler; // Asignar el handler configurado al servidor Indy

   // Validar configuración SSL después de crearla
   if IsProduction then
     ValidateSSLConfiguration;

 except
   on E: Exception do
   begin
     LogMessage(Format('Error configuring SSL IOHandler: %s - %s. SSL will be disabled.', [E.ClassName, E.Message]), logError);
     FreeAndNil(FSSLIOHandler); // Liberar si la configuración falló
     if Assigned(FServerInstance) and (FServerInstance.IOHandler = FSSLIOHandler) then
        FServerInstance.IOHandler := nil;
     FHTTPServerConfig.SSLEnabled := False; // Asegurar que el estado refleje el fallo

     if IsProduction then
       raise EConfigurationError.CreateFmt('Critical SSL configuration failure in production: %s', [E.Message]);
   end;
 end;
end;

procedure TServerBase.ValidateSSLFiles(const CertPath, KeyPath, RootCertPath: string; IsProduction: Boolean);
var
 TestStream: TFileStream;
begin
 // Validar existencia de archivos de certificado
 if not TFile.Exists(CertPath) then
 begin
   var ErrorMsg := Format('SSL CertFile not found at resolved path: "%s" (Original: "%s").', [CertPath, FHTTPServerConfig.SSLCertFile]);
   LogMessage(ErrorMsg, logError);
   raise EConfigurationError.Create(ErrorMsg);
 end;

 if not TFile.Exists(KeyPath) then
 begin
   var ErrorMsg := Format('SSL KeyFile not found at resolved path: "%s" (Original: "%s").', [KeyPath, FHTTPServerConfig.SSLKeyFile]);
   LogMessage(ErrorMsg, logError);
   raise EConfigurationError.Create(ErrorMsg);
 end;

 if (RootCertPath.Trim <> '') and (not TFile.Exists(RootCertPath)) then
 begin
   LogMessage(Format('SSL RootCertFile specified but not found at resolved path: "%s" (Original: "%s"). Proceeding without it.',
     [RootCertPath, FHTTPServerConfig.SSLRootCertFile]), logWarning);
   // No es crítico, SSL puede funcionar sin RootCert en algunos casos
 end;

 // Validaciones adicionales en producción
 if IsProduction then
 begin
   try
     // Verificar que el archivo de certificado sea legible y no esté vacío
     TestStream := TFileStream.Create(CertPath, fmOpenRead or fmShareDenyNone);
     try
       if TestStream.Size = 0 then
         raise EConfigurationError.CreateFmt('SSL certificate file is empty: %s', [CertPath]);
       if TestStream.Size < 100 then // Un certificado válido debe tener al menos cierto tamaño
         LogMessage(Format('WARNING: SSL certificate file seems unusually small (%d bytes): %s', [TestStream.Size, CertPath]), logWarning);
     finally
       TestStream.Free;
     end;

     // Verificar que el archivo de clave privada sea legible y no esté vacío
     TestStream := TFileStream.Create(KeyPath, fmOpenRead or fmShareDenyNone);
     try
       if TestStream.Size = 0 then
         raise EConfigurationError.CreateFmt('SSL key file is empty: %s', [KeyPath]);
       if TestStream.Size < 50 then // Una clave privada válida debe tener al menos cierto tamaño
         LogMessage(Format('WARNING: SSL key file seems unusually small (%d bytes): %s', [TestStream.Size, KeyPath]), logWarning);
     finally
       TestStream.Free;
     end;

     // Verificar permisos básicos - el archivo debe ser accesible
     var CertAttributes := TFile.GetAttributes(CertPath);
     var KeyAttributes := TFile.GetAttributes(KeyPath);

     if TFileAttribute.faReadOnly in CertAttributes then
       LogMessage(Format('SSL certificate file is read-only: %s', [CertPath]), logDebug);
     if TFileAttribute.faReadOnly in KeyAttributes then
       LogMessage(Format('SSL key file is read-only: %s', [KeyPath]), logDebug);

   except
     on E: EFileStreamError do
     begin
       var ErrorMsg := Format('Cannot access SSL file: %s', [E.Message]);
       LogMessage(ErrorMsg, logError);
       raise EConfigurationError.Create(ErrorMsg);
     end;
     on E: EConfigurationError do
       raise;// Re-lanzar errores de configuración
     on E: Exception do
     begin
       var ErrorMsg := Format('SSL file validation failed: %s', [E.Message]);
       LogMessage(ErrorMsg, logError);
       raise EConfigurationError.Create(ErrorMsg);
     end;
   end;
 end;

 LogMessage('SSL files validation passed', logDebug);
end;

procedure TServerBase.ValidateSSLConfiguration;
begin
 if not Assigned(FSSLIOHandler) then
 begin
   raise EConfigurationError.Create('SSL IOHandler not created despite SSL being enabled');
 end;

 // Validar que los archivos de certificado contengan datos válidos
 try
   // Intentar acceder a las propiedades SSL para forzar validación inicial
   var TestCertPath := FSSLIOHandler.SSLOptions.CertFile;
   var TestKeyPath := FSSLIOHandler.SSLOptions.KeyFile;

   if TestCertPath.IsEmpty or TestKeyPath.IsEmpty then
     raise EConfigurationError.Create('SSL IOHandler not properly configured with certificate paths');

   // Verificar que el modo esté correctamente configurado
   if FSSLIOHandler.SSLOptions.Mode <> sslmServer then
     raise EConfigurationError.Create('SSL IOHandler not configured in server mode');

   LogMessage('SSL configuration validation passed', logInfo);

 except
   on E: EConfigurationError do
     raise; // Re-lanzar errores de configuración
   on E: Exception do
   begin
     var ErrorMsg := Format('SSL configuration validation failed: %s', [E.Message]);
     LogMessage(ErrorMsg, logError);
     raise EConfigurationError.Create(ErrorMsg);
   end;
 end;
end;

function TServerBase.IsConnectionSecure(AContext: TIdContext): Boolean;
var
  SSLHandler: TIdSSLIOHandlerSocketOpenSSL;
begin
  Result := False;
  if Assigned(AContext) and Assigned(AContext.Connection) and Assigned(AContext.Connection.IOHandler) then
  begin
    if AContext.Connection.IOHandler is TIdSSLIOHandlerSocketOpenSSL then
    begin
      SSLHandler := TIdSSLIOHandlerSocketOpenSSL(AContext.Connection.IOHandler);
      try
        Result := not SSLHandler.PassThrough;
      except
        on E: Exception do
        begin
          LogMessage(Format('Error checking SSL status: %s', [E.Message]), logWarning);
          Result := False;
        end;
      end;
    end;
  end;
end;

procedure TServerBase.DoIndyConnect(AContext: TIdContext);
begin
  TInterlocked.Increment(FActiveConnections);
  LogMessage(Format('New connection from %s. Active connections: %d',
    [IfThen(Assigned(AContext) and Assigned(AContext.Binding), AContext.Binding.PeerIP, 'UnknownIP'), FActiveConnections]), logInfo);
end;

procedure TServerBase.DoIndyDisconnect(AContext: TIdContext);
begin
  TInterlocked.Decrement(FActiveConnections);
  LogMessage(Format('Connection closed from %s. Active connections: %d',
    [IfThen(Assigned(AContext) and Assigned(AContext.Binding), AContext.Binding.PeerIP, 'UnknownIP'), FActiveConnections]), logInfo);
end;

procedure TServerBase.DoIndyRequest(AContext: TIdContext; ARequest: TIdHTTPRequestInfo; AResponse: TIdHTTPResponseInfo);
var
  LStartTimeTicks: Int64;
  LHandledByRouter: Boolean;
  LContinueProcessing: Boolean;
  LRouteInfo: TRoute;
  LRouteFound: Boolean;
  RouteRequiresAuthCheck: Boolean;
begin
  LStartTimeTicks := TStopwatch.GetTimestamp;
  LContinueProcessing := True;
  LHandledByRouter := False;
  LRouteFound := False;
  FillChar(LRouteInfo, SizeOf(TRoute), 0);

  TInterlocked.Increment(FTotalRequests);

  LogMessage(Format('Request: %s %s from %s. UserAgent: %s',
              [ARequest.Command, ARequest.Document, AContext.Binding.PeerIP, ARequest.UserAgent]), logDebug);
  try
    if Assigned(FCORSMiddleware) then
    begin
      if FCORSMiddleware.ApplyCORSPolicy(ARequest, AResponse) then
      begin
        LogMessage(Format('CORS preflight or policy applied for %s. Request fully handled by CORS middleware.', [ARequest.Document]), logInfo);
        LContinueProcessing := False;
      end;
    end;

    if LContinueProcessing and Assigned(FSecurityMiddleware) then
    begin
      if not FSecurityMiddleware.ValidateRequest(ARequest, AResponse) then
      begin
        LogMessage(Format('Request rejected by SecurityMiddleware: %s %s. Response: %d',
                    [ARequest.Command, ARequest.Document, AResponse.ResponseNo]), logWarning);
        LContinueProcessing := False;
      end;
    end;

    if LContinueProcessing and Assigned(FRouteManager) then
    begin
      LRouteFound := FRouteManager.FindRouteInfo(ARequest.Command, ARequest.Document, LRouteInfo);
      if not LRouteFound then
         LogMessage(Format('No route definition found by FindRouteInfo for %s %s. Auth will proceed assuming protection if applicable.',
           [ARequest.Command, ARequest.Document]), logDebug);
    end;

    if LContinueProcessing and Assigned(FAuthMiddleware) then
    begin
      RouteRequiresAuthCheck := True;
      if LRouteFound then RouteRequiresAuthCheck := LRouteInfo.RequiresAuth;

      if not FAuthMiddleware.Authenticate(ARequest, AResponse, RouteRequiresAuthCheck) then
      begin
          LogMessage(Format('Request rejected by AuthMiddleware: %s %s. Response: %d',
                            [ARequest.Command, ARequest.Document, AResponse.ResponseNo]), logWarning);
          LContinueProcessing := False;
      end;
    end;

    if LContinueProcessing and Assigned(FRouteManager) then
    begin
      LHandledByRouter := FRouteManager.HandleRoute(ARequest, AResponse);
      if not LHandledByRouter and (AResponse.ResponseNo < 400) then
      begin
        LogMessage(Format('RouteManager.HandleRoute returned false but no error set for %s %s. Setting 404.',
          [ARequest.Command, ARequest.Document]), logWarning);
        AResponse.ResponseNo := 404;
        AResponse.ContentType := 'application/json';
        AResponse.ContentText := '{"success":false, "message":"Endpoint not found."}';
      end;
    end
    else if LContinueProcessing and not Assigned(FRouteManager) then
    begin
      LogMessage('RouteManager not assigned. Cannot dispatch request.', logError);
      AResponse.ResponseNo := 500;
      AResponse.ContentType := 'application/json';
      AResponse.ContentText := '{"success":false, "message":"Internal server error: Router not configured."}';
    end;

  except
    on E: Exception do
    begin
      TInterlocked.Increment(FFailedRequests);
      // Pasar ARequest directamente a DoIndyErrorInRequest
      DoIndyErrorInRequest(AContext, ARequest, AResponse, E);
    end;
  end;
  var logLevel: TLogLevel;
  var LElapsedMs := Trunc((TStopwatch.GetTimestamp - LStartTimeTicks) / TStopwatch.Frequency * 1000);
  logLevel:=logInfo;
  if AResponse.ResponseNo >= 400 then
     logLevel:=logWarning;
  LogMessage(Format('Request %s %s from %s processed in %dms. Response: %d. ContentType: %s. Sent: %d bytes.',
             [ARequest.Command, ARequest.Document, AContext.Binding.PeerIP,
              LElapsedMs, AResponse.ResponseNo, AResponse.ContentType, AResponse.ContentLength]),logLevel);
end;

procedure TServerBase.IndyServerOnExceptionHandler(AContext: TIdContext; AException: Exception);
var
  ClientIP: string;
begin
  ClientIP := 'UnknownIP';
  if Assigned(AContext) and Assigned(AContext.Binding) then
    ClientIP := AContext.Binding.PeerIP;

  TInterlocked.Increment(FFailedRequests);

  LogMessage(Format('Indy Server Exception for %s: %s - %s. Context: %p',
    [ClientIP, AException.ClassName, AException.Message, Pointer(AContext)]), logError);
end;

// Firma modificada para aceptar ARequest directamente
procedure TServerBase.DoIndyErrorInRequest(AContext: TIdContext; ARequest: TIdHTTPRequestInfo; AResponse: TIdHTTPResponseInfo; AException: Exception);
begin
  // Ya no es necesario obtener OriginalRequest de AContext, se pasa directamente.
  // El parámetro ARequest (TIdHTTPRequestInfo) ahora viene del llamador (DoIndyRequest).
  TBaseController.HandleError(AException, AResponse, ARequest);
end;

procedure TServerBase.CleanupServerResources;
begin
  LogMessage('TServerBase: Cleaning up framework resources (middlewares)...', logDebug);
  FreeAndNil(FSecurityMiddleware);
  FreeAndNil(FAuthMiddleware);
  FreeAndNil(FCORSMiddleware);
end;

procedure TServerBase.PerformShutdownTasks;
var
  MaxWaitSeconds: Integer;
  StartTime: TDateTime;
  ActiveContextsCount: Integer;
begin
  LogMessage('TServerBase: Performing pre-stop shutdown tasks (waiting for active contexts)...', logDebug);

  MaxWaitSeconds := Self.HTTPConfig.ShutdownGracePeriodSeconds;

  if Assigned(HTTPServerInstance) and Assigned(HTTPServerInstance.Contexts) and (MaxWaitSeconds > 0) then
  begin
    StartTime := NowUTC;
    // Loop until contexts are done or timeout is reached
    repeat
      var LContextList := HTTPServerInstance.Contexts.LockList;
      try
        ActiveContextsCount := LContextList.Count;
      finally
        HTTPServerInstance.Contexts.UnlockList;
      end;

      if ActiveContextsCount = 0 then Break; // All contexts finished
      // ...
    until SecondsBetween(NowUTC, StartTime) >= MaxWaitSeconds;

    // Final check
    var LFinalContextList := HTTPServerInstance.Contexts.LockList;
    try
      ActiveContextsCount := LFinalContextList.Count;
    finally
      HTTPServerInstance.Contexts.UnlockList;
    end;

    ActiveContextsCount := HTTPServerInstance.Contexts.LockList.Count; // Final check
    HTTPServerInstance.Contexts.UnlockList;
    if ActiveContextsCount > 0 then
      LogMessage(Format('TServerBase: Shutdown grace period (%d sec) ended. %d active Indy contexts may be cut off.',
         [MaxWaitSeconds, ActiveContextsCount]), logWarning)
    else
      LogMessage('TServerBase: All active Indy contexts closed gracefully within grace period.', logInfo);
  end
  else if MaxWaitSeconds <= 0 then
    LogMessage('TServerBase: ShutdownGracePeriodSeconds is 0 or less. Not waiting for active contexts.', logDebug);

  LogMessage('TServerBase: Pre-stop shutdown tasks completed.', logDebug);
end;

function TServerBase.IsRunning: Boolean;
var
  LState: TServerState;
  LServerIndyActive: Boolean;
begin
  if not Assigned(FLock) then // Should not happen if Create was successful
  begin
    Result := False;
    Exit;
  end;

  FLock.Acquire;
  try
    LState := FServerState;
    LServerIndyActive := Assigned(FServerInstance) and FServerInstance.Active;
  finally
    FLock.Release;
  end;
  Result := (LState = ssRunning) and LServerIndyActive;
end;

function TServerBase.GetServerStats: TJSONObject;
var
  LState: TServerState;
  LStartupTime: TDateTime;
  LActiveConns: Integer;
  LTotalReqs: Int64;
  LFailedReqs: Integer;
  LServerIndyActive: Boolean;
begin
  Result := TJSONObject.Create;

  if not Assigned(FLock) then
  begin
    LogMessage('FLock not assigned in GetServerStats. Returning error stats.', logError);
    Result.AddPair('error', 'Server state lock not available');
    Result.AddPair('state', TRttiEnumerationType.GetName<TServerState>(ssError));
    Exit;
  end;

  FLock.Acquire;
  try
    LStartupTime := FStartUpTimeUTC;
    LState := FServerState;
    LActiveConns := FActiveConnections;
    LTotalReqs := FTotalRequests;
    LFailedReqs := FFailedRequests;
    LServerIndyActive := Assigned(FServerInstance) and FServerInstance.Active;
  finally
    FLock.Release;
  end;

  Result.AddPair('state', TRttiEnumerationType.GetName<TServerState>(LState));
  Result.AddPair('startup_time_utc', DateToISO8601(LStartupTime));
  Result.AddPair('active_connections', LActiveConns);
  Result.AddPair('total_requests', LTotalReqs);
  Result.AddPair('failed_requests', LFailedReqs);
  Result.AddPair('server_indy_active', LServerIndyActive);
  // Placeholders for more detailed stats if implemented
  Result.AddPair('bytes_sent', TJSONNumber.Create(0));
  Result.AddPair('bytes_received', TJSONNumber.Create(0));
  Result.AddPair('avg_response_time_ms', TJSONNumber.Create(0.0));
end;

end.
