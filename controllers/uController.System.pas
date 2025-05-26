unit uController.System;

interface

uses
  System.SysUtils, System.JSON, System.Generics.Collections, System.Rtti, // Added Rtti
  IdCustomHTTPServer,
  uLib.Controller.Base,
  uLib.Routes,
  uLib.Database.Pool,
  uLib.Database.Types,
  uLib.Database.Connection, // Para el cast a TBaseConnection si fuera necesario
  uLib.Logger,
  uLib.Server.Types,      // Para EConfigurationError
  uLib.Config.Manager;  // Added for centralized config access

type
  TSystemController = class(TBaseController)
  private
    class procedure ValidateLoginCredentials(const CredentialsJSON: TJSONObject;
      out Username, PasswordOut: string);
  public
    class procedure RegisterRoutes; override;

    class procedure Login(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
    class procedure Logout(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
    class procedure GetSystemStatus(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
    class procedure GetSystemMetrics(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
  end;

const
  SYSTEM_CONTROLLER_DB_POOL_NAME = 'MainDB_PG'; // Ajustar según config.json

  SQL_VALIDATE_USER =
    'SELECT c.id, l.user_name, r.name as role, l.password,'+
    '       c.email, c.first_name, c.last_name, c.phone,'+
    '       c.is_active, c.is_verified'+
    '  FROM login_users l'+
    '       INNER JOIN clients c ON'+
    '             (l.client_id = c.id)'+
    '       INNER JOIN roles r ON'+
    '             (l.role_id = r.id)'+
    ' WHERE (LOWER(l.user_name) = LOWER(:username)) AND (password=:password_hashed)';

implementation

uses
  System.DateUtils, System.Hash, System.NetEncoding,
  Data.DB, FireDac.Stan.Param,
  JOSE.Core.JWT, JOSE.Core.Builder, JOSE.Core.JWK,
  uLib.Session.Manager,
  uLib.Server.Manager,

  uLib.Utils;

class procedure TSystemController.RegisterRoutes;
begin
  if not Assigned(FRouteManager) then
  begin
    LogMessage('CRITICAL: FRouteManager not assigned in TSystemController. Routes will not be registered.', logError);
    Exit;
  end;

  //FRouteManager.AddRoute('GET',  '/', GetSystemStatus, False);
  FRouteManager.AddRoute('POST', 'login', Login, False);
  FRouteManager.AddRoute('GET',  'status', GetSystemStatus, False);
  FRouteManager.AddRoute('POST', 'logout', Logout, True); // Logout should require auth
  FRouteManager.AddRoute('GET',  'metrics', GetSystemMetrics, True); // Metrics typically require auth
  LogMessage('TSystemController routes registered.', logInfo);
end;

class procedure TSystemController.ValidateLoginCredentials(
  const CredentialsJSON: TJSONObject; out Username, PasswordOut: string);
begin
  if not Assigned(CredentialsJSON) then
    raise EMissingParameterException.Create('Missing credentials in request body.');

  Username := TJSONHelper.GetString(CredentialsJSON, 'username', '').Trim;
  PasswordOut := TJSONHelper.GetString(CredentialsJSON, 'password', ''); // No hacer Trim a la contraseña

  if Username.IsEmpty then
    raise EMissingParameterException.Create('Username is required.');
  if PasswordOut = '' then // Comparar con string vacío, no Trimmed
    raise EMissingParameterException.Create('Password is required.');
  LogMessage('Login credentials syntax validated (username and password presence).', logDebug);
end;

class procedure TSystemController.Login(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
var
  CredentialsJSONValue: TJSONValue;
  CredentialsJSON: TJSONObject;
  Username, Password, UserID, UserRole, FullName, Email: string;
  HashedLoginPassword, PasswordSaltFromConfig: string;
  DBConn: IDBConnection;
  Dataset: TDataSet;
  Session: TSessionData;
  Params: TFDParams;
  JWT: TJWT;
  JWKSignKey: TJWK;
  TokenString: string;
  TokenExpiryHours: Integer;
  JWTSecretFromConfig, JWTIssuerFromConfig, JWTAudienceFromConfig: string;
  RespJSON: TJSONObject;
  ConfigMgr: TConfigManager;
  JWTSettingsJSON: TJSONObject; // Sub-sección "jwt"
begin
  CredentialsJSONValue := nil; CredentialsJSON := nil; DBConn := nil; Dataset := nil;
  JWT := nil; JWKSignKey := nil; RespJSON := nil; JWTSettingsJSON := nil;

  try
    // --- Cargar Configuración JWT de forma centralizada ---
    ConfigMgr := TConfigManager.GetInstance(); // Obtener instancia del gestor de configuración
    JWTSettingsJSON := TJSONHelper.GetJSONObject(ConfigMgr.ConfigData, 'security.jwt'); // Obtener la sección 'security.jwt'

    if not Assigned(JWTSettingsJSON) then
    begin
      LogMessage('TSystemController.Login: JWT configuration section ("security.jwt") not found in app config via TConfigManager. Login will fail.', logError);
      raise EConfigurationError.Create('JWT settings are missing or incomplete in the server configuration.');
    end;

    JWTSecretFromConfig    := TJSONHelper.GetString(JWTSettingsJSON, 'secret', '');
    JWTIssuerFromConfig    := TJSONHelper.GetString(JWTSettingsJSON, 'issuer', 'DefaultIssuer');
    JWTAudienceFromConfig  := TJSONHelper.GetString(JWTSettingsJSON, 'audience', 'DefaultAudience');
    TokenExpiryHours       := TJSONHelper.GetInteger(JWTSettingsJSON, 'expirationHours', 1);
    PasswordSaltFromConfig := TJSONHelper.GetString(JWTSettingsJSON, 'passwordSalt', ''); // Sal global para el hash (ver nota de seguridad)

    if JWTSecretFromConfig.IsEmpty or (Length(JWTSecretFromConfig) < 32) then
    begin
       LogMessage('CRITICAL SECURITY WARNING: JWT_SECRET_KEY is missing, empty, or too short! Must be at least 32 random bytes.', logFatal);
       raise EConfigurationError.Create('JWT secret key is insecure or not configured.');
    end;
   (*
    if PasswordSaltFromConfig.IsEmpty then
    begin
       LogMessage('CRITICAL SECURITY WARNING: PasswordSalt is missing or empty in JWT settings ("security.jwt.passwordSalt").', logFatal);
       raise EConfigurationError.Create('Password salt is not configured for password hashing.');
    end;
    *)
    // --- Fin Configuración JWT ---

    CredentialsJSONValue := GetRequestBody(Request);
    if not (Assigned(CredentialsJSONValue) and (CredentialsJSONValue is TJSONObject)) then
      raise EInvalidRequestException.Create('Request body must be a valid JSON object for login.');
    CredentialsJSON := CredentialsJSONValue as TJSONObject;

    ValidateLoginCredentials(CredentialsJSON, Username, Password);

    // --- Hashing de Contraseña ---
    // SEGURIDAD IMPORTANTE:
    // 1. Usar sales ÚNICAS POR USUARIO, almacenadas junto al hash del usuario.
    // 2. Usar algoritmos de hash ADAPTATIVOS y LENTOS (Argon2, scrypt, bcrypt) en lugar de SHA256 simple.
    // La implementación actual con una sal global y SHA256 es vulnerable a ataques de rainbow tables y fuerza bruta.
    // Este es un placeholder y DEBE ser reemplazado en un entorno de producción.
    HashedLoginPassword := THashSHA2.GetHashString(Lowercase(username)+':'+Password + PasswordSaltFromConfig);
    LogMessage(Format('Login attempt for user: %s. Hashing password for comparison (SECURITY PLACEHOLDER - DO NOT USE IN PRODUCTION AS IS).', [Username]), logDebug);
    // --- Fin Hashing de Contraseña ---
    Params:=TFDParams.Create;
    Params.Add('username',Username);
    Params.Add('password_hash',HashedLoginPassword);
    DBConn := AcquireDBConnection(SYSTEM_CONTROLLER_DB_POOL_NAME);
    Dataset := DBConn.ExecuteReader(SQL_VALIDATE_USER, params);
    try
      if Dataset.IsEmpty then
      begin
        LogMessage(Format('Login failed for user: %s. Invalid username or password.', [Username]), logWarning);
        raise EUnauthorizedException.Create('Invalid username or password.');
      end;

      UserID     := Dataset.FieldByName('id').AsString; // Asumir que el ID es string, ajustar si es Integer
      UserRole   := Dataset.FieldByName('role').AsString;
      FullName   := Dataset.FieldByName('full_name').AsString;
      Email      := Dataset.FieldByName('email').AsString;
      LogMessage(Format('User %s (ID: %s, Role: %s) successfully authenticated against database.', [Username, UserID, UserRole]), logInfo);
    finally
      FreeAndNil(Dataset);
    end;
    try
      Session := TSessionManager.GetInstance.CreateNewSession;
      Session.SetValue('user_id', UserID);
      Session.SetValue('username', Username);
      Session.SetValue('user_role', UserRole);
      LogMessage(Format('Server session %s created for user %s.', [Session.ID, Username]), logInfo);

      JWT := TJWT.Create;
      JWKSignKey := TJWK.Create(TEncoding.UTF8.GetBytes(JWTSecretFromConfig)); // Usar la clave secreta de la config

      JWT.Claims.Issuer     := JWTIssuerFromConfig;
      JWT.Claims.Audience   := JWTAudienceFromConfig;
      JWT.Claims.Subject    := UserID; // Subject suele ser el ID del usuario
      JWT.Claims.IssuedAt   := NowUTC;
      JWT.Claims.Expiration := IncHour(NowUTC, TokenExpiryHours);
      JWT.Claims.SetClaim('username', Username); // Claim personalizado
      JWT.Claims.SetClaim('role', UserRole);     // Claim personalizado
      JWT.Claims.SetClaim('sid', Session.ID);    // ID de sesión del servidor, para posible revocación/gestión

      //TokenString := TJOSE.BuildJWT(TJOSEHMACSigner.HS256(JWKSignKey), JWT);
      TokenString := TJOSE.SHA256CompactToken(TJSONHelper.GetString(GConfigManager.ConfigData,'security.jwt.secret'), JWT);
      RespJSON := TJSONObject.Create;
      var UserInfoJSON := TJSONObject.Create;
      UserInfoJSON.AddPair('id', UserID);
      UserInfoJSON.AddPair('username', Username);
      UserInfoJSON.AddPair('fullName', FullName);
      UserInfoJSON.AddPair('email', Email);
      UserInfoJSON.AddPair('role', UserRole);
      RespJSON.AddPair('user', UserInfoJSON);
      RespJSON.AddPair('token', TokenString);
      RespJSON.AddPair('expiresInSeconds', TJSONNumber.Create(TokenExpiryHours * 3600)); // Enviar en segundos

      Response.ContentType := 'application/json';
      Response.ResponseNo := 200;
      Response.ContentText := RespJSON.ToJSON;
      LogMessage(Format('Login successful for user %s. JWT issued.', [Username]), logInfo);

    except
      on E: Exception do
        HandleError(E, Response, Request); // TBaseController.HandleError se encarga de loguear y formatear
    end;
  finally
    FreeAndNil(CredentialsJSONValue); // CredentialsJSON es un cast, no necesita liberación separada
    FreeAndNil(JWT);
    FreeAndNil(JWKSignKey);
    FreeAndNil(RespJSON);
    // JWTSettingsJSON es una referencia obtenida de TConfigManager, no se libera aquí.
    // ConfigMgr es una referencia a un Singleton, no se libera aquí.
    ReleaseDBConnection(DBConn, SYSTEM_CONTROLLER_DB_POOL_NAME);
  end;
end;

class procedure TSystemController.Logout(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
var
  SessionIDFromClaim: string;
begin
  try
    // TAuthMiddleware debería haber validado el token y poblado ARequest.Params
    SessionIDFromClaim := Request.Params.Values['session_id'];

    if SessionIDFromClaim <> '' then
    begin
      // InvalidateSession en TSessionManager se encarga de remover la sesión del servidor.
      // El token JWT en sí mismo sigue siendo válido hasta que expire, a menos que se implemente una lista negra.
      TSessionManager.GetInstance.InvalidateSession(SessionIDFromClaim);
      LogMessage(Format('User server session %s invalidated due to logout request.', [SessionIDFromClaim]), logInfo);
    end
    else
      LogMessage('Logout: No server session ID found in request context (e.g., from JWT''s "sid" claim). Client is responsible for discarding JWT.', logDebug);

    Response.ContentType := 'application/json';
    Response.ResponseNo := 200;
    Response.ContentText := '{"success":true, "message":"Logged out successfully. Please discard your token on client-side."}';
  except
    on E: Exception do
      HandleError(E, Response, Request);
  end;
end;


class procedure TSystemController.GetSystemStatus(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
var
  StatusJSON: TJSONObject;
  LServerManager: TServerManager;
  LServerBaseStatsJSON: TJSONObject;
  LDBPoolManager: TDBConnectionPoolManager;
  LDBPoolsStatsArray: TJSONArray;
  AppName, AppVersion: string;
  ConfigMgr: TConfigManager;
  AppSectionFromConfig: TJSONObject;
begin
  StatusJSON := TJSONObject.Create;
  LServerBaseStatsJSON := nil;
  LDBPoolsStatsArray := nil;
  AppSectionFromConfig := nil;

  try
    ConfigMgr := TConfigManager.GetInstance();
    AppSectionFromConfig := TJSONHelper.GetJSONObject(ConfigMgr.ConfigData,'application'); // Obtener sección 'application'

    if Assigned(AppSectionFromConfig) then
    begin
      AppName := TJSONHelper.GetString(AppSectionFromConfig, 'name', 'UnknownApp');
      AppVersion := TJSONHelper.GetString(AppSectionFromConfig, 'version', '0.0.0');
    end
    else
    begin
      AppName := 'MercadoSaintServer (No AppConfig)';
      AppVersion := 'N/A';
      LogMessage('GetSystemStatus: "application" section not found in AppConfig via TConfigManager.', logWarning);
    end;
    StatusJSON.AddPair('application_name', AppName);
    StatusJSON.AddPair('application_version', AppVersion);

    LServerManager := TServerManager.GetInstance();
    if Assigned(LServerManager) and Assigned(LServerManager.ServerInstance) then
    begin
      LServerBaseStatsJSON := LServerManager.ServerInstance.GetServerStats; // Devuelve un TJSONObject nuevo
      if Assigned(LServerBaseStatsJSON) then
      begin
        StatusJSON.AddPair('web_server_stats', LServerBaseStatsJSON); // StatusJSON toma posesión
        LServerBaseStatsJSON := nil; // Evitar doble liberación
      end
      else
        StatusJSON.AddPair('web_server_stats', TJSONString.Create('Not available (GetServerStats returned nil)'));
    end
    else
      StatusJSON.AddPair('web_server_manager', TJSONString.Create('Not available or server not initialized'));

    StatusJSON.AddPair('timestamp_utc', DateToISO8601(NowUTC));
    StatusJSON.AddPair('status_message', 'System operational');

    try
      LDBPoolManager := TDBConnectionPoolManager.GetInstance;
      if Assigned(LDBPoolManager) then
      begin
        LDBPoolsStatsArray := LDBPoolManager.GetAllPoolsStats; // Devuelve un TJSONArray nuevo
        StatusJSON.AddPair('database_pools', LDBPoolsStatsArray); // StatusJSON toma posesión
        LDBPoolsStatsArray := nil; // Evitar doble liberación
      end
      else
        StatusJSON.AddPair('database_pool_manager', TJSONString.Create('Not available'));

      Response.ContentType := 'application/json';
      Response.ResponseNo := 200;
      Response.ContentText := StatusJSON.ToJSON;
    except
      on E: Exception do
        HandleError(E, Response, Request);
    end;
  finally
    FreeAndNil(StatusJSON); // Libera StatusJSON y los objetos JSON que posee
    // AppSectionFromConfig es una referencia, no se libera aquí.
    // LServerBaseStatsJSON y LDBPoolsStatsArray ya fueron nilled o su posesión transferida.
  end;
end;

class procedure TSystemController.GetSystemMetrics(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
var
  MetricsJSON: TJSONObject;
  // Funciones placeholder para métricas. En un sistema real, se obtendrían de APIs del SO o librerías.
  function GetCurrentCPULoad: Double; begin Randomize; Result := 10.0 + Random * 25.0; end; // Simulado 10-35%
  function GetCurrentFreeMemoryMB: Int64; begin Randomize; Result := 512 + Random(2048); end; // Simulado 512MB - 2.5GB
begin
  MetricsJSON := TJSONObject.Create;
  try
    try
      MetricsJSON.AddPair('cpu_load_percent', GetCurrentCPULoad);
      MetricsJSON.AddPair('memory_free_mb', GetCurrentFreeMemoryMB);
      // Aquí se podrían añadir más métricas: uso de disco, descriptores de archivo, etc.
      // Estas métricas son específicas de la plataforma y pueden requerir código condicional o librerías.

      Response.ContentType := 'application/json';
      Response.ResponseNo := 200;
      Response.ContentText := MetricsJSON.ToJSON;
    except
      on E: Exception do
        HandleError(E, Response, Request);
    end;
  finally
    MetricsJSON.Free;
  end;
end;

initialization
  TControllerRegistry.RegisterController(TSystemController);
end.

