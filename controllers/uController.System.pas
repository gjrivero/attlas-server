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

type
  TPasswordHashingLib = class
  public
    class function VerifyPassword(const APassword, AStoredHash, AStoredSalt: string): Boolean;
  end;

const
  SYSTEM_CONTROLLER_DB_POOL_NAME = 'MainDB_PG'; // Ajustar seg�n config.json

  SQL_VALIDATE_USER =
    'SELECT cl.id, lu.user_name, r.name as role, lu.password_hash, lu.password_salt, ' +
    '       cl.email, cl.first_name as fullName, cl.last_name, cl.phone, ' +
    '       lu.is_active, lu.is_verified ' +
    '  FROM login_users lu' +
    '       INNER JOIN clients cl ON (lu.client_id = cl.id)' +
    '       INNER JOIN roles r ON (lu.role_id = r.id)' +
    ' WHERE (LOWER(lu.user_name) = LOWER(:username)) AND lu.is_active = true';

implementation

uses
  System.DateUtils, System.Hash, System.NetEncoding,
  Data.DB, FireDac.Stan.Param,
  JOSE.Core.JWT, JOSE.Core.Builder, JOSE.Core.JWK,
  uLib.Session.Manager,
  uLib.Server.Manager,

  uLib.Utils;

class function TPasswordHashingLib.VerifyPassword(const APassword, AStoredHash, AStoredSalt: string): Boolean;
var
  AttemptedHash: string;
begin
  // ESTA ES UNA IMPLEMENTACI�N DE EJEMPLO MUY SIMPLIFICADA Y NO SEGURA.
  // DEBES USAR UNA LIBRER�A REAL PARA BCRYPT, SCRYPT O ARGON2.
  // Aqu� solo se simula el proceso.
  // Una librer�a real tomar�a (Password, Salt) -> Hash, y luego comparar�a ese Hash con StoredHash.
  // Por ejemplo, si SHA256 fuera el (mal) algoritmo:
  // AttemptedHash := THashSHA2.GetHashString(APassword + AStoredSalt); // NO USAR SHA256 PARA CONTRASE�AS
  // Result := CompareMem(PChar(AttemptedHash), PChar(AStoredHash), Length(AStoredHash));

  // Dado que no puedo implementar una librer�a aqu�, se asume que esta funci�n hace la magia:
  LogMessage('[TPasswordHashingLib.VerifyPassword] Placeholder: Comparando contrase�a proporcionada con hash almacenado usando sal. ESTO DEBE SER REEMPLAZADO CON UNA LIBRER�A SEGURA.', logCritical);
  // Simulaci�n de fallo/�xito para permitir que el flujo contin�e.
  // En una implementaci�n real, esto har�a la comparaci�n criptogr�fica.
  if (AStoredHash <> '') and (APassword = 'testpassword') then // Simulaci�n de �xito con una contrase�a de prueba
    Result := True
  else
    Result := False;
  if not Result then
     LogMessage(Format('[TPasswordHashingLib.VerifyPassword] Placeholder: La contrase�a "%s" no coincide con el hash almacenado "%s" usando la sal "%s".', [APassword, Copy(AStoredHash,1,10)+'...', Copy(AStoredSalt,1,5)+'...']), logDebug);
end;

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
  PasswordOut := TJSONHelper.GetString(CredentialsJSON, 'password', ''); // No hacer Trim a la contrase�a

  if Username.IsEmpty then
    raise EMissingParameterException.Create('Username is required.');
  if PasswordOut = '' then // Comparar con string vac�o, no Trimmed
    raise EMissingParameterException.Create('Password is required.');
  LogMessage('Login credentials syntax validated (username and password presence).', logDebug);
end;

class procedure TSystemController.Login(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
var
  CredentialsJSONValue: TJSONValue;
  CredentialsJSON: TJSONObject;
  Username, Password, UserID, UserRole, FullName, Email: string;
  // HashedLoginPassword, PasswordSaltFromConfig: string; // Ya no se usan as�
  StoredPasswordHash, StoredPasswordSalt: string; // Nuevas variables
  DBConn: IDBConnection;
  Dataset: TDataSet;
  Session: TSessionData;
  Params: TFDParams;
  JWT: TJWT;
  // JWKSignKey: TJWK; // TJOSE.SHA256CompactToken no necesita TJWK directamente
  TokenString: string;
  TokenExpiryHours: Integer;
  JWTSecretFromConfig, JWTIssuerFromConfig, JWTAudienceFromConfig: string;
  RespJSON: TJSONObject;
  ConfigMgr: TConfigManager;
  JWTSettingsJSON: TJSONObject;
  LoginUserIsActive: Boolean;
begin
  CredentialsJSONValue := nil; CredentialsJSON := nil; DBConn := nil; Dataset := nil;
  JWT := nil; (*JWKSignKey := nil;*) RespJSON := nil; JWTSettingsJSON := nil;

  try
    ConfigMgr := TConfigManager.GetInstance();
    JWTSettingsJSON := TJSONHelper.GetJSONObject(ConfigMgr.ConfigData,'security.jwt');

    if not Assigned(JWTSettingsJSON) then
    begin
      LogMessage('TSystemController.Login: JWT configuration section ("security.jwt") not found. Login will fail.', logError);
      raise EConfigurationError.Create('JWT settings are missing in server configuration.');
    end;

    JWTSecretFromConfig    := TJSONHelper.GetString(JWTSettingsJSON, 'secret', '');
    JWTIssuerFromConfig    := TJSONHelper.GetString(JWTSettingsJSON, 'issuer', 'DefaultIssuer');
    JWTAudienceFromConfig  := TJSONHelper.GetString(JWTSettingsJSON, 'audience', 'DefaultAudience');
    TokenExpiryHours       := TJSONHelper.GetInteger(JWTSettingsJSON, 'expirationHours', 1);
    // PasswordSaltFromConfig := GetStr(JWTSettingsJSON, 'passwordSalt', ''); // YA NO SE USA PARA HASHING

    if JWTSecretFromConfig.IsEmpty or (Length(JWTSecretFromConfig) < 32) then
    begin
      LogMessage('CRITICAL SECURITY WARNING: JWT_SECRET_KEY is missing, empty, or too short! Must be at least 32 random bytes.', logFatal);
      raise EConfigurationError.Create('JWT secret key is insecure or not configured.');
    end;

    CredentialsJSONValue := GetRequestBody(Request);
    if not (Assigned(CredentialsJSONValue) and (CredentialsJSONValue is TJSONObject)) then
      raise EInvalidRequestException.Create('Request body must be a valid JSON object for login.');
    CredentialsJSON := CredentialsJSONValue as TJSONObject;

    ValidateLoginCredentials(CredentialsJSON, Username, Password); // Password aqu� es la contrase�a en texto plano

    // --- OBTENER HASH Y SALT ALMACENADOS DEL USUARIO ---
    Params := TFDParams.Create;
    try
      Params.Add('username', LowerCase(Username)); // Almacenar/comparar usernames en min�sculas es una opci�n
      DBConn := AcquireDBConnection(SYSTEM_CONTROLLER_DB_POOL_NAME);
      Dataset := DBConn.ExecuteReader(SQL_VALIDATE_USER, Params); // SQL_VALIDATE_USER ahora solo filtra por username y active
      try
        if Dataset.IsEmpty then
        begin
          LogMessage(Format('Login failed for user: %s. User not found or not active.', [Username]), logWarning);
          raise EUnauthorizedException.Create('Invalid username or password.'); // Mensaje gen�rico
        end;

        // Obtener el hash y la sal almacenados
        StoredPasswordHash := Dataset.FieldByName('password_hash').AsString;
        StoredPasswordSalt := Dataset.FieldByName('password_salt').AsString;
        LoginUserIsActive  := Dataset.FieldByName('is_active').AsBoolean; // Ya filtrado por la SQL, pero bueno tenerlo

        if not LoginUserIsActive then // Doble chequeo, aunque la SQL ya deber�a filtrar
        begin
          LogMessage(Format('Login failed for user: %s. User account is not active.', [Username]), logWarning);
          raise EUnauthorizedException.Create('User account is not active.');
        end;

        // --- VERIFICAR CONTRASE�A ---
        // ��� USAR UNA LIBRER�A DE HASHING SEGURA AQU� !!!
        if not TPasswordHashingLib.VerifyPassword(Password, StoredPasswordHash, StoredPasswordSalt) then
        begin
          LogMessage(Format('Login failed for user: %s. Password mismatch.', [Username]), logWarning);
          raise EUnauthorizedException.Create('Invalid username or password.'); // Mensaje gen�rico
        end;

        // Si llegamos aqu�, la contrase�a es correcta
        UserID     := Dataset.FieldByName('id').AsString;
        UserRole   := Dataset.FieldByName('role').AsString;
        FullName   := Dataset.FieldByName('fullName').AsString; // Ajustado al alias de la SQL
        Email      := Dataset.FieldByName('email').AsString;
        LogMessage(Format('User %s (ID: %s, Role: %s) successfully authenticated.', [Username, UserID, UserRole]), logInfo);
      finally
        FreeAndNil(Dataset);
      end;
    finally
      FreeAndNil(Params);
      // DBConn se libera en el finally principal
    end;

    // --- CREAR SESI�N Y JWT (sin cambios significativos aqu�, solo la l�gica previa de hash) ---
    Session := TSessionManager.GetInstance.CreateNewSession;
    Session.SetValue('user_id', UserID);
    Session.SetValue('username', Username);
    Session.SetValue('user_role', UserRole);
    LogMessage(Format('Server session %s created for user %s.', [Session.ID, Username]), logInfo);

    JWT := TJWT.Create;
    // JWKSignKey := TJWK.Create(TEncoding.UTF8.GetBytes(JWTSecretFromConfig)); // No necesario para SHA256CompactToken si se usa el secreto directamente

    JWT.Claims.Issuer     := JWTIssuerFromConfig;
    JWT.Claims.Audience   := JWTAudienceFromConfig;
    JWT.Claims.Subject    := UserID;
    JWT.Claims.IssuedAt   := NowUTC;
    JWT.Claims.Expiration := IncHour(NowUTC, TokenExpiryHours);
    JWT.Claims.SetClaim('username', Username);
    JWT.Claims.SetClaim('role', UserRole);
    JWT.Claims.SetClaim('sid', Session.ID);

    // Usar el secreto directamente como string, TJOSE.SHA256CompactToken lo convertir� a bytes internamente.
    TokenString := TJOSE.SHA256CompactToken(JWTSecretFromConfig, JWT);

    RespJSON := TJSONObject.Create;
    var UserInfoJSON := TJSONObject.Create;
    UserInfoJSON.AddPair('id', UserID);
    UserInfoJSON.AddPair('username', Username);
    UserInfoJSON.AddPair('fullName', FullName);
    UserInfoJSON.AddPair('email', Email);
    UserInfoJSON.AddPair('role', UserRole);
    RespJSON.AddPair('user', UserInfoJSON);
    RespJSON.AddPair('token', TokenString);
    RespJSON.AddPair('expiresInSeconds', TJSONNumber.Create(TokenExpiryHours * 3600));

    Response.ContentType := 'application/json';
    Response.ResponseNo := 200;
    Response.ContentText := RespJSON.ToJSON;
    LogMessage(Format('Login successful for user %s. JWT issued.', [Username]), logInfo);
  finally
    FreeAndNil(CredentialsJSONValue);
    FreeAndNil(JWT);
    // FreeAndNil(JWKSignKey); // Ya no se usa directamente aqu�
    FreeAndNil(RespJSON);
    ReleaseDBConnection(DBConn, SYSTEM_CONTROLLER_DB_POOL_NAME);
  end;

end;

class procedure TSystemController.Logout(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
var
  SessionIDFromClaim: string;
begin
  try
    // TAuthMiddleware deber�a haber validado el token y poblado ARequest.Params
    SessionIDFromClaim := Request.Params.Values['session_id'];

    if SessionIDFromClaim <> '' then
    begin
      // InvalidateSession en TSessionManager se encarga de remover la sesi�n del servidor.
      // El token JWT en s� mismo sigue siendo v�lido hasta que expire, a menos que se implemente una lista negra.
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
    AppSectionFromConfig := TJSONHelper.GetJSONObject(ConfigMgr.ConfigData,'application'); // Obtener secci�n 'application'

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
        StatusJSON.AddPair('web_server_stats', LServerBaseStatsJSON); // StatusJSON toma posesi�n
        LServerBaseStatsJSON := nil; // Evitar doble liberaci�n
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
        StatusJSON.AddPair('database_pools', LDBPoolsStatsArray); // StatusJSON toma posesi�n
        LDBPoolsStatsArray := nil; // Evitar doble liberaci�n
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
    // AppSectionFromConfig es una referencia, no se libera aqu�.
    // LServerBaseStatsJSON y LDBPoolsStatsArray ya fueron nilled o su posesi�n transferida.
  end;
end;

class procedure TSystemController.GetSystemMetrics(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
var
  MetricsJSON: TJSONObject;
  // Funciones placeholder para m�tricas. En un sistema real, se obtendr�an de APIs del SO o librer�as.
  function GetCurrentCPULoad: Double; begin Randomize; Result := 10.0 + Random * 25.0; end; // Simulado 10-35%
  function GetCurrentFreeMemoryMB: Int64; begin Randomize; Result := 512 + Random(2048); end; // Simulado 512MB - 2.5GB
begin
  MetricsJSON := TJSONObject.Create;
  try
    try
      MetricsJSON.AddPair('cpu_load_percent', GetCurrentCPULoad);
      MetricsJSON.AddPair('memory_free_mb', GetCurrentFreeMemoryMB);
      // Aqu� se podr�an a�adir m�s m�tricas: uso de disco, descriptores de archivo, etc.
      // Estas m�tricas son espec�ficas de la plataforma y pueden requerir c�digo condicional o librer�as.

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

