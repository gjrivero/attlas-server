unit uController.System;

interface

uses
  System.SysUtils, System.JSON, System.Generics.Collections, System.Rtti,
  System.DateUtils, System.Hash, System.NetEncoding, System.Classes,
  {$IFDEF MSWINDOWS}
  Winapi.Windows,
  {$ENDIF}
  {$IFDEF LINUX}
  System.IOUtils,
  {$ENDIF}
  Data.DB,
  FireDAC.Stan.Param,
  IdCustomHTTPServer,
  JOSE.Core.JWT, JOSE.Core.Builder, JOSE.Core.JWK,
  uLib.Controller.Base,
  uLib.Routes,
  uLib.Database.Pool,
  uLib.Database.Types,
  uLib.Database.Connection,
  uLib.Logger,
  uLib.Server.Types,
  uLib.Config.Manager,
  uLib.Session.Manager,
  uLib.Server.Manager,
  uLib.Utils;

type
  TSystemController = class(TBaseController)
  private
    class var
      LastIdle, LastKernel, LastUser: UInt64;
      LastCheck: Cardinal;
      LastTotalCPU, LastIdleCPU: UInt64; // Para Linux
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
  SYSTEM_CONTROLLER_DB_POOL_NAME = 'MainDB_PG';
  SQL_VALIDATE_USER =
    'SELECT cl.id, lu.user_name, r.name as role, lu.password_hash, lu.password_salt, ' +
    '       cl.email, cl.first_name as fullName, cl.last_name, cl.phone, ' +
    '       lu.is_active, lu.is_verified ' +
    '  FROM login_users lu' +
    '       INNER JOIN clients cl ON (lu.client_id = cl.id)' +
    '       INNER JOIN roles r ON (lu.role_id = r.id)' +
    ' WHERE (LOWER(lu.user_name) = LOWER(:username)) AND lu.is_active = true';

implementation

{$IFDEF MSWINDOWS}
// Declaración de funciones de Windows API si no están disponibles

function GetSystemTimes(lpIdleTime, lpKernelTime, lpUserTime: PFileTime): BOOL; stdcall;
  external kernel32;

function GlobalMemoryStatusEx(var lpBuffer: TMemoryStatusEx): BOOL; stdcall;
  external kernel32;
(*
type
  TMemoryStatusEx = record
    dwLength: DWORD;
    dwMemoryLoad: DWORD;
    ullTotalPhys: UInt64;
    ullAvailPhys: UInt64;
    ullTotalPageFile: UInt64;
    ullAvailPageFile: UInt64;
    ullTotalVirtual: UInt64;
    ullAvailVirtual: UInt64;
    ullAvailExtendedVirtual: UInt64;
  end;
*)
{$ENDIF}

{ TPasswordHashingLib }

class function TPasswordHashingLib.VerifyPassword(const APassword, AStoredHash, AStoredSalt: string): Boolean;
var
  AttemptedHash: string;
begin
  // ESTA ES UNA IMPLEMENTACIÓN DE EJEMPLO MUY SIMPLIFICADA Y NO SEGURA.
  // DEBES USAR UNA LIBRERÍA REAL PARA BCRYPT, SCRYPT O ARGON2.
  LogMessage('[TPasswordHashingLib.VerifyPassword] Placeholder: Comparando contraseña proporcionada con hash almacenado usando sal. ESTO DEBE SER REEMPLAZADO CON UNA LIBRERÍA SEGURA.', logCritical);

  // Simulación de fallo/éxito para permitir que el flujo continúe.
  if (AStoredHash <> '') and (APassword = 'testpassword') then
    Result := True
  else
    Result := False;

  if not Result then
     LogMessage(Format('[TPasswordHashingLib.VerifyPassword] Placeholder: La contraseña "%s" no coincide con el hash almacenado "%s" usando la sal "%s".',
       [APassword, Copy(AStoredHash,1,10)+'...', Copy(AStoredSalt,1,5)+'...']), logDebug);
end;

{ TSystemController }

class procedure TSystemController.RegisterRoutes;
begin
  if not Assigned(FRouteManager) then
  begin
    LogMessage('CRITICAL: FRouteManager not assigned in TSystemController. Routes will not be registered.', logError);
    Exit;
  end;

  FRouteManager.AddRoute('POST', 'login', Login, False);
  FRouteManager.AddRoute('GET',  'status', GetSystemStatus, False);
  FRouteManager.AddRoute('POST', 'logout', Logout, True);
  FRouteManager.AddRoute('GET',  'metrics', GetSystemMetrics, True);

  LogMessage('TSystemController routes registered.', logInfo);
end;

class procedure TSystemController.ValidateLoginCredentials(
  const CredentialsJSON: TJSONObject; out Username, PasswordOut: string);
begin
  if not Assigned(CredentialsJSON) then
    raise EMissingParameterException.Create('Missing credentials in request body.');

  Username := TJSONHelper.GetString(CredentialsJSON, 'username', '').Trim;
  PasswordOut := TJSONHelper.GetString(CredentialsJSON, 'password', '');

  if Username.IsEmpty then
    raise EMissingParameterException.Create('Username is required.');

  if PasswordOut = '' then
    raise EMissingParameterException.Create('Password is required.');

  LogMessage('Login credentials syntax validated (username and password presence).', logDebug);
end;

class procedure TSystemController.Login(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
var
  CredentialsJSONValue: TJSONValue;
  CredentialsJSON: TJSONObject;
  Username, Password, UserID, UserRole, FullName, Email: string;
  StoredPasswordHash, StoredPasswordSalt: string;
  DBConn: IDBConnection;
  Dataset: TDataSet;
  Session: TSessionData;
  Params: TFDParams;
  JWT: TJWT;
  TokenString: string;
  TokenExpiryHours: Integer;
  JWTSecretFromConfig, JWTIssuerFromConfig, JWTAudienceFromConfig: string;
  RespJSON: TJSONObject;
  ConfigMgr: TConfigManager;
  JWTSettingsJSON: TJSONObject;
  LoginUserIsActive: Boolean;
  UserInfoJSON: TJSONObject;
begin
  CredentialsJSONValue := nil;
  CredentialsJSON := nil;
  DBConn := nil;
  Dataset := nil;
  JWT := nil;
  RespJSON := nil;
  JWTSettingsJSON := nil;
  UserInfoJSON := nil;

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

    if JWTSecretFromConfig.IsEmpty or (Length(JWTSecretFromConfig) < 32) then
    begin
      LogMessage('CRITICAL SECURITY WARNING: JWT_SECRET_KEY is missing, empty, or too short! Must be at least 32 random bytes.', logFatal);
      raise EConfigurationError.Create('JWT secret key is insecure or not configured.');
    end;

    CredentialsJSONValue := GetRequestBody(Request);
    if not (Assigned(CredentialsJSONValue) and (CredentialsJSONValue is TJSONObject)) then
      raise EInvalidRequestException.Create('Request body must be a valid JSON object for login.');

    CredentialsJSON := CredentialsJSONValue as TJSONObject;
    ValidateLoginCredentials(CredentialsJSON, Username, Password);

    // Obtener hash y salt almacenados del usuario
    Params := TFDParams.Create;
    try
      Params.Add('username', LowerCase(Username));
      DBConn := AcquireDBConnection(SYSTEM_CONTROLLER_DB_POOL_NAME);
      Dataset := DBConn.ExecuteReader(SQL_VALIDATE_USER, Params);

      try
        if Dataset.IsEmpty then
        begin
          LogMessage(Format('Login failed for user: %s. User not found or not active.', [Username]), logWarning);
          raise EUnauthorizedException.Create('Invalid username or password.');
        end;

        StoredPasswordHash := Dataset.FieldByName('password_hash').AsString;
        StoredPasswordSalt := Dataset.FieldByName('password_salt').AsString;
        LoginUserIsActive  := Dataset.FieldByName('is_active').AsBoolean;

        if not LoginUserIsActive then
        begin
          LogMessage(Format('Login failed for user: %s. User account is not active.', [Username]), logWarning);
          raise EUnauthorizedException.Create('User account is not active.');
        end;

        // Verificar contraseña
        if not TPasswordHashingLib.VerifyPassword(Password, StoredPasswordHash, StoredPasswordSalt) then
        begin
          LogMessage(Format('Login failed for user: %s. Password mismatch.', [Username]), logWarning);
          raise EUnauthorizedException.Create('Invalid username or password.');
        end;

        UserID     := Dataset.FieldByName('id').AsString;
        UserRole   := Dataset.FieldByName('role').AsString;
        FullName   := Dataset.FieldByName('fullName').AsString;
        Email      := Dataset.FieldByName('email').AsString;

        LogMessage(Format('User %s (ID: %s, Role: %s) successfully authenticated.', [Username, UserID, UserRole]), logInfo);
      finally
        FreeAndNil(Dataset);
      end;
    finally
      FreeAndNil(Params);
    end;

    // Crear sesión y JWT
    Session := TSessionManager.GetInstance.CreateNewSession;
    Session.SetValue('user_id', UserID);
    Session.SetValue('username', Username);
    Session.SetValue('user_role', UserRole);
    LogMessage(Format('Server session %s created for user %s.', [Session.ID, Username]), logInfo);

    JWT := TJWT.Create;
    JWT.Claims.Issuer     := JWTIssuerFromConfig;
    JWT.Claims.Audience   := JWTAudienceFromConfig;
    JWT.Claims.Subject    := UserID;
    JWT.Claims.IssuedAt   := NowUTC;
    JWT.Claims.Expiration := IncHour(NowUTC, TokenExpiryHours);
    JWT.Claims.SetClaim('username', Username);
    JWT.Claims.SetClaim('role', UserRole);
    JWT.Claims.SetClaim('sid', Session.ID);

    TokenString := TJOSE.SHA256CompactToken(JWTSecretFromConfig, JWT);

    RespJSON := TJSONObject.Create;
    UserInfoJSON := TJSONObject.Create;
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
    FreeAndNil(RespJSON);
    ReleaseDBConnection(DBConn, SYSTEM_CONTROLLER_DB_POOL_NAME);
  end;
end;

class procedure TSystemController.Logout(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
var
  SessionIDFromClaim: string;
begin
  try
    SessionIDFromClaim := Request.Params.Values['session_id'];
    if SessionIDFromClaim <> '' then
    begin
      TSessionManager.GetInstance.InvalidateSession(SessionIDFromClaim);
      LogMessage(Format('User server session %s invalidated due to logout request.', [SessionIDFromClaim]), logInfo);
    end
    else
      LogMessage('Logout: No server session ID found in request context. Client is responsible for discarding JWT.', logDebug);

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
    AppSectionFromConfig := TJSONHelper.GetJSONObject(ConfigMgr.ConfigData,'application');

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
      LServerBaseStatsJSON := LServerManager.ServerInstance.GetServerStats;
      if Assigned(LServerBaseStatsJSON) then
      begin
        StatusJSON.AddPair('web_server_stats', LServerBaseStatsJSON);
        LServerBaseStatsJSON := nil;
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
        LDBPoolsStatsArray := LDBPoolManager.GetAllPoolsStats;
        StatusJSON.AddPair('database_pools', LDBPoolsStatsArray);
        LDBPoolsStatsArray := nil;
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
    FreeAndNil(StatusJSON);
  end;
end;

class procedure TSystemController.GetSystemMetrics(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
var
  MetricsJSON: TJSONObject;

  function GetCurrentCPULoad: Double;
  {$IFDEF MSWINDOWS}
  var
    IdleTime, KernelTime, UserTime: TFileTime;
    SysIdle, SysKernel, SysUser: UInt64;
    IdleDiff, KernelDiff, UserDiff, Total: UInt64;
  begin
    Result := -1;
    if GetSystemTimes(@IdleTime, @KernelTime, @UserTime) then
    begin
      SysIdle := UInt64(IdleTime.dwLowDateTime) or (UInt64(IdleTime.dwHighDateTime) shl 32);
      SysKernel := UInt64(KernelTime.dwLowDateTime) or (UInt64(KernelTime.dwHighDateTime) shl 32);
      SysUser := UInt64(UserTime.dwLowDateTime) or (UInt64(UserTime.dwHighDateTime) shl 32);

      // CORRECCIÓN: Inicializar en primera llamada
      if LastCheck = 0 then
      begin
        LastIdle := SysIdle;
        LastKernel := SysKernel;
        LastUser := SysUser;
        LastCheck := GetTickCount;
        Result := 0.0; // Primera medición
        Exit;
      end;

      IdleDiff := SysIdle - LastIdle;
      KernelDiff := SysKernel - LastKernel;
      UserDiff := SysUser - LastUser;
      Total := KernelDiff + UserDiff;

      if Total > 0 then
        Result := 100.0 * (1.0 - (IdleDiff / Total))
      else
        Result := 0.0;

      LastIdle := SysIdle;
      LastKernel := SysKernel;
      LastUser := SysUser;
      LastCheck := GetTickCount;
    end;
  end;
  {$ELSEIF Defined(LINUX)}
  var
    StatFile: TextFile;
    Line: string;
    Values: TArray<string>;
    Idle, Total: UInt64;
    IdleDiff, TotalDiff: UInt64;
    i: Integer;

  class var
    LastIdle, LastTotal: UInt64;
  begin
    Result := -1;
    AssignFile(StatFile, '/proc/stat');
    try
      Reset(StatFile);
      ReadLn(StatFile, Line);
      CloseFile(StatFile);

      Values := Line.Split([' '], TStringSplitOptions.ExcludeEmpty);
      if Length(Values) >= 5 then
      begin
        Idle := StrToUInt64Def(Values[4], 0);
        Total := 0;
        for i := 1 to High(Values) do
          Total := Total + StrToUInt64Def(Values[i], 0);

        if LastTotal <> 0 then
        begin
          IdleDiff := Idle - LastIdle;
          TotalDiff := Total - LastTotal;
          if TotalDiff > 0 then
            Result := 100.0 * (1.0 - (IdleDiff / TotalDiff))
          else
            Result := 0.0;
        end
        else
          Result := 0.0;

        LastIdle := Idle;
        LastTotal := Total;
      end;
    except
      Result := -1;
    end;
  end;
  {$ELSE}
  begin
    Result := -1; // Plataforma no soportada
  end;
  {$ENDIF}

  function GetCurrentFreeMemoryMB: Int64;
  {$IFDEF MSWINDOWS}
  var
    MemStatus: TMemoryStatusEx;
  begin
    MemStatus.dwLength := SizeOf(MemStatus);
    if GlobalMemoryStatusEx(MemStatus) then
      Result := MemStatus.ullAvailPhys div (1024 * 1024)
    else
      Result := -1;
  end;
  {$ELSEIF Defined(LINUX)}
  var
    MemInfo: TStringList;
    Line: string;
    Parts: TArray<string>;
  begin
    Result := -1;
    MemInfo := TStringList.Create;
    try
      MemInfo.LoadFromFile('/proc/meminfo');
      for Line in MemInfo do
      begin
        if Line.StartsWith('MemAvailable:') then
        begin
          Parts := Line.Split([' '], TStringSplitOptions.ExcludeEmpty);
          if Length(Parts) >= 2 then
          begin
            Result := StrToInt64Def(Parts[1], 0) div 1024; // kB a MB
            Break;
          end;
        end;
      end;
    finally
      MemInfo.Free;
    end;
  end;
  {$ELSE}
  begin
    Result := -1; // Plataforma no soportada
  end;
  {$ENDIF}

begin
  MetricsJSON := TJSONObject.Create;
  try
    try
      MetricsJSON.AddPair('cpu_load_percent', TJSONNumber.Create(GetCurrentCPULoad));
      MetricsJSON.AddPair('memory_free_mb', TJSONNumber.Create(GetCurrentFreeMemoryMB));

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
