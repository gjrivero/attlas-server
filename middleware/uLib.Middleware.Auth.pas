unit uLib.Middleware.Auth;

interface

uses
  System.SysUtils, System.Classes, System.JSON,
  System.Types, System.Generics.Collections,
  IdCustomHTTPServer, System.SyncObjs,
  JOSE.Core.JWT, JOSE.Core.Builder, JOSE.Core.JWK, // Builder y JWK se usan para crear tokens, aquí para validar
  uLib.Logger, uLib.Server.Types; // Para EConfigurationError

type
  TAuthResult = (arSuccess, arInvalidToken, arExpiredToken, arNoTokenProvided, arConfigurationError, arPathExcluded, arAuthNotRequired, arError); // Added arError

  TExtractedClaimsData = record
    UserID: string;
    Username: string;
    UserRole: string;
    SessionID: string;
    class function CreateEmpty: TExtractedClaimsData; static;
  end;

  TAuthMiddleware = class
  private
    FMiddlewareSpecificConfig: TJSONObject; // Configuración específica para este middleware (ej. excludedPaths, tokenLookupSources)
    FJWTSecretKeyBytes: TBytes; // Clave secreta JWT en bytes, obtenida de AJWTSettingsConfig
    FJWTIssuer: string;         // Obtenido de AJWTSettingsConfig
    FJWTAudience: string;       // Obtenido de AJWTSettingsConfig
    FExcludedPaths: TArray<string>; // Cargado desde FMiddlewareSpecificConfig
    FAllowPublicAccessToOptions: Boolean; // Nueva opción para permitir OPTIONS sin autenticación

    function IsPathExcluded(const APath: string): Boolean;
    function ValidateTokenAndExtractClaims(const AToken: string; out AExtractedData: TExtractedClaimsData): TAuthResult;
    function ExtractTokenFromRequest(ARequest: TIdHTTPRequestInfo): string;
    procedure LogAuthenticationAttempt(ARequest: TIdHTTPRequestInfo; AResult: TAuthResult; const ATokenUsed: string = '');
    procedure LoadMiddlewareSpecificConfig; // Carga excludedPaths, tokenLookupSources desde FMiddlewareSpecificConfig

  public
    // AJWTSettingsConfig: Es la sección "security.jwt" del config.json global.
    // AMiddlewareConfig: Es la sección específica de este middleware, ej. "security.authMiddleware" del config.json.
    constructor Create(const AJWTSettingsConfig: TJSONObject; const AMiddlewareConfig: TJSONObject);
    destructor Destroy; override;

    function Authenticate(ARequest: TIdHTTPRequestInfo; AResponse: TIdHTTPResponseInfo; ARouteRequiresAuth: Boolean): Boolean;
  end;

implementation

uses
  System.Math,        // Para Max
  System.DateUtils,
  System.StrUtils,    // Para StartsText, SameText, Copy
  System.NetEncoding,    // Para TEncoding
  System.Rtti,        // Para TRttiEnumerationType
  uLib.Utils;

class function TExtractedClaimsData.CreateEmpty: TExtractedClaimsData;
begin
  Result.UserID := '';
  Result.Username := '';
  Result.UserRole := '';
  Result.SessionID := '';
end;

constructor TAuthMiddleware.Create(const AJWTSettingsConfig: TJSONObject; const AMiddlewareConfig: TJSONObject);
var
  SecretStr: string;
begin
  inherited Create;

  // 1. Clonar y procesar la configuración específica del middleware (excludedPaths, tokenLookupSources)
  if Assigned(AMiddlewareConfig) then
    FMiddlewareSpecificConfig := AMiddlewareConfig.Clone as TJSONObject
  else
    FMiddlewareSpecificConfig := TJSONObject.Create; // Crear uno vacío si no se provee

  LoadMiddlewareSpecificConfig; // Carga FExcludedPaths y otras configuraciones de FMiddlewareSpecificConfig

  // 2. Procesar la configuración JWT (secreto, issuer, audience) que se pasa explícitamente
  if Assigned(AJWTSettingsConfig) then
  begin
    SecretStr        := TJSONHelper.GetString(AJWTSettingsConfig, 'secret', '');
    FJWTIssuer       := TJSONHelper.GetString(AJWTSettingsConfig, 'issuer', ''); // Default a vacío si no se especifica
    FJWTAudience     := TJSONHelper.GetString(AJWTSettingsConfig, 'audience', ''); // Default a vacío

    if SecretStr.IsEmpty then
      LogMessage('AuthMiddleware.Create: CRITICAL - JWT Secret is empty in provided JWT settings. Token validation will FAIL.', logFatal)
    else if Length(SecretStr) < 32 then // HS256 requiere al menos 32 bytes (256 bits)
      LogMessage('AuthMiddleware.Create: SECURITY WARNING - JWT Secret is too short (<32 bytes). This is insecure.', logError);

    FJWTSecretKeyBytes := TEncoding.UTF8.GetBytes(SecretStr);
    LogMessage(Format('AuthMiddleware created. Issuer: "%s", Audience: "%s". Secret loaded (length: %d bytes). Excluded paths: %d. Public OPTIONS: %s.',
      [FJWTIssuer, FJWTAudience, Length(FJWTSecretKeyBytes), Length(FExcludedPaths), BoolToStr(FAllowPublicAccessToOptions, True)]), logInfo);
  end
  else
  begin
    LogMessage('AuthMiddleware.Create: CRITICAL - AJWTSettingsConfig (security.jwt) is nil. Token validation will FAIL.', logFatal);
    FJWTSecretKeyBytes := []; // Asegurar que esté vacío si no hay config
    FJWTIssuer := '';
    FJWTAudience := '';
  end;
  LogMessage('TAuthMiddleware instance configured.', logDebug);
end;

destructor TAuthMiddleware.Destroy;
begin
  LogMessage('TAuthMiddleware destroying...', logDebug);
  FreeAndNil(FMiddlewareSpecificConfig);
  inherited;
end;

procedure TAuthMiddleware.LoadMiddlewareSpecificConfig;
var
  ExcludedPathsNode: TJSONArray;
  I: Integer;
begin
  // Cargar rutas excluidas desde FMiddlewareSpecificConfig
  if Assigned(FMiddlewareSpecificConfig) and
     FMiddlewareSpecificConfig.TryGetValue('excludedPaths', ExcludedPathsNode) and // Corrected to TryGetValue
     Assigned(ExcludedPathsNode) then
  begin
    SetLength(FExcludedPaths, ExcludedPathsNode.Count);
    for I := 0 to ExcludedPathsNode.Count - 1 do
      if Assigned(ExcludedPathsNode.Items[I]) then // Check if item is assigned
        FExcludedPaths[I] := Trim(ExcludedPathsNode.Items[I].Value)
      else
        FExcludedPaths[I] := ''; // Handle nil item if necessary
    LogMessage(Format('AuthMiddleware: Loaded %d excluded paths from middleware config.', [Length(FExcludedPaths)]), logInfo);
  end
  else
  begin
    FExcludedPaths := [];
    LogMessage('AuthMiddleware: "excludedPaths" not found in middleware config or config is nil. No paths explicitly excluded by default.', logInfo);
  end;

  // Cargar opción para permitir OPTIONS sin autenticación
  if Assigned(FMiddlewareSpecificConfig) then
    FAllowPublicAccessToOptions := TJSONHelper.GetBoolean(FMiddlewareSpecificConfig, 'allowPublicAccessToOptionsMethod', False)
  else
    FAllowPublicAccessToOptions := False; // Default si no hay config específica del middleware
end;

function TAuthMiddleware.IsPathExcluded(const APath: string): Boolean;
var
  TrimmedPath: string;
  ExcludedPathPattern: string;
begin
  Result := False;
  // FExcludedPaths es TArray<string>, no necesita check de Assigned si siempre se inicializa (ej. a vacío)
  // if not Assigned(FExcludedPaths) then Exit; // No es necesario si FExcludedPaths siempre está asignado

  TrimmedPath := APath.Trim.ToLower;

  for ExcludedPathPattern in FExcludedPaths do
  begin
    if (ExcludedPathPattern <> '') and TrimmedPath.StartsWith(ExcludedPathPattern.Trim.ToLower) then
    begin
      Result := True;
      Exit;
    end;
  end;
end;

function TAuthMiddleware.ExtractTokenFromRequest(ARequest: TIdHTTPRequestInfo): string;
const
  BearerPrefix = 'Bearer ';
var
  AuthHeader: string;
  TokenLookupSourcesConfig: TJSONArray;
  SourceDescValue: TJSONValue;
  SourceDesc: string;
  SourceParts: TArray<string>;
  LookupType, HeaderName, Prefix, QueryParamName: string;
  I: Integer;
begin
  Result := '';
  if not Assigned(ARequest) then Exit; // Safety check

  AuthHeader := ARequest.CustomHeaders.Values['Authorization'];
  if System.StrUtils.StartsText(BearerPrefix, AuthHeader) then
  begin
    Result := System.Copy(AuthHeader, Length(BearerPrefix) + 1, MaxInt); // Use System.Copy
    if Result <> '' then Exit;
  end;

  if Assigned(FMiddlewareSpecificConfig) and
     FMiddlewareSpecificConfig.TryGetValue('tokenLookupSources', TokenLookupSourcesConfig) and
     Assigned(TokenLookupSourcesConfig) then
  begin
    for I := 0 to TokenLookupSourcesConfig.Count - 1 do
    begin
      SourceDescValue := TokenLookupSourcesConfig.Items[I];
      if not Assigned(SourceDescValue) or not (SourceDescValue is TJSONString) then Continue;

      SourceDesc := (SourceDescValue as TJSONString).Value;
      SourceParts := SourceDesc.Split([':']);

      if Length(SourceParts) > 0 then
      begin
        LookupType := SourceParts[0].ToLower.Trim;
        if (LookupType = 'header') and (Length(SourceParts) >= 2) then
        begin
          HeaderName := SourceParts[1].Trim;
          Prefix := '';
          if (Length(SourceParts) >= 3) then
             Prefix := SourceParts[2].Trim + ' ';

          AuthHeader := ARequest.CustomHeaders.Values[HeaderName];
          if (Prefix = '') or System.StrUtils.StartsText(Prefix, AuthHeader) then
          begin
            Result := System.Copy(AuthHeader, Length(Prefix) + 1, MaxInt);
            if Result <> '' then Exit;
          end;
        end
        else if (LookupType = 'queryparam') and (Length(SourceParts) >= 2) then
        begin
          QueryParamName := SourceParts[1].Trim;
          Result := ARequest.Params.Values[QueryParamName];
          if Result <> '' then Exit;
        end;
      end;
    end;
  end;
end;

function TAuthMiddleware.ValidateTokenAndExtractClaims(const AToken: string; out AExtractedData: TExtractedClaimsData): TAuthResult;
var
  JWT: TJWT;
  NowTime: TDateTime;
  ClaimsRef: TJWTClaims; // Referencia temporal a los claims internos del TJWT
begin
  // Inicializar valores de salida y resultado por defecto
  AExtractedData := TExtractedClaimsData.CreateEmpty;
  Result := arConfigurationError; // Default as per original logic if FJWTSecretKeyBytes is empty

  if Length(FJWTSecretKeyBytes) = 0 then
  begin
    LogMessage('AuthMiddleware.ValidateToken: JWT Secret Key is not configured or is empty. Cannot validate tokens.', logError);
    Exit; // Result ya es arConfigurationError
  end;

  JWT := nil; // Importante inicializar a nil antes del bloque try..finally
  NowTime := NowUTC;
  ClaimsRef := nil;

  try // Bloque try..except externo para manejar excepciones generales y de JOSE
    try // Bloque try..finally interno para asegurar la liberación de JWT
      JWT := TJOSE.DeserializeCompact(FJWTSecretKeyBytes, AToken);

      // A partir de aquí, si JWT es nil (aunque DeserializeCompact suele lanzar excepción en fallo),
      // o si algo más falla, el finally se ejecutará.

      if not Assigned(JWT) then // Aunque DeserializeCompact debería lanzar excepción si falla
      begin
        LogMessage(Format('AuthMiddleware: Token %.10s... deserialization failed, JWT object is nil (unexpected).', [System.Copy(AToken,1,10)]), logError);
        Result := arInvalidToken;
        Exit; // Salir de la función
      end;

      ClaimsRef := JWT.Claims;

      if not Assigned(ClaimsRef) then
      begin
        Result := arInvalidToken;
        LogMessage(Format('AuthMiddleware: Token %.10s... deserialized but has no claims object.', [System.Copy(AToken,1,10)]), logWarning);
        Exit; // Salir de la función
      end;

      if (ClaimsRef.Expiration < NowTime) then
      begin
        Result := arExpiredToken;
        LogMessage(Format('AuthMiddleware: Token %.10s... EXPIRED (Exp: %s, NowUTC: %s)',
          [System.Copy(AToken,1,10), DateToISO8601(ClaimsRef.Expiration), DateToISO8601(NowTime)]), logWarning);
        Exit; // Salir de la función
      end;

      if (ClaimsRef.NotBefore > 0) and (ClaimsRef.NotBefore > NowTime) then
      begin
         Result := arInvalidToken;
         LogMessage(Format('AuthMiddleware: Token %.10s... NOT YET VALID (NBF: %s, NowUTC: %s)',
           [System.Copy(AToken,1,10), DateToISO8601(ClaimsRef.NotBefore), DateToISO8601(NowTime)]), logWarning);
         Exit; // Salir de la función
      end;

      if (FJWTIssuer <> '') and (not SameText(ClaimsRef.Issuer, FJWTIssuer)) then
      begin
         Result := arInvalidToken;
         LogMessage(Format('AuthMiddleware: Token %.10s... INVALID ISSUER (Expected: "%s", Got: "%s")',
           [System.Copy(AToken,1,10), FJWTIssuer, ClaimsRef.Issuer]), logWarning);
         Exit; // Salir de la función
      end;

      // Lógica de Audiencia (simplificada, ajustar a la API real de JOSE si es necesario)
      if (FJWTAudience <> '') then
      begin
        var AudienceMatch := False;
        var AudienceValueInToken := ''; // Para logging

        if ClaimsRef.HasAudience then // Asumir que esto existe y es fiable
        begin
          // Intenta leer 'aud' como string simple
          if ClaimsRef.Audience <> '' then // Asumiendo que TJWTClaims.Audience devuelve el string si es único
          begin
            AudienceValueInToken := ClaimsRef.Audience;
            if SameText(ClaimsRef.Audience, FJWTAudience) then
              AudienceMatch := True;
          end;

          // Si no hubo coincidencia y puede ser un array (necesitarías verificar cómo tu lib JOSE maneja esto)
          // El código original usaba ClaimsRef.AudienceArray, así que lo mantenemos si es válido.
          if not AudienceMatch and Assigned(ClaimsRef.AudienceArray) and (Length(ClaimsRef.AudienceArray) > 0) then
          begin
            if AudienceValueInToken = '' then AudienceValueInToken := '[Array]'; // Placeholder para log
            for var audItem in ClaimsRef.AudienceArray do
            begin
              if SameText(audItem, FJWTAudience) then
              begin
                AudienceMatch := True;
                AudienceValueInToken := audItem; // Guardar el que coincidió para log
                Break;
              end;
            end;
          end
          else if AudienceValueInToken = '' then // Si no había string simple ni array
             AudienceValueInToken := '{No Audience Claim Found}';
        end
        else
           AudienceValueInToken := '{No Audience Claim Present}';


        if not AudienceMatch then
        begin
           Result := arInvalidToken;
           LogMessage(Format('AuthMiddleware: Token %.10s... INVALID AUDIENCE (Expected: "%s", Got: "%s")',
             [System.Copy(AToken,1,10), FJWTAudience, AudienceValueInToken]), logWarning);
           Exit; // Salir de la función
        end;
      end; // Fin if (FJWTAudience <> '')

      // Si todas las validaciones pasan, extraer los datos necesarios
      AExtractedData.UserID    := ClaimsRef.Subject;
      AExtractedData.Username  := ClaimsRef.JSON.GetValue<string>('username', '');
      AExtractedData.UserRole  := ClaimsRef.JSON.GetValue<string>('role', '');
      AExtractedData.SessionID := ClaimsRef.JSON.GetValue<string>('sid', '');

      Result := arSuccess;
      LogMessage(Format('AuthMiddleware: Token %.10s... validated successfully. User: %s', [System.Copy(AToken,1,10), AExtractedData.UserID]), logDebug);

    finally // Este finally corresponde al try..finally interno
      FreeAndNil(JWT); // Asegura que JWT se libere siempre, incluso si se usa Exit.
    end;
  except // Este except corresponde al try..except externo
    on E: Exception do
    begin
      LogMessage(Format('AuthMiddleware: Unexpected exception during token validation for %.10s... : %s - %s',
        [System.Copy(AToken,1,10), E.ClassName, E.Message]), logError);
      Result := arError;
      // AExtractedData ya fue inicializado a vacío al principio.
    end;
  end;
end;

procedure TAuthMiddleware.LogAuthenticationAttempt( ARequest: TIdHTTPRequestInfo;
                                                    AResult: TAuthResult;
                                                    const ATokenUsed: string = '');
const
  AUTH_RESULT_STR: array[TAuthResult] of string = (
    'Success', 'Invalid Token', 'Expired Token', 'No Token Provided',
    'Configuration Error', 'Path Excluded', 'Auth Not Required', 'Internal Error'
  );
var
  Loglevel: TLogLevel;
  ClientIP: string;
  Path: string;
  MsgBuilder: TStringBuilder;
begin
  ClientIP := IfThen(Assigned(ARequest), ARequest.RemoteIP, 'N/A');
  Path := IfThen(Assigned(ARequest), ARequest.Document, 'N/A');

  case AResult of
    arSuccess, arPathExcluded, arAuthNotRequired: Loglevel := logInfo;
    arNoTokenProvided: Loglevel := logDebug;
  else
    Loglevel := logWarning;
  end;

  MsgBuilder := TStringBuilder.Create;
  try
    MsgBuilder.AppendFormat('Auth attempt for [%s] from IP [%s]: %s', [Path, ClientIP, AUTH_RESULT_STR[AResult]]);
    if (AResult = arSuccess) and Assigned(ARequest) and (ARequest.Params.Values['user_id'] <> '') then // Check ARequest before Params
       MsgBuilder.AppendFormat(' (User ID: %s)', [ARequest.Params.Values['user_id']])
    else if ATokenUsed <> '' then
       MsgBuilder.AppendFormat(' (Token: %.10s...)', [System.Copy(ATokenUsed,1,10)]);

    LogMessage(MsgBuilder.ToString, Loglevel);
  finally
    MsgBuilder.Free;
  end;
end;

function TAuthMiddleware.Authenticate(ARequest: TIdHTTPRequestInfo; AResponse: TIdHTTPResponseInfo; ARouteRequiresAuth: Boolean): Boolean;
var
  TokenValue: string;
  ExtractedData: TExtractedClaimsData; // Usar el nuevo record
  AuthenticationResult: TAuthResult;
  IsPublicOptionsRequest: Boolean;
begin
  Result := True;
  AuthenticationResult := arAuthNotRequired;

  if not Assigned(ARequest) or not Assigned(AResponse) then
  begin
    LogMessage('AuthMiddleware.Authenticate: ARequest or AResponse is nil. Aborting authentication.', logError);
    Result := False;
    Exit;
  end;

  try
    IsPublicOptionsRequest := FAllowPublicAccessToOptions and SameText(ARequest.Command, 'OPTIONS');

    if IsPublicOptionsRequest then
    begin
      LogAuthenticationAttempt(ARequest, arPathExcluded);
      Exit;
    end;

    if IsPathExcluded(ARequest.Document) then
    begin
      LogAuthenticationAttempt(ARequest, arPathExcluded);
      Exit;
    end;

    if not ARouteRequiresAuth then
    begin
      LogAuthenticationAttempt(ARequest, arAuthNotRequired);
      Exit;
    end;

    TokenValue := ExtractTokenFromRequest(ARequest);

    if TokenValue.IsEmpty then
    begin
      AuthenticationResult := arNoTokenProvided;
      Result := False;
    end
    else
    begin
      // Llamar a la función modificada
      AuthenticationResult := ValidateTokenAndExtractClaims(TokenValue, ExtractedData);
      Result := (AuthenticationResult = arSuccess);

      if Result then // Token es válido y los datos se extrajeron a ExtractedData
      begin
        // Poblar ARequest.Params con la información de ExtractedData
        ARequest.Params.Values['user_id']    := ExtractedData.UserID;
        ARequest.Params.Values['username']   := ExtractedData.Username;
        ARequest.Params.Values['user_role']  := ExtractedData.UserRole;
        ARequest.Params.Values['session_id'] := ExtractedData.SessionID;
      end;
      // Si Result es False, ExtractedData contendrá valores vacíos.
    end;

    if not Result then
    begin
      AResponse.ResponseNo := 401;
      AResponse.ContentType := 'application/json';
      case AuthenticationResult of
        arInvalidToken: AResponse.ContentText := '{"success":false, "error":"Invalid or malformed authentication token"}';
        arExpiredToken: AResponse.ContentText := '{"success":false, "error":"Authentication token has expired"}';
        arNoTokenProvided: AResponse.ContentText := '{"success":false, "error":"Authentication token is required"}';
        arConfigurationError: AResponse.ContentText := '{"success":false, "error":"Authentication configuration error on server"}';
      else // arError o un estado inesperado
        AResponse.ContentText := '{"success":false, "error":"Authentication failed"}';
      end;
    end;

    LogAuthenticationAttempt(ARequest, AuthenticationResult, TokenValue);
  except
    on E: Exception do
    begin
      LogMessage(Format('AuthMiddleware.Authenticate: Unhandled exception: %s - %s', [E.ClassName, E.Message]), logCritical);
      Result := False;
      if Assigned(AResponse) and (AResponse.ResponseNo < 400) and (not AResponse.HeaderHasBeenWritten) then
      begin
        AResponse.ResponseNo := 500;
        AResponse.ContentType := 'application/json';
        AResponse.ContentText := '{"success":false, "error":"Internal server error during authentication."}';
      end;
    end;
  end;
end;

end.
