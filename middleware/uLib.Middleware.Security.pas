unit uLib.Middleware.Security;

interface

uses
  System.SysUtils, System.Classes, System.Hash, System.JSON,
  System.SyncObjs, System.Generics.Collections, System.RegularExpressions, System.Threading, // Added System.Threading
  IdCustomHTTPServer, IdGlobal, // IdGlobal para TEncoding
  uLib.Session.Manager, // Para TCSRFProtection
  uLib.Logger,         // Para LogMessage
  uLib.Server.Types;   // Para EConfigurationError

type
  TSecurityHeaders = class
  private
    FCSP: string;
    FFrameOptions: string;
    FXSSProtection: string;
    FHSTS: string;
    FContentTypeOptions: string;
    FReferrerPolicy: string;
    FPermissionsPolicy: string;
    FXDownloadOptions: string;
    FXDNSPrefetchControl: string;
  public
    constructor Create;
    procedure LoadFromConfig(AConfigSection: TJSONObject);
    procedure ApplyToResponse(AResponse: TIdHTTPResponseInfo; AIsServerSSLEnabled: Boolean);
  end;

  TRateLimitEntry = record
    LastRequestTimeUTC: TDateTime;
    RequestCount: Integer;
    BlockedUntilUTC: TDateTime;
  end;

  TRateLimiter = class
  private
    FRequests: TDictionary<string, TRateLimitEntry>;
    FLock: TCriticalSection;
    FMaxRequestsPerWindow: Integer;
    FWindowSeconds: Integer;
    FBurstLimit: Integer;
    FBlockDurationMinutes: Integer;
    FCleanupThread: TThread;
    FStopCleanupEvent: TEvent; // Event to signal cleanup thread to stop

    procedure CleanupOldEntriesProc; // Renamed and signature changed for TThread
    procedure DoCleanupOldEntries;   // Actual cleanup logic
  public
    constructor Create(AConfig: TJSONObject);
    destructor Destroy; override;
    function IsRequestRateLimited(const AIPAddress: string; AResponse: TIdHTTPResponseInfo): Boolean;
    procedure ResetLimitForIP(const AIPAddress: string);
  end;

  TCSRFProtection = class
  private
    FSessionManager: TSessionManager;
    FTokenHeaderName: string;
    FTokenFormFieldName: string;
    FTokenSessionKey: string;
    FProtectedMethods: TArray<string>;

    function GenerateCSRFToken(const ASessionID: string): string;
    function GetCSRFTokenFromRequest(ARequest: TIdHTTPRequestInfo): string;
  public
    constructor Create(ASessionManager: TSessionManager; AConfig: TJSONObject);
    function ValidateCSRFToken(ARequest: TIdHTTPRequestInfo; AResponse: TIdHTTPResponseInfo; AIsUserAuthenticated: Boolean): Boolean;
    procedure AddOrRefreshTokenInResponse(ARequest: TIdHTTPRequestInfo; AResponse: TIdHTTPResponseInfo);
  end;

  TSecurityMiddleware = class
  private
    FMiddlewareConfig: TJSONObject;
    FRateLimiterInstance: TRateLimiter;
    FCSRFProtectionInstance: TCSRFProtection;
    FSecurityHeadersInstance: TSecurityHeaders;
    FIsServerSSLEnabled: Boolean;

    function IsRequestUserAuthenticated(ARequest: TIdHTTPRequestInfo): Boolean;
    function ShouldCSRFProtectRequest(ARequest: TIdHTTPRequestInfo): Boolean;
  public
    constructor Create(AConfigJSONObject: TJSONObject);
    destructor Destroy; override;
    function ValidateRequest(ARequest: TIdHTTPRequestInfo; AResponse: TIdHTTPResponseInfo): Boolean;
  end;

implementation

uses
  System.DateUtils, System.NetEncoding, // System.Threading already in interface
  System.Rtti, // For TRttiEnumerationType if needed, not directly used here but good for consistency
  System.StrUtils, // For IfThen, SameText, Copy
  uLib.Config.Manager,

  uLib.Utils;

{ TSecurityHeaders }
constructor TSecurityHeaders.Create;
begin
  inherited Create;
  FCSP := 'default-src ''self''; script-src ''self''; style-src ''self'' ''unsafe-inline''; img-src ''self'' data:; object-src ''none''; frame-ancestors ''none'';';
  FFrameOptions := 'SAMEORIGIN';
  FXSSProtection := '1; mode=block';
  FHSTS := 'max-age=31536000; includeSubDomains'; // WARNING: Only if site is ALWAYS HTTPS
  FContentTypeOptions := 'nosniff';
  FReferrerPolicy := 'strict-origin-when-cross-origin';
  FPermissionsPolicy := 'geolocation=(), microphone=(), camera=()';
  FXDownloadOptions := 'noopen';
  FXDNSPrefetchControl := 'off';
  LogMessage('TSecurityHeaders created with default values.', logDebug);
end;

procedure TSecurityHeaders.LoadFromConfig(AConfigSection: TJSONObject);
begin
  if not Assigned(AConfigSection) then Exit;
  LogMessage('TSecurityHeaders: Loading configuration...', logDebug);
  FCSP                  := TJSONHelper.GetString(AConfigSection, 'contentSecurityPolicy', FCSP);
  FFrameOptions         := TJSONHelper.GetString(AConfigSection, 'xFrameOptions', FFrameOptions);
  FXSSProtection        := TJSONHelper.GetString(AConfigSection, 'xXSSProtection', FXSSProtection);
  FHSTS                 := TJSONHelper.GetString(AConfigSection, 'strictTransportSecurity', FHSTS);
  FContentTypeOptions   := TJSONHelper.GetString(AConfigSection, 'xContentTypeOptions', FContentTypeOptions);
  FReferrerPolicy       := TJSONHelper.GetString(AConfigSection, 'referrerPolicy', FReferrerPolicy);
  FPermissionsPolicy    := TJSONHelper.GetString(AConfigSection, 'permissionsPolicy', FPermissionsPolicy);
  FXDownloadOptions     := TJSONHelper.GetString(AConfigSection, 'xDownloadOptions', FXDownloadOptions);
  FXDNSPrefetchControl  := TJSONHelper.GetString(AConfigSection, 'xDNSPrefetchControl', FXDNSPrefetchControl);
  LogMessage('TSecurityHeaders configuration loaded.', logInfo);
end;

procedure TSecurityHeaders.ApplyToResponse(AResponse: TIdHTTPResponseInfo; AIsServerSSLEnabled: Boolean);
begin
  if not Assigned(AResponse) then Exit;

  if FCSP <> '' then AResponse.CustomHeaders.Values['Content-Security-Policy'] := FCSP;
  if FFrameOptions <> '' then AResponse.CustomHeaders.Values['X-Frame-Options'] := FFrameOptions;
  if FXSSProtection <> '' then AResponse.CustomHeaders.Values['X-XSS-Protection'] := FXSSProtection;
  if AIsServerSSLEnabled then // Solo aplicar HSTS si SSL está realmente habilitado en el servidor
  begin
    if FHSTS <> '' then
      AResponse.CustomHeaders.Values['Strict-Transport-Security'] := FHSTS;
  end
  else if FHSTS <> '' then // Si HSTS está configurado pero SSL no está activo en el servidor
  begin
    LogMessage('TSecurityHeaders: HSTS is configured but server SSL is disabled. HSTS header will NOT be sent.', logWarning);
    // Opcionalmente, se podría remover si ya estuviera por alguna razón (aunque no debería)
    // if AResponse.CustomHeaders.IndexOfName('Strict-Transport-Security') > -1 then
    //   AResponse.CustomHeaders.Delete(AResponse.CustomHeaders.IndexOfName('Strict-Transport-Security'));
  end;  if FContentTypeOptions <> '' then AResponse.CustomHeaders.Values['X-Content-Type-Options'] := FContentTypeOptions;
  if FReferrerPolicy <> '' then AResponse.CustomHeaders.Values['Referrer-Policy'] := FReferrerPolicy;
  if FPermissionsPolicy <> '' then AResponse.CustomHeaders.Values['Permissions-Policy'] := FPermissionsPolicy;
  if FXDownloadOptions <> '' then AResponse.CustomHeaders.Values['X-Download-Options'] := FXDownloadOptions;
  if FXDNSPrefetchControl <> '' then AResponse.CustomHeaders.Values['X-DNS-Prefetch-Control'] := FXDNSPrefetchControl;
  LogMessage('Security headers applied to response.', logSpam);
end;

{ TRateLimiter }
constructor TRateLimiter.Create(AConfig: TJSONObject);
begin
  inherited Create;
  FRequests := TDictionary<string, TRateLimitEntry>.Create;
  FLock := TCriticalSection.Create;
  FStopCleanupEvent := TEvent.Create(nil, True, False, ''); // ManualReset=True, InitialState=False

  if Assigned(AConfig) then
  begin
    FMaxRequestsPerWindow := TJSONHelper.GetInteger(AConfig, 'maxRequests', 60);
    FWindowSeconds        := TJSONHelper.GetInteger(AConfig, 'windowSeconds', 60);
    FBurstLimit           := TJSONHelper.GetInteger(AConfig, 'burstLimit', FMaxRequestsPerWindow + (FMaxRequestsPerWindow div 2));
    FBlockDurationMinutes := TJSONHelper.GetInteger(AConfig, 'blockMinutes', 5);
  end
  else
  begin
    FMaxRequestsPerWindow := 60; FWindowSeconds := 60; FBurstLimit := 90; FBlockDurationMinutes := 5;
    LogMessage('TRateLimiter: No configuration provided, using default values.', logWarning);
  end;

  FCleanupThread := TThread.CreateAnonymousThread(CleanupOldEntriesProc);
  // (FCleanupThread as TThread).FreeOnTerminate := False; // Default is False. We will manage it.
  FCleanupThread.Start;
  LogMessage(Format('TRateLimiter created. MaxReq: %d/%ds, Burst: %d, Block: %dmin. Cleanup thread started.',
    [FMaxRequestsPerWindow, FWindowSeconds, FBurstLimit, FBlockDurationMinutes]), logInfo);
end;

destructor TRateLimiter.Destroy;
begin
  LogMessage('TRateLimiter destroying...', logDebug);
  if Assigned(FStopCleanupEvent) then
    FStopCleanupEvent.SetEvent; // Signal cleanup thread to stop

  if Assigned(FCleanupThread) then
  begin
    LogMessage('TRateLimiter: Waiting for cleanup thread to terminate...', logDebug);
    if not FCleanupThread.Finished then // Check if it's actually running/not finished
       FCleanupThread.WaitFor; // Wait for the thread to finish its current loop and exit
    FreeAndNil(FCleanupThread); // Free the TThread object itself
  end;

  FreeAndNil(FStopCleanupEvent);
  FreeAndNil(FRequests);
  FreeAndNil(FLock);
  LogMessage('TRateLimiter destroyed.', logDebug);
  inherited;
end;

procedure TRateLimiter.CleanupOldEntriesProc;
const
  CLEANUP_INTERVAL_MS = 5 * 60 * 1000; // 5 minutos
begin
  LogMessage('RateLimiter cleanup thread (CleanupOldEntriesProc) started.', logInfo);
  try
    while True do
    begin
      if FStopCleanupEvent.WaitFor(CLEANUP_INTERVAL_MS) = wrSignaled then
      begin
        LogMessage('RateLimiter cleanup thread: Stop event signaled. Exiting.', logInfo);
        Break;
      end;
      DoCleanupOldEntries;
    end;
  except
    on E: Exception do
      LogMessage(Format('RateLimiter cleanup thread: Unhandled exception: %s - %s. Thread terminating.', [E.ClassName, E.Message]), logCritical);
  end;
  LogMessage('RateLimiter cleanup thread (CleanupOldEntriesProc) finished.', logInfo);
end;

procedure TRateLimiter.DoCleanupOldEntries;
var
  IPsToRemove: TList<string>;
  NowTimeUTC: TDateTime;
  Entry: TRateLimitEntry;
  Pair: TPair<string, TRateLimitEntry>;
begin
  IPsToRemove := TList<string>.Create;
  NowTimeUTC := NowUTC;
  FLock.Acquire;
  try
    for Pair in FRequests do
    begin
      Entry := Pair.Value;
      if (NowTimeUTC > Entry.BlockedUntilUTC) and // Not currently blocked
         (SecondsBetween(NowTimeUTC, Entry.LastRequestTimeUTC) > (FWindowSeconds * 5)) then // And old
        IPsToRemove.Add(Pair.Key);
    end;

    if IPsToRemove.Count > 0 then
    begin
      for var IP in IPsToRemove do
        FRequests.Remove(IP);
      LogMessage(Format('RateLimiter: Cleaned up %d old IP entries from tracking.', [IPsToRemove.Count]), logDebug);
    end;
  finally
    FLock.Release;
    IPsToRemove.Free;
  end;
end;

function TRateLimiter.IsRequestRateLimited(const AIPAddress: string; AResponse: TIdHTTPResponseInfo): Boolean;
var
  Entry: TRateLimitEntry;
  NowTimeUTC: TDateTime;
begin
  Result := False;
  NowTimeUTC := NowUTC;

  FLock.Acquire;
  try
    if FRequests.TryGetValue(AIPAddress, Entry) then
    begin
      if NowTimeUTC < Entry.BlockedUntilUTC then
      begin
        LogMessage(Format('Rate Limit: IP %s is currently blocked until %s (UTC).', [AIPAddress, DateToISO8601(Entry.BlockedUntilUTC)]), logWarning);
        Result := True;
      end
      else
      begin
        if SecondsBetween(NowTimeUTC, Entry.LastRequestTimeUTC) > FWindowSeconds then
        begin
          Entry.RequestCount := 1;
          Entry.LastRequestTimeUTC := NowTimeUTC;
        end
        else
        begin
          Inc(Entry.RequestCount);
        end;

        if Entry.RequestCount > FBurstLimit then
        begin
          Entry.BlockedUntilUTC := IncMinute(NowTimeUTC, FBlockDurationMinutes);
          LogMessage(Format('Rate Limit: IP %s EXCEEDED BURST LIMIT (%d > %d). Blocked until %s (UTC).',
            [AIPAddress, Entry.RequestCount, FBurstLimit, DateToISO8601(Entry.BlockedUntilUTC)]), logWarning);
          Result := True;
        end
        else if Entry.RequestCount > FMaxRequestsPerWindow then
        begin
          LogMessage(Format('Rate Limit: IP %s exceeded request window soft limit (%d > %d). Burst available up to %d.',
            [AIPAddress, Entry.RequestCount, FMaxRequestsPerWindow, FBurstLimit]), logInfo);
        end;
        FRequests.AddOrSetValue(AIPAddress, Entry);
      end;
    end
    else
    begin
      Entry.LastRequestTimeUTC := NowTimeUTC;
      Entry.RequestCount := 1;
      Entry.BlockedUntilUTC := 0; // TDateTime(0)
      FRequests.Add(AIPAddress, Entry);
      LogMessage(Format('Rate Limit: First request from IP %s. Tracking started.', [AIPAddress]), logSpam);
    end;
  finally
    FLock.Release;
  end;

  if Result and Assigned(AResponse) then // Check if AResponse is assigned
  begin
    AResponse.ResponseNo := 429; // Too Many Requests
    AResponse.ContentType := 'application/json';
    AResponse.ContentText := '{"success":false, "error":"Rate limit exceeded. Please try again later."}';
    AResponse.CustomHeaders.Values['Retry-After'] := IntToStr(FBlockDurationMinutes * 60);
  end;
end;

procedure TRateLimiter.ResetLimitForIP(const AIPAddress: string);
begin
  FLock.Acquire;
  try
    FRequests.Remove(AIPAddress);
    LogMessage(Format('Rate limit tracking reset for IP %s.', [AIPAddress]), logInfo);
  finally
    FLock.Release;
  end;
end;

{ TCSRFProtection }
constructor TCSRFProtection.Create(ASessionManager: TSessionManager; AConfig: TJSONObject);
var
  MethodsNode: TJSONArray;
  I: Integer;
  MethodStr: string;
  TempProtectedMethods: TList<string>; // Use a temporary list for building
begin
  inherited Create;
  if not Assigned(ASessionManager) then
    raise EConfigurationError.Create('TCSRFProtection requires a valid TSessionManager instance.');
  FSessionManager := ASessionManager;
  TempProtectedMethods := TList<string>.Create;

  try
    if Assigned(AConfig) then
    begin
      FTokenHeaderName    := TJSONHelper.GetString(AConfig, 'tokenHeaderName', 'X-CSRF-Token');
      FTokenFormFieldName := TJSONHelper.GetString(AConfig, 'tokenFormFieldName', '__CSRFToken__');
      FTokenSessionKey    := TJSONHelper.GetString(AConfig, 'tokenSessionKey', 'csrf_token');

      if AConfig.TryGetValue('protectedMethods', MethodsNode) and Assigned(MethodsNode) then
      begin
        for I := 0 to MethodsNode.Count - 1 do
        begin
          if Assigned(MethodsNode.Items[I]) then // Check if item is assigned
          begin
            MethodStr := Trim(MethodsNode.Items[I].Value);
            if MethodStr <> '' then
              TempProtectedMethods.Add(UpperCase(MethodStr));
          end;
        end;
        FProtectedMethods := TempProtectedMethods.ToArray;
      end
      else
        FProtectedMethods := ['POST', 'PUT', 'DELETE', 'PATCH'];
    end
    else
    begin
      FTokenHeaderName := 'X-CSRF-Token';
      FTokenFormFieldName := '__CSRFToken__';
      FTokenSessionKey := 'csrf_token';
      FProtectedMethods := ['POST', 'PUT', 'DELETE', 'PATCH'];
      LogMessage('TCSRFProtection: No configuration provided, using default values.', logWarning);
    end;
  finally
    TempProtectedMethods.Free;
  end;

  LogMessage(Format('TCSRFProtection created. Header: %s, FormField: %s, SessionKey: %s, ProtectedMethods: %s',
    [FTokenHeaderName, FTokenFormFieldName, FTokenSessionKey, string.Join(',', FProtectedMethods)]), logInfo);
end;

function TCSRFProtection.GenerateCSRFToken(const ASessionID: string): string;
var
  Seed: string;
  GuidBytes: TBytes;
  SessionIDBytes: TBytes;
  RandomBytes: TBytes;
  CombinedBytes: TBytes;
  LRandom: Cardinal;
begin
  GuidBytes := TEncoding.UTF8.GetBytes(TGuid.NewGuid.ToString);
  SessionIDBytes := TEncoding.UTF8.GetBytes(ASessionID);

  LRandom := System.Random(High(Cardinal));
  SetLength(RandomBytes, SizeOf(Cardinal));
  Move(LRandom, RandomBytes[0], SizeOf(Cardinal));

  SetLength(CombinedBytes, Length(GuidBytes) + Length(SessionIDBytes) + Length(RandomBytes));
  Move(GuidBytes[0], CombinedBytes[0], Length(GuidBytes));
  Move(SessionIDBytes[0], CombinedBytes[Length(GuidBytes)], Length(SessionIDBytes));
  Move(RandomBytes[0], CombinedBytes[Length(GuidBytes) + Length(SessionIDBytes)], Length(RandomBytes));

  Result := THashSHA2.GetHashString(TEncoding.UTF8.GetString(CombinedBytes), SHA256);
end;

function TCSRFProtection.GetCSRFTokenFromRequest(ARequest: TIdHTTPRequestInfo): string;
begin
  Result := '';
  if not Assigned(ARequest) then Exit;

  Result := ARequest.CustomHeaders.Values[FTokenHeaderName];
  if Result = '' then
  begin
    // Check for ARequest.RequestInfo before accessing ContentType
    if (ARequest.Document<>'') and
       SameText(ARequest.ContentType, 'application/x-www-form-urlencoded') then
      Result := ARequest.Params.Values[FTokenFormFieldName];
  end;
end;

function TCSRFProtection.ValidateCSRFToken(ARequest: TIdHTTPRequestInfo; AResponse: TIdHTTPResponseInfo; AIsUserAuthenticated: Boolean): Boolean;
var
  SessionID: string;
  Session: TSessionData;
  ExpectedToken, ReceivedToken: string;
  Method: string;
  MethodRequiresProtection: Boolean;
begin
  Result := True;
  if not Assigned(ARequest) or not Assigned(AResponse) then
  begin
    LogMessage('ValidateCSRFToken: ARequest or AResponse is nil.', logError);
    Exit(False); // Cannot proceed
  end;

  MethodRequiresProtection := False;
  Method := UpperCase(ARequest.Command);
  for var ProtectedMethod in FProtectedMethods do
  begin
    if Method = ProtectedMethod then
    begin
      MethodRequiresProtection := True;
      Break;
    end;
  end;

  if not MethodRequiresProtection then
  begin
    LogMessage(Format('CSRF: Method %s for %s does not require CSRF protection.', [ARequest.Command, ARequest.Document]), logSpam);
    Exit;
  end;

  if not AIsUserAuthenticated then
  begin
    LogMessage(Format('CSRF: User not authenticated for %s %s. CSRF protection skipped.', [ARequest.Command, ARequest.Document]), logDebug);
    Exit;
  end;

  SessionID := ARequest.Params.Values['session_id'];
  if SessionID = '' then
  begin
    LogMessage('CSRF: No session_id found in Request.Params for CSRF validation on ' + ARequest.Document + '.', logWarning);
    AResponse.ResponseNo := 403;
    AResponse.ContentType := 'application/json';
    AResponse.ContentText := '{"success":false, "error":"Session not found for CSRF validation."}';
    Result := False;
    Exit;
  end;

  Session := FSessionManager.GetSessionByID(SessionID);
  if not Assigned(Session) then
  begin
    LogMessage(Format('CSRF: Session %s not found or expired for CSRF validation on %s', [SessionID, ARequest.Document]), logWarning);
    AResponse.ResponseNo := 403;
    AResponse.ContentType := 'application/json';
    AResponse.ContentText := '{"success":false, "error":"Invalid or expired session for CSRF validation."}';
    Result := False;
    Exit;
  end;

  ExpectedToken := Session.GetValue(FTokenSessionKey);
  ReceivedToken := GetCSRFTokenFromRequest(ARequest);

  if (ExpectedToken = '') or (ReceivedToken = '') or (Length(ExpectedToken) <> Length(ReceivedToken)) or
     not CompareMem(PChar(ExpectedToken), PChar(ReceivedToken), Length(ExpectedToken)) then
  begin
    LogMessage(Format('CSRF Token Mismatch/Missing for session %s on %s. Expected (session): "%.10s...", Received (request): "%.10s..."',
      [SessionID, ARequest.Document, ExpectedToken, ReceivedToken]), logWarning);
    AResponse.ResponseNo := 403;
    AResponse.ContentType := 'application/json';
    AResponse.ContentText := '{"success":false, "error":"Invalid or missing CSRF token."}';
    Result := False;
    Exit;
  end;

  LogMessage(Format('CSRF Token validated successfully for session %s on %s', [SessionID, ARequest.Document]), logDebug);
end;

procedure TCSRFProtection.AddOrRefreshTokenInResponse(ARequest: TIdHTTPRequestInfo; AResponse: TIdHTTPResponseInfo);
var
  SessionID: string;
  Session: TSessionData;
  NewToken: string;
begin
  if not (Assigned(ARequest) and Assigned(AResponse) {and IsRequestUserAuthenticated(ARequest)}) then
     Exit;

  SessionID := ARequest.Params.Values['session_id'];
  if SessionID = '' then Exit;

  Session := FSessionManager.GetSessionByID(SessionID);
  if not Assigned(Session) then Exit;

  NewToken := GenerateCSRFToken(Session.ID);
  Session.SetValue(FTokenSessionKey, NewToken);
  AResponse.CustomHeaders.Values[FTokenHeaderName] := NewToken;
  LogMessage(Format('CSRF Token added/refreshed in response for session %s. Header: %s', [Session.ID, FTokenHeaderName]), logDebug);
end;


{ TSecurityMiddleware }
constructor TSecurityMiddleware.Create(AConfigJSONObject: TJSONObject);
var
  RateLimitConfigJSON,
  CSRFConfigJSON,
  HeadersConfigJSON: TJSONObject;
  ServerConfigSection, SSLConfigSection: TJSONObject; // Para leer server.ssl.enabled
  ConfigMgr: TConfigManager;
begin
  inherited Create;
  FIsServerSSLEnabled := False;
  try
    ConfigMgr := TConfigManager.GetInstance; // Asumir que ConfigManager ya está inicializado
    ServerConfigSection := TJSONHelper.GetJSONObject(ConfigMgr.ConfigData,'server');
    if Assigned(ServerConfigSection) then
    begin
      SSLConfigSection := ServerConfigSection.GetValue<TJSONObject>('ssl'); // GetValue es más seguro que GetJSONObject si puede no existir
      if Assigned(SSLConfigSection) then
        FIsServerSSLEnabled := TJSONHelper.GetBoolean(SSLConfigSection, 'enabled', False);
    end;
    LogMessage(Format('TSecurityMiddleware: Server SSL enabled status read as: %s', [BoolToStr(FIsServerSSLEnabled, True)]), logInfo);
  except
    on E: Exception do
      LogMessage(Format('TSecurityMiddleware: Error reading server SSL config: %s. Assuming SSL is disabled.', [E.Message]), logWarning);
  end;

  if Assigned(AConfigJSONObject) then
    FMiddlewareConfig := AConfigJSONObject.Clone as TJSONObject
  else
    FMiddlewareConfig := TJSONObject.Create;

  RateLimitConfigJSON := nil; // Initialize
  if FMiddlewareConfig.TryGetValue('rateLimiter', RateLimitConfigJSON) and Assigned(RateLimitConfigJSON) then
    FRateLimiterInstance := TRateLimiter.Create(RateLimitConfigJSON)
  else
    FRateLimiterInstance := TRateLimiter.Create(nil);

  CSRFConfigJSON := nil; // Initialize
  if FMiddlewareConfig.TryGetValue('csrfProtection', CSRFConfigJSON) and Assigned(CSRFConfigJSON) then
    FCSRFProtectionInstance := TCSRFProtection.Create(TSessionManager.GetInstance, CSRFConfigJSON)
  else
    FCSRFProtectionInstance := TCSRFProtection.Create(TSessionManager.GetInstance, nil);

  HeadersConfigJSON := nil; // Initialize
  FSecurityHeadersInstance := TSecurityHeaders.Create;
  if FMiddlewareConfig.TryGetValue('securityHeaders', HeadersConfigJSON) and Assigned(HeadersConfigJSON) then
    FSecurityHeadersInstance.LoadFromConfig(HeadersConfigJSON);

  LogMessage('TSecurityMiddleware created and sub-components initialized.', logInfo);
end;

destructor TSecurityMiddleware.Destroy;
begin
  LogMessage('TSecurityMiddleware destroying...', logDebug);
  FreeAndNil(FRateLimiterInstance);
  FreeAndNil(FCSRFProtectionInstance);
  FreeAndNil(FSecurityHeadersInstance);
  FreeAndNil(FMiddlewareConfig);
  inherited;
end;

function TSecurityMiddleware.IsRequestUserAuthenticated(ARequest: TIdHTTPRequestInfo): Boolean;
begin
  Result := Assigned(ARequest) and (ARequest.Params.Values['user_id'] <> '') and (ARequest.Params.Values['session_id'] <> '');
end;

function TSecurityMiddleware.ShouldCSRFProtectRequest(ARequest: TIdHTTPRequestInfo): Boolean;
var
  Method: string;
  IsProtectedMethod: Boolean;
begin
  Result := False;
  if not Assigned(ARequest) then Exit;

  if IsRequestUserAuthenticated(ARequest) then
  begin
    Method := UpperCase(ARequest.Command);
    IsProtectedMethod := False;
    if Assigned(FCSRFProtectionInstance) then
    begin
      for var ProtectedMethod in FCSRFProtectionInstance.FProtectedMethods do
      begin
        if Method = ProtectedMethod then
        begin
          IsProtectedMethod := True;
          break;
        end;
      end;
    end;
    Result := IsProtectedMethod;
  end;
end;

function TSecurityMiddleware.ValidateRequest(ARequest: TIdHTTPRequestInfo; AResponse: TIdHTTPResponseInfo): Boolean;
begin
  Result := True; // Assume valid until a check fails

  // Safety checks for nil request/response
  if not Assigned(ARequest) or not Assigned(AResponse) then
  begin
    LogMessage('TSecurityMiddleware.ValidateRequest: ARequest or AResponse is nil. Cannot validate.', logError);
    Exit(False);
  end;

  // 1. Apply Security Headers to all responses
  if Assigned(FSecurityHeadersInstance) then
    FSecurityHeadersInstance.ApplyToResponse(AResponse, FIsServerSSLEnabled); // Pasar el estado SSL

  // 2. Rate Limiting
  if Assigned(FRateLimiterInstance) then
  begin
    if FRateLimiterInstance.IsRequestRateLimited(ARequest.RemoteIP, AResponse) then
    begin
      LogMessage(Format('Request from %s to %s blocked by Rate Limiter.', [ARequest.RemoteIP, ARequest.Document]), logWarning);
      Result := False;
      Exit; // RateLimiter already set AResponse
    end;
  end;

  // 3. CSRF Validation (if applicable)
  if Assigned(FCSRFProtectionInstance) and ShouldCSRFProtectRequest(ARequest) then
  begin
    if not FCSRFProtectionInstance.ValidateCSRFToken(ARequest, AResponse, IsRequestUserAuthenticated(ARequest)) then
    begin
      LogMessage(Format('CSRF validation failed for %s %s from %s.', [ARequest.Command, ARequest.Document, ARequest.RemoteIP]), logWarning);
      Result := False; // ValidateCSRFToken already set AResponse
      Exit;
    end;
  end;

  // 4. Add/Refresh CSRF Token in Response (conditionally)
  // This is often done for GET requests that might precede a POST/PUT/DELETE,
  // or after a successful state-changing operation if tokens are single-use or rotated.
  // For an API, this might be done on login, or the client might request a new token.
  // The current placement (on every successful validated request) might be too broad.
  // if Result and Assigned(FCSRFProtectionInstance) and IsRequestUserAuthenticated(ARequest) then
  // begin
  //   FCSRFProtectionInstance.AddOrRefreshTokenInResponse(ARequest, AResponse);
  // end;

  if Result then
     LogMessage(Format('SecurityMiddleware: Request %s %s from %s PASSED all checks.',
       [ARequest.Command, ARequest.Document, ARequest.RemoteIP]), logDebug)
  else if AResponse.ResponseNo < 400 then // If a check failed but didn't set an error response code
     LogMessage(Format('SecurityMiddleware: Request %s %s from %s FAILED security checks, but no error code set by sub-validator.',
       [ARequest.Command, ARequest.Document, ARequest.RemoteIP]), logWarning);

end;

end.
