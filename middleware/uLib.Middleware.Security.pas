unit uLib.Middleware.Security;

interface

uses
  System.SysUtils, System.Classes, System.Hash, System.JSON,
  System.SyncObjs, System.Generics.Collections, System.RegularExpressions,
  System.Threading, System.DateUtils, System.NetEncoding, System.Rtti,
  System.StrUtils, System.Math,
  IdCustomHTTPServer, IdGlobal,
  uLib.Session.Manager,
  uLib.Logger,
  uLib.Server.Types,
  uLib.Config.Manager,
  uLib.Utils;

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

type
  // Simple cancellation token implementation
  TCancellationToken = class
  private
    FEvent: TEvent;
    FIsCancelled: Boolean;
    FLock: TCriticalSection;
  public
    constructor Create;
    destructor Destroy; override;
    procedure Cancel;
    function IsCancelled: Boolean;
    function WaitHandle: TEvent;
    function WaitOne(ATimeoutMs: Integer): Boolean;
  end;

  TCancellationTokenSource = class
  private
    FToken: TCancellationToken;
  public
    constructor Create;
    destructor Destroy; override;
    procedure Cancel;
    property Token: TCancellationToken read FToken;
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

    FStopEvent: TEvent;
    FCleanupTask: ITask;
    FIsShuttingDown: Boolean;

    procedure CleanupOldEntriesProc;
    procedure DoCleanupOldEntries;
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

{ TSecurityHeaders }

constructor TSecurityHeaders.Create;
begin
  inherited Create;
  FCSP := 'default-src ''self''; script-src ''self''; style-src ''self'' ''unsafe-inline''; img-src ''self'' data:; object-src ''none''; frame-ancestors ''none'';';
  FFrameOptions := 'SAMEORIGIN';
  FXSSProtection := '1; mode=block';
  FHSTS := 'max-age=31536000; includeSubDomains';
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

  if FCSP <> '' then
    AResponse.CustomHeaders.Values['Content-Security-Policy'] := FCSP;
  if FFrameOptions <> '' then
    AResponse.CustomHeaders.Values['X-Frame-Options'] := FFrameOptions;
  if FXSSProtection <> '' then
    AResponse.CustomHeaders.Values['X-XSS-Protection'] := FXSSProtection;

  if AIsServerSSLEnabled then
  begin
    if FHSTS <> '' then
      AResponse.CustomHeaders.Values['Strict-Transport-Security'] := FHSTS;
  end
  else if FHSTS <> '' then
  begin
    LogMessage('TSecurityHeaders: HSTS is configured but server SSL is disabled. HSTS header will NOT be sent.', logWarning);
  end;

  if FContentTypeOptions <> '' then
    AResponse.CustomHeaders.Values['X-Content-Type-Options'] := FContentTypeOptions;
  if FReferrerPolicy <> '' then
    AResponse.CustomHeaders.Values['Referrer-Policy'] := FReferrerPolicy;
  if FPermissionsPolicy <> '' then
    AResponse.CustomHeaders.Values['Permissions-Policy'] := FPermissionsPolicy;
  if FXDownloadOptions <> '' then
    AResponse.CustomHeaders.Values['X-Download-Options'] := FXDownloadOptions;
  if FXDNSPrefetchControl <> '' then
    AResponse.CustomHeaders.Values['X-DNS-Prefetch-Control'] := FXDNSPrefetchControl;

  LogMessage('Security headers applied to response.', logSpam);
end;

{ TCancellationToken }

constructor TCancellationToken.Create;
begin
  inherited Create;
  FEvent := TEvent.Create(nil, True, False, ''); // Manual reset, initially false
  FIsCancelled := False;
  FLock := TCriticalSection.Create;
end;

destructor TCancellationToken.Destroy;
begin
  FreeAndNil(FEvent);
  FreeAndNil(FLock);
  inherited;
end;

procedure TCancellationToken.Cancel;
begin
  FLock.Acquire;
  try
    if not FIsCancelled then
    begin
      FIsCancelled := True;
      if Assigned(FEvent) then
        FEvent.SetEvent;
    end;
  finally
    FLock.Release;
  end;
end;

function TCancellationToken.IsCancelled: Boolean;
begin
  FLock.Acquire;
  try
    Result := FIsCancelled;
  finally
    FLock.Release;
  end;
end;

function TCancellationToken.WaitHandle: TEvent;
begin
  Result := FEvent;
end;

function TCancellationToken.WaitOne(ATimeoutMs: Integer): Boolean;
begin
  if not Assigned(FEvent) then
  begin
    Result := IsCancelled;
    Exit;
  end;

  Result := (FEvent.WaitFor(ATimeoutMs) = wrSignaled);
end;

{ TCancellationTokenSource }

constructor TCancellationTokenSource.Create;
begin
  inherited Create;
  FToken := TCancellationToken.Create;
end;

destructor TCancellationTokenSource.Destroy;
begin
  FreeAndNil(FToken);
  inherited;
end;

procedure TCancellationTokenSource.Cancel;
begin
  if Assigned(FToken) then
    FToken.Cancel;
end;
{ TRateLimiter }

constructor TRateLimiter.Create(AConfig: TJSONObject);
begin
  inherited Create;

  FRequests := TDictionary<string, TRateLimitEntry>.Create;
  FLock := TCriticalSection.Create;
  FStopEvent := TEvent.Create(nil, True, False, ''); // Manual reset, initially false
  FIsShuttingDown := False;

  // Load configuration
  if Assigned(AConfig) then
  begin
    FMaxRequestsPerWindow := TJSONHelper.GetInteger(AConfig, 'maxRequests', 60);
    FWindowSeconds        := TJSONHelper.GetInteger(AConfig, 'windowSeconds', 60);
    FBurstLimit           := TJSONHelper.GetInteger(AConfig, 'burstLimit', FMaxRequestsPerWindow + (FMaxRequestsPerWindow div 2));
    FBlockDurationMinutes := TJSONHelper.GetInteger(AConfig, 'blockMinutes', 5);
  end
  else
  begin
    FMaxRequestsPerWindow := 60;
    FWindowSeconds := 60;
    FBurstLimit := 90;
    FBlockDurationMinutes := 5;
    LogMessage('TRateLimiter: No configuration provided, using default values.', logWarning);
  end;

  // Start cleanup task
  FCleanupTask := TTask.Run(
    procedure
    begin
      CleanupOldEntriesProc;
    end);
  LogMessage(Format('TRateLimiter created. MaxReq: %d/%ds, Burst: %d, Block: %dmin. Cleanup task started.',
    [FMaxRequestsPerWindow, FWindowSeconds, FBurstLimit, FBlockDurationMinutes]), logInfo);
end;

destructor TRateLimiter.Destroy;
const
  TASK_WAIT_TIMEOUT = 5000;
begin
  LogMessage('TRateLimiter destroying...', logDebug);

  FIsShuttingDown := True;

  // Signal stop event
  if Assigned(FStopEvent) then
  begin
    LogMessage('TRateLimiter: Signalling cleanup task to stop...', logDebug);
    FStopEvent.SetEvent;
  end;

  // Wait for cleanup task to complete
  if Assigned(FCleanupTask) then
  begin
    try
      LogMessage('TRateLimiter: Waiting for cleanup task to finish...', logDebug);

      case FCleanupTask.Wait(TASK_WAIT_TIMEOUT) of
        True:
          LogMessage('TRateLimiter: Cleanup task terminated successfully.', logDebug);
        False:
          LogMessage('TRateLimiter: Cleanup task timeout.', logWarning);
      end;
    except
      on E: Exception do
        LogMessage(Format('TRateLimiter: Error waiting for cleanup task: %s', [E.Message]), logError);
    end;

    FCleanupTask := nil;
  end;

  FreeAndNil(FStopEvent);
  FreeAndNil(FRequests);
  FreeAndNil(FLock);

  inherited;
end;

procedure TRateLimiter.CleanupOldEntriesProc;
const
  CLEANUP_INTERVAL_MS = 5 * 60 * 1000;
  CHECK_INTERVAL_MS = 1000;
var
  RemainingWait: Integer;
begin
  LogMessage('RateLimiter cleanup task started.', logInfo);

  try
    while not FIsShuttingDown do
    begin
      RemainingWait := CLEANUP_INTERVAL_MS;

      // Wait in small intervals for responsive cancellation
      while (RemainingWait > 0) and not FIsShuttingDown do
      begin
        var WaitTime := Min(CHECK_INTERVAL_MS, RemainingWait);

        if Assigned(FStopEvent) and (FStopEvent.WaitFor(WaitTime) = wrSignaled) then
        begin
          LogMessage('RateLimiter cleanup task: Stop event signaled. Exiting.', logInfo);
          Exit;
        end;

        Dec(RemainingWait, WaitTime);
      end;

      // Perform cleanup if not stopped
      if not FIsShuttingDown then
      begin
        try
          DoCleanupOldEntries;
        except
          on E: Exception do
            LogMessage(Format('RateLimiter cleanup: Error: %s', [E.Message]), logError);
        end;
      end;
    end;
  except
    on E: Exception do
      LogMessage(Format('RateLimiter cleanup task exception: %s', [E.Message]), logCritical);
  end;

  LogMessage('RateLimiter cleanup task finished.', logInfo);
end;

procedure TRateLimiter.DoCleanupOldEntries;
var
  IPsToRemove: TList<string>;
  NowTimeUTC: TDateTime;
  Entry: TRateLimitEntry;
  Pair: TPair<string, TRateLimitEntry>;
begin
  if FIsShuttingDown or not Assigned(FLock) or not Assigned(FRequests) then Exit;

  IPsToRemove := TList<string>.Create;
  try
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
    end;
  finally
    IPsToRemove.Free;
  end;
end;

function TRateLimiter.IsRequestRateLimited(const AIPAddress: string; AResponse: TIdHTTPResponseInfo): Boolean;
var
  Entry: TRateLimitEntry;
  NowTimeUTC: TDateTime;
begin
  Result := False;

  if FIsShuttingDown or not Assigned(FLock) or not Assigned(FRequests) then Exit;

  NowTimeUTC := NowUTC;

  FLock.Acquire;
  try
    if FRequests.TryGetValue(AIPAddress, Entry) then
    begin
      if NowTimeUTC < Entry.BlockedUntilUTC then
      begin
        LogMessage(Format('Rate Limit: IP %s is currently blocked until %s (UTC).',
          [AIPAddress, DateToISO8601(Entry.BlockedUntilUTC)]), logWarning);
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

  if Result and Assigned(AResponse) then
  begin
    AResponse.ResponseNo := 429; // Too Many Requests
    AResponse.ContentType := 'application/json';
    AResponse.ContentText := '{"success":false, "error":"Rate limit exceeded. Please try again later."}';
    AResponse.CustomHeaders.Values['Retry-After'] := IntToStr(FBlockDurationMinutes * 60);
  end;
end;

procedure TRateLimiter.ResetLimitForIP(const AIPAddress: string);
begin
  if FIsShuttingDown or not Assigned(FLock) or not Assigned(FRequests) then Exit;

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
  TempProtectedMethods: TList<string>;
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
          if Assigned(MethodsNode.Items[I]) then
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
    if (ARequest.Document <> '') and
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
    Exit(False);
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
  if not (Assigned(ARequest) and Assigned(AResponse)) then Exit;

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
  RateLimitConfigJSON, CSRFConfigJSON, HeadersConfigJSON: TJSONObject;
  ServerConfigSection, SSLConfigSection: TJSONObject;
  ConfigMgr: TConfigManager;
begin
  inherited Create;

  FIsServerSSLEnabled := False;

  try
    ConfigMgr := TConfigManager.GetInstance;
    ServerConfigSection := TJSONHelper.GetJSONObject(ConfigMgr.ConfigData, 'server');
    if Assigned(ServerConfigSection) then
    begin
      SSLConfigSection := ServerConfigSection.GetValue<TJSONObject>('ssl');
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

  // Initialize Rate Limiter
  RateLimitConfigJSON := nil;
  if FMiddlewareConfig.TryGetValue('rateLimiter', RateLimitConfigJSON) and Assigned(RateLimitConfigJSON) then
    FRateLimiterInstance := TRateLimiter.Create(RateLimitConfigJSON)
  else
    FRateLimiterInstance := TRateLimiter.Create(nil);

  // Initialize CSRF Protection
  CSRFConfigJSON := nil;
  if FMiddlewareConfig.TryGetValue('csrfProtection', CSRFConfigJSON) and Assigned(CSRFConfigJSON) then
    FCSRFProtectionInstance := TCSRFProtection.Create(TSessionManager.GetInstance, CSRFConfigJSON)
  else
    FCSRFProtectionInstance := TCSRFProtection.Create(TSessionManager.GetInstance, nil);

  // Initialize Security Headers
  HeadersConfigJSON := nil;
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

  LogMessage('TSecurityMiddleware destroyed.', logDebug);
  inherited;
end;

function TSecurityMiddleware.IsRequestUserAuthenticated(ARequest: TIdHTTPRequestInfo): Boolean;
begin
  Result := Assigned(ARequest) and
            (ARequest.Params.Values['user_id'] <> '') and
            (ARequest.Params.Values['session_id'] <> '');
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
          Break;
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
    FSecurityHeadersInstance.ApplyToResponse(AResponse, FIsServerSSLEnabled);

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
  // Uncomment if you want to add CSRF tokens to successful responses
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

