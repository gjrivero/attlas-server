unit uLib.Controller.Base;

interface

uses
  System.SysUtils, System.Classes,
  System.Generics.Collections, System.JSON, System.Rtti, System.SyncObjs,
  IdCustomHTTPServer,   // TIdHTTPRequestInfo, TIdHTTPResponseInfo

  uLib.Routes,          // For TRouteManager
  uLib.Database.Pool,   // For TDBConnectionPoolManager
  uLib.Database.Types,  // For IDBConnection, EDBPoolError, EDBConnectionError, EDBCommandError
  uLib.Server.Types,    // For EConfigurationError and other server types if needed
  uLib.Logger;

type
  TControllerClass = class of TBaseController;

  // Custom Exception types for controller layer or request validation
  EInterpretedError = class(Exception) // For errors that already have an HTTP code associated
  private
    FErrorCode: Integer;
  public
    constructor Create(const Msg: string; AErrorCode: Integer);
    property ErrorCode: Integer read FErrorCode;
  end;

  EInvalidRequestException = class(Exception); // General request error
  EMissingParameterException = class(EInvalidRequestException);
  EInvalidParameterException = class(EInvalidRequestException);
  EUnauthorizedException = class(EInvalidRequestException);

  // Pool para reutilizar objetos JSON frecuentes
  TJSONObjectPool = class
  private
    FPool: TThreadList<TJSONObject>;
    FMaxPoolSize: Integer;
    class var FInstance: TJSONObjectPool;
    class var FLock: TCriticalSection;
  public
    class function GetInstance: TJSONObjectPool;
    class constructor Create;
    class destructor Destroy;

    constructor CreateInstance(AMaxSize: Integer = 50);
    destructor Destroy; override;

    function AcquireObject: TJSONObject;
    procedure ReleaseObject(var AObject: TJSONObject);
  end;

  TErrorResponseCache = class
  private
    FCache: TDictionary<string, string>;
    FOrder: TList<string>; // LRU order: most recent at end
    FLock: TCriticalSection;
    FMaxSize: Integer;
    procedure TouchKey(const Key: string);
    procedure EnforceLimit;
  public
    constructor Create(AMaxSize: Integer = 100);
    destructor Destroy; override;

    function GetErrorResponse(const AStatusCode: Integer; const AMessage: string): string;
    procedure ClearCache;
  end;


  TBaseController = class
  protected
    class var FRouteManager: TRouteManager;
    class var FErrorResponseCache: TErrorResponseCache;
    class var FJSONPool: TJSONObjectPool;

    class function AcquireDBConnection(const APoolName: string): IDBConnection;
    class procedure ReleaseDBConnection(var AIDBConnection: IDBConnection; const APoolNameHint: string = '');

  public
    constructor Create; // Simplified
    destructor Destroy; override; // Simplified

    class function GetRequestBody(Request: TIdHTTPRequestInfo): TJSONValue; // Optimized version
    class function GetRequestBodySafe(Request: TIdHTTPRequestInfo; out AErrorMsg: string): TJSONValue; // Safe version
    class procedure RegisterRoutes; virtual; abstract;
    class procedure Initialize(RouteManager: TRouteManager);
    class procedure HandleError(E: Exception; Response: TIdHTTPResponseInfo; Request: TIdHTTPRequestInfo = nil);
    class procedure HandleErrorOptimized(E: Exception; Response: TIdHTTPResponseInfo; Request: TIdHTTPRequestInfo = nil);
  end;

  TControllerRegistry = class
  private
    class var FControllers: TList<TControllerClass>;
    class var FLock: TCriticalSection;
    class var FInitialized: Boolean;
  public
    class constructor CreateRegistry;
    class destructor DestroyRegistry;
    class procedure RegisterController(ControllerClass: TControllerClass);
    class procedure InitializeAll(RouteManager: TRouteManager);
  end;


implementation

uses
  System.StrUtils,          // For IfThen, etc.
  System.NetEncoding,       // For TEncoding (used in GetRequestBody)
  System.Math,              // For Min, Max
  uLib.Database.Connection;

{ EInterpretedError }
constructor EInterpretedError.Create(const Msg: string; AErrorCode: Integer);
begin
  inherited Create(Msg);
  FErrorCode := AErrorCode;
end;

{ TErrorResponseCache }

constructor TErrorResponseCache.Create(AMaxSize: Integer = 100);
begin
  inherited Create;
  FCache := TDictionary<string, string>.Create;
  FOrder := TList<string>.Create;
  FLock := TCriticalSection.Create;
  FMaxSize := AMaxSize;
end;

destructor TErrorResponseCache.Destroy;
begin
  FreeAndNil(FCache);
  FreeAndNil(FOrder);
  FreeAndNil(FLock);
  inherited;
end;

procedure TErrorResponseCache.TouchKey(const Key: string);
var
  Idx: Integer;
begin
  Idx := FOrder.IndexOf(Key);
  if Idx >= 0 then
    FOrder.Delete(Idx);
  FOrder.Add(Key);
end;

procedure TErrorResponseCache.EnforceLimit;
begin
  while FCache.Count > FMaxSize do
  begin
    // Remove least recently used
    if FOrder.Count > 0 then
    begin
      FCache.Remove(FOrder.First);
      FOrder.Delete(0);
    end
    else
      Break;
  end;
end;

function TErrorResponseCache.GetErrorResponse(const AStatusCode: Integer; const AMessage: string): string;
var
  CacheKey: string;
begin
  CacheKey := Format('%d|%s', [AStatusCode, AMessage]);

  FLock.Acquire;
  try
    if FCache.TryGetValue(CacheKey, Result) then
    begin
      TouchKey(CacheKey);
    end
    else
    begin
      // Crear respuesta solo si no está en cache
      Result := Format('{"success":false,"message":"%s","code":%d}',
        [StringReplace(AMessage, '"', '\"', [rfReplaceAll]), AStatusCode]);
      FCache.Add(CacheKey, Result);
      FOrder.Add(CacheKey);
      EnforceLimit;
    end;
  finally
    FLock.Release;
  end;
end;

procedure TErrorResponseCache.ClearCache;
begin
  FLock.Acquire;
  try
    FCache.Clear;
    FOrder.Clear;
  finally
    FLock.Release;
  end;
end;

{ TJSONObjectPool }

class constructor TJSONObjectPool.Create;
begin
  FLock := TCriticalSection.Create;
  FInstance := nil;
end;

class destructor TJSONObjectPool.Destroy;
begin
  FreeAndNil(FInstance);
  FreeAndNil(FLock);
end;

class function TJSONObjectPool.GetInstance: TJSONObjectPool;
begin
  if not Assigned(FInstance) then
  begin
    FLock.Acquire;
    try
      if not Assigned(FInstance) then
        FInstance := TJSONObjectPool.CreateInstance;
    finally
      FLock.Release;
    end;
  end;
  Result := FInstance;
end;

constructor TJSONObjectPool.CreateInstance(AMaxSize: Integer = 50);
begin
  inherited Create;
  FMaxPoolSize := AMaxSize;
  FPool := TThreadList<TJSONObject>.Create;
end;

destructor TJSONObjectPool.Destroy;
var
  List: TList<TJSONObject>;
  I: Integer;
begin
  if Assigned(FPool) then
  begin
    List := FPool.LockList;
    try
      for I := 0 to List.Count - 1 do
        List[I].Free;
      List.Clear;
    finally
      FPool.UnlockList;
    end;
    FreeAndNil(FPool);
  end;
  inherited;
end;

function TJSONObjectPool.AcquireObject: TJSONObject;
var
  List: TList<TJSONObject>;
begin
  Result := nil;
  if Assigned(FPool) then
  begin
    List := FPool.LockList;
    try
      if List.Count > 0 then
      begin
        Result := List.Last;
        List.Delete(List.Count - 1);
      end;
    finally
      FPool.UnlockList;
    end;
  end;

  if not Assigned(Result) then
    Result := TJSONObject.Create;
end;

procedure TJSONObjectPool.ReleaseObject(var AObject: TJSONObject);
var
  List: TList<TJSONObject>;
  CanPool: Boolean;
  PairToRemove: TJSONPair;
  I: Integer;
begin
  if not Assigned(AObject) then Exit;

  CanPool := False;
  if Assigned(FPool) then
  begin
    List := FPool.LockList;
    try
      if List.Count < FMaxPoolSize then
      begin
        for I := AObject.Count - 1 downto 0 do
        begin
          PairToRemove := AObject.Pairs[I];
          AObject.RemovePair(PairToRemove.JsonString.Value).Free;
        end;
        List.Add(AObject);
        CanPool := True;
      end;
    finally
      FPool.UnlockList;
    end;
  end;

  if not CanPool then
    FreeAndNil(AObject)
  else
    AObject := nil; // No liberar, est� en el pool
end;

{ TBaseController }

constructor TBaseController.Create;
begin
  inherited Create;
  LogMessage(Format('TBaseController instance created. Self: %p', [Pointer(Self)]), logSpam);
end;

destructor TBaseController.Destroy;
begin
  LogMessage(Format('TBaseController instance destroying. Self: %p', [Pointer(Self)]), logSpam);
  inherited;
end;

class function TBaseController.AcquireDBConnection(const APoolName: string): IDBConnection;
begin
  Result := nil;
  if APoolName.IsEmpty then
  begin
    LogMessage('AcquireDBConnection: Pool name cannot be empty.', logError);
    raise EDBPoolError.Create('Pool name not specified for AcquireDBConnection.');
  end;

  LogMessage(Format('Controller attempting to acquire DB Connection from pool "%s"...', [APoolName]), logSpam);
  try
    Result := TDBConnectionPoolManager.GetInstance.AcquireConnection(APoolName);
    if Assigned(Result) then
      LogMessage(Format('DB Connection acquired successfully from pool "%s" by controller.', [APoolName]), logSpam)
    else
    begin
      LogMessage(Format('Failed to acquire DB Connection from pool "%s" (AcquireConnection returned nil, but should raise on failure).', [APoolName]), logError);
      raise EDBPoolError.CreateFmt('Failed to acquire DB Connection from pool "%s" (returned nil).', [APoolName]);
    end;
  except
    on E: EDBPoolError do
    begin
      LogMessage(Format('EDBPoolError acquiring connection from pool "%s": %s', [APoolName, E.Message]), logError);
      raise;
    end;
    on E: EDBConnectionError do
    begin
      LogMessage(Format('EDBConnectionError acquiring connection from pool "%s": %s', [APoolName, E.Message]), logError);
      raise;
    end;
    on E: Exception do
    begin
      LogMessage(Format('Generic exception acquiring connection from pool "%s": %s - %s', [APoolName, E.ClassName, E.Message]), logError);
      raise EDBConnectionError.CreateFmt('Failed to acquire DB connection from pool "%s" due to an unexpected error: %s', [APoolName, E.Message]);
    end;
  end;
end;

class procedure TBaseController.ReleaseDBConnection(var AIDBConnection: IDBConnection; const APoolNameHint: string = '');
var
  LPoolName: string;
  LBaseConn: TBaseConnection;
begin
  if Assigned(AIDBConnection) then
  begin
    LPoolName := APoolNameHint;
    if LPoolName.IsEmpty then
    begin
      if (AIDBConnection is TBaseConnection) then
      begin
         LBaseConn := AIDBConnection as TBaseConnection;
         LPoolName := LBaseConn.ConnectionConfigName;
      end;
      if LPoolName.IsEmpty then
         LogMessage('ReleaseDBConnection: No pool hint and could not determine pool name from connection object.', logSpam);
    end;

    LogMessage(Format('Controller attempting to release DB Connection back to pool (Hint/Actual: "%s")...', [IfThen(LPoolName<>'', LPoolName, 'Unknown')]), logSpam);
    try
      TDBConnectionPoolManager.GetInstance.ReleaseConnection(AIDBConnection, LPoolName);
      LogMessage(Format('DB Connection released back to pool (Hint/Actual: "%s") by controller.', [IfThen(LPoolName<>'', LPoolName, 'Unknown')]), logSpam);
    except
      on E: Exception do
        LogMessage(Format('Exception releasing DB Connection (Hint/Actual: "%s"): %s - %s', [IfThen(LPoolName<>'', LPoolName, 'Unknown'), E.ClassName, E.Message]), logError);
    end;
    AIDBConnection := nil;
  end;
end;

class function TBaseController.GetRequestBody(Request: TIdHTTPRequestInfo): TJSONValue;
var
  ErrorMsg: string;
begin
  Result := GetRequestBodySafe(Request, ErrorMsg);
  if not Assigned(Result) then
    raise EInvalidRequestException.Create(ErrorMsg);
end;

class function TBaseController.GetRequestBodySafe(Request: TIdHTTPRequestInfo; out AErrorMsg: string): TJSONValue;
var
  ContentStream: TStringStream;
  JsonString: string;
  LContentLength: Int64;
  StreamOwned: Boolean;
begin
  Result := nil;
  AErrorMsg := '';
  ContentStream := nil;
  StreamOwned := False;

  if not Assigned(Request) then
  begin
    AErrorMsg := 'HTTP Request object is missing.';
    LogMessage('GetRequestBodySafe: Request object is nil.', logError);
    Exit;
  end;

  LContentLength := Request.ContentLength;
  if (LContentLength = 0) then
  begin
    LogMessage('GetRequestBodySafe: Request has no body content (ContentLength is 0).', logSpam);
    Result := TJSONObject.Create; // Retornar objeto vac�o en lugar de nil
    Exit;
  end;

  try
    // Optimizaci�n: usar el stream existente si est� disponible
    if Assigned(Request.PostStream) and (Request.PostStream.Size > 0) then
    begin
      // Leer directamente del PostStream existente
      Request.PostStream.Position := 0;
      ContentStream := TStringStream.Create('', TEncoding.UTF8);
      StreamOwned := True;
      try
        ContentStream.CopyFrom(Request.PostStream, Request.PostStream.Size);
        Request.PostStream.Position := 0; // Resetear para otros usos
      except
        on E: Exception do
        begin
          AErrorMsg := Format('Error reading from PostStream: %s', [E.Message]);
          Exit;
        end;
      end;
    end
    else
    begin
      LogMessage('GetRequestBodySafe: Request has no PostStream with data, despite ContentLength > 0.', logSpam);
      Result := TJSONObject.Create;
      Exit;
    end;

    JsonString := ContentStream.DataString;
    if JsonString.Trim.IsEmpty then
    begin
      LogMessage('GetRequestBodySafe: Body stream contains only whitespace.', logSpam);
      Result := TJSONObject.Create;
      Exit;
    end;

    try
      Result := TJSONObject.ParseJSONValue(JsonString);
      if not Assigned(Result) then
      begin
        LogMessage('GetRequestBodySafe: Parsed JSON value is nil (e.g., input was JSON "null" or malformed). Original: ' + Copy(JsonString,1,100), logWarning);
        Result := TJSONObject.Create;
      end
      else
        LogMessage(Format('GetRequestBodySafe: Successfully parsed JSON of type %s.', [Result.ClassName]), logSpam);
    except
      on E: Exception do
      begin
        AErrorMsg := Format('Error parsing request body as JSON: %s. Ensure valid JSON format.', [E.Message]);
        LogMessage(Format('Error parsing request body as JSON: %s - %s. Body: %s', [E.ClassName, E.Message, Copy(JsonString, 1, 200)]), logError);
        Exit;
      end;
    end;
  finally
    if StreamOwned and Assigned(ContentStream) then
      FreeAndNil(ContentStream);
  end;
end;

class procedure TBaseController.Initialize(RouteManager: TRouteManager);
begin
  if not Assigned(RouteManager) then
  begin
     LogMessage('TBaseController.Initialize: RouteManager is nil. Cannot assign.', logError);
     Exit;
  end;
  FRouteManager := RouteManager;

  // Inicializar componentes singleton
  if not Assigned(FErrorResponseCache) then
    FErrorResponseCache := TErrorResponseCache.Create;
  if not Assigned(FJSONPool) then
    FJSONPool := TJSONObjectPool.GetInstance;

  LogMessage(Format('TBaseController (and descendants) initialized with RouteManager: %s', [RouteManager.ClassName]), logInfo);
end;

class procedure TBaseController.HandleError(E: Exception; Response: TIdHTTPResponseInfo; Request: TIdHTTPRequestInfo = nil);
begin
  // Usar la versi�n optimizada por defecto
  HandleErrorOptimized(E, Response, Request);
end;

class procedure TBaseController.HandleErrorOptimized(E: Exception; Response: TIdHTTPResponseInfo; Request: TIdHTTPRequestInfo = nil);
var
  ErrorResponse: TJSONObject;
  StatusCode: Integer;
  ErrorMessage, LoggedErrorMessage: string;
  RequestInfo: string;
  ResponseText: string;
begin
  RequestInfo := '';
  if Assigned(Request) then
    RequestInfo := Format(' (Request: %s %s)', [Request.Command, Request.Document]);

  LoggedErrorMessage := Format('Controller HandleErrorOptimized: Exception %s: %s%s', [E.ClassName, E.Message, RequestInfo]);
  LogMessage(LoggedErrorMessage, logError);

  StatusCode := 500; // Default
  ErrorMessage := 'An unexpected internal server error occurred.';

  if Assigned(Response) and (not Response.HeaderHasBeenWritten) and (Response.ResponseNo < 400) then
  begin
    // Determinar c�digo de estado y mensaje
    if E is EDBPoolError then
    begin
      StatusCode := 503;
      ErrorMessage := 'Database service is temporarily unavailable.';
    end
    else if E is EDBConnectionError then
    begin
      StatusCode := 503;
      ErrorMessage := 'Database connection error.';
    end
    else if E is EDBCommandError then
    begin
      StatusCode := 500;
      ErrorMessage := 'Error processing database command.';
    end
    else if E is EConfigurationError then
    begin
      StatusCode := 500;
      ErrorMessage := 'Server configuration error.';
    end
    else if E is EUnauthorizedException then
    begin
      StatusCode := 401;
      ErrorMessage := IfThen(E.Message <> '', E.Message, 'Authentication required or credentials invalid.');
    end
    else if E is EMissingParameterException then
    begin
      StatusCode := 400;
      ErrorMessage := 'Missing required parameter: ' + E.Message;
    end
    else if E is EInvalidParameterException then
    begin
      StatusCode := 400;
      ErrorMessage := 'Invalid parameter provided: ' + E.Message;
    end
    else if E is EInvalidRequestException then
    begin
      StatusCode := 400;
      ErrorMessage := IfThen(E.Message <> '', E.Message, 'The request could not be understood or was missing required parameters.');
    end
    else if E is EInterpretedError then
    begin
      StatusCode := (E as EInterpretedError).ErrorCode;
      ErrorMessage := E.Message;
    end;

    // Usar cache para respuestas comunes
    if Assigned(FErrorResponseCache) then
    begin
      ResponseText := FErrorResponseCache.GetErrorResponse(StatusCode, ErrorMessage);
    end
    else
    begin
      // Fallback si no hay cache disponible
      ErrorResponse := nil;
      if Assigned(FJSONPool) then
        ErrorResponse := FJSONPool.AcquireObject
      else
        ErrorResponse := TJSONObject.Create;

      try
        ErrorResponse.AddPair('success', TJSONBool.Create(False));
        ErrorResponse.AddPair('message', ErrorMessage);
        ErrorResponse.AddPair('code', TJSONNumber.Create(StatusCode));

        {$IFDEF DEBUG}
        ErrorResponse.AddPair('exception_type', E.ClassName);
        if E.Message <> ErrorMessage then
           ErrorResponse.AddPair('original_message', E.Message);
        {$ENDIF}

        ResponseText := ErrorResponse.ToJSON;
      finally
        if Assigned(FJSONPool) then
          FJSONPool.ReleaseObject(ErrorResponse)
        else
          FreeAndNil(ErrorResponse);
      end;
    end;

    Response.ResponseNo := StatusCode;
    Response.ContentType := 'application/json';
    Response.ContentText := ResponseText;
    LogMessage(Format('Sending error response: Code %d, Message: %s', [StatusCode, ErrorMessage]), logInfo);
  end
  else if Assigned(Response) and Response.HeaderHasBeenWritten then
    LogMessage(Format('Could not set error response (Code %d, Msg: %s) because headers were already written for request%s.',
      [StatusCode, ErrorMessage, RequestInfo]), logWarning)
  else if not Assigned(Response) then
    LogMessage(Format('Could not set error response (Code %d, Msg: %s) because Response object was nil for request%s.',
      [StatusCode, ErrorMessage, RequestInfo]), logError);
end;

{ TControllerRegistry }

class constructor TControllerRegistry.CreateRegistry;
begin
  FLock := TCriticalSection.Create;
  FControllers := TList<TControllerClass>.Create;
  FInitialized := False;
end;

class destructor TControllerRegistry.DestroyRegistry;
begin
  FreeAndNil(FControllers);
  FreeAndNil(FLock);
  LogMessage('TControllerRegistry.DestroyRegistry: Resources freed.', logDebug);
end;

class procedure TControllerRegistry.RegisterController(ControllerClass: TControllerClass);
begin
  if not Assigned(FLock) then
  begin
    LogMessage('CRITICAL: TControllerRegistry.RegisterController: FLock is nil!', logFatal);
    Exit;
  end;

  FLock.Acquire;
  try
    if not Assigned(FControllers) then
    begin
      LogMessage('CRITICAL: TControllerRegistry.RegisterController: FControllers is nil!', logFatal);
      Exit;
    end;

    if Assigned(ControllerClass) then
    begin
      if FControllers.IndexOf(ControllerClass) = -1 then
      begin
        FControllers.Add(ControllerClass);
        LogMessage(Format('Controller %s registered.', [ControllerClass.ClassName]), logInfo);
      end
      else
        LogMessage(Format('Controller %s already registered. Ignoring duplicate registration.', [ControllerClass.ClassName]), logDebug);
    end
    else
      LogMessage('TControllerRegistry.RegisterController: Attempted to register a nil ControllerClass.', logError);
  finally
    FLock.Release;
  end;
end;

class procedure TControllerRegistry.InitializeAll(RouteManager: TRouteManager);
var
  ControllerClass: TControllerClass;
  ControllerList: TList<TControllerClass>;
  I: Integer;
begin
  if not Assigned(FLock) then
  begin
    LogMessage('Controller registry lock not initialized. Cannot initialize controllers.', logError);
    Exit;
  end;

  if not Assigned(RouteManager) then
  begin
    LogMessage('RouteManager not provided to TControllerRegistry.InitializeAll. Cannot register routes.', logError);
    Exit;
  end;

  ControllerList := TList<TControllerClass>.Create;
  try
    FLock.Acquire;
    try
      if not Assigned(FControllers) then
      begin
        LogMessage('Controller registry (FControllers) not initialized. Cannot initialize controllers.', logError);
        Exit;
      end;

      // Crear copia para iterar fuera del lock
      for I := 0 to FControllers.Count - 1 do
        ControllerList.Add(FControllers[I]);

      FInitialized := True;
    finally
      FLock.Release;
    end;

    LogMessage(Format('TControllerRegistry: Initializing %d registered controllers...', [ControllerList.Count]), logInfo);
    for ControllerClass in ControllerList do
    begin
      try
        ControllerClass.Initialize(RouteManager);
        ControllerClass.RegisterRoutes;
        LogMessage(Format('Controller %s initialized and routes registered.', [ControllerClass.ClassName]), logDebug);
      except
        on E: Exception do
          LogMessage(Format('Error initializing or registering routes for controller %s: %s - %s', [ControllerClass.ClassName, E.ClassName, E.Message]), logError);
      end;
    end;
    LogMessage('TControllerRegistry: All registered controllers processed for initialization.', logInfo);
  finally
    ControllerList.Free;
  end;
end;

initialization
  // Class constructor TControllerRegistry.CreateRegistry is called automatically.
finalization
  // Class destructor TControllerRegistry.DestroyRegistry is called automatically.
  if Assigned(TBaseController.FErrorResponseCache) then
    FreeAndNil(TBaseController.FErrorResponseCache);
end.
