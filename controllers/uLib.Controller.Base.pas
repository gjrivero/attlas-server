unit uLib.Controller.Base;

interface

uses
  System.SysUtils, System.Classes,
  System.Generics.Collections, System.JSON, System.Rtti,
  IdCustomHTTPServer,   // TIdHTTPRequestInfo, TIdHTTPResponseInfo

  uLib.Routes,          // For TRouteManager
  // uLib.UrlParser,    // No longer needed here
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

  TBaseController = class
  protected
    class var FRouteManager: TRouteManager;
    // FUrlParserInstance: TUrlParser; // Removed
    // FParamsInstance: TJSONObject;    // Removed

    class function AcquireDBConnection(const APoolName: string): IDBConnection;
    class procedure ReleaseDBConnection(var AIDBConnection: IDBConnection; const APoolNameHint: string = '');

  public
    constructor Create; // Simplified
    destructor Destroy; override; // Simplified

    // function GetParam(const Key: string; const DefaultValue: string = ''): string; // Removed
    // function ProcessRequest(const AFullURL: string; ARequestContentStream: TStream = nil): Boolean; // Removed
    // property Params: TJSONObject read FParamsInstance; // Removed

    class function GetRequestBody(Request: TIdHTTPRequestInfo): TJSONValue; // Kept as it's a useful utility
    class procedure RegisterRoutes; virtual; abstract;
    class procedure Initialize(RouteManager: TRouteManager);
    class procedure HandleError(E: Exception; Response: TIdHTTPResponseInfo; Request: TIdHTTPRequestInfo = nil);
  end;

  TControllerRegistry = class
  private
    class var FControllers: TList<TControllerClass>;
  public
    class constructor CreateRegistry;
    class destructor DestroyRegistry;
    class procedure RegisterController(ControllerClass: TControllerClass);
    class procedure InitializeAll(RouteManager: TRouteManager);
  end;

implementation

uses
  System.StrUtils,          // For IfThen, etc.
  System.NetEncoding,          // For TEncoding (used in GetRequestBody)
  uLib.Database.Connection;

{ EInterpretedError }
constructor EInterpretedError.Create(const Msg: string; AErrorCode: Integer);
begin
  inherited Create(Msg);
  FErrorCode := AErrorCode;
end;

{ TBaseController }

constructor TBaseController.Create;
begin
  inherited Create;
  // FUrlParserInstance := TUrlParser.Create; // Removed
  // FParamsInstance := TJSONObject.Create;    // Removed
  LogMessage(Format('TBaseController instance created. Self: %p', [Pointer(Self)]), logDebug);
end;

destructor TBaseController.Destroy;
begin
  LogMessage(Format('TBaseController instance destroying. Self: %p', [Pointer(Self)]), logDebug);
  // FreeAndNil(FUrlParserInstance); // Removed
  // FreeAndNil(FParamsInstance);    // Removed
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

  LogMessage(Format('Controller attempting to acquire DB Connection from pool "%s"...', [APoolName]), logDebug);
  try
    Result := TDBConnectionPoolManager.GetInstance.AcquireConnection(APoolName);
    if Assigned(Result) then
      LogMessage(Format('DB Connection acquired successfully from pool "%s" by controller.', [APoolName]), logInfo)
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
      // Check if the interface supports being cast to TBaseConnection to get ConnectionConfigName
      if (AIDBConnection is TBaseConnection) then
      begin
         LBaseConn := AIDBConnection as TBaseConnection;
         LPoolName := LBaseConn.ConnectionConfigName;
      end;
      if LPoolName.IsEmpty then
         LogMessage('ReleaseDBConnection: No pool hint and could not determine pool name from connection object.', logDebug);
    end;

    LogMessage(Format('Controller attempting to release DB Connection back to pool (Hint/Actual: "%s")...', [IfThen(LPoolName<>'', LPoolName, 'Unknown')]), logDebug);
    try
      TDBConnectionPoolManager.GetInstance.ReleaseConnection(AIDBConnection, LPoolName);
      LogMessage(Format('DB Connection released back to pool (Hint/Actual: "%s") by controller.', [IfThen(LPoolName<>'', LPoolName, 'Unknown')]), logInfo);
    except
      on E: Exception do
        LogMessage(Format('Exception releasing DB Connection (Hint/Actual: "%s"): %s - %s', [IfThen(LPoolName<>'', LPoolName, 'Unknown'), E.ClassName, E.Message]), logError);
    end;
    AIDBConnection := nil;
  end;
end;

class function TBaseController.GetRequestBody(Request: TIdHTTPRequestInfo): TJSONValue;
var
  ContentStream: TStringStream;
  JsonString: string;
  LContentLength: Int64;
begin
  Result := nil;
  if not Assigned(Request) then
  begin
    LogMessage('GetRequestBody: Request object is nil.', logError);
    raise EInvalidRequestException.Create('HTTP Request object is missing.');
  end;

  LContentLength := Request.ContentLength;
  if (LContentLength = 0) then
  begin
    LogMessage('GetRequestBody: Request has no body content (ContentLength is 0).', logDebug);
    Result := TJSONObject.Create;
    Exit;
  end;

  ContentStream := nil; // Initialize
  (*if (Request.Document<>'') then
  begin
    ContentStream := TStringStream.Create(Request.Document, TEncoding.UTF8);
  end
  else*)
  if Assigned(Request.PostStream) and (Request.PostStream.Size > 0) then
  begin
    ContentStream := TStringStream.Create('', TEncoding.UTF8);
    Request.PostStream.Position := 0;
    ContentStream.CopyFrom(Request.PostStream, Request.PostStream.Size);
    Request.PostStream.Position := 0;
  end
  else
  begin
    LogMessage('GetRequestBody: Request has no ContentStream or PostStream with data, despite ContentLength > 0.', logDebug);
    Result := TJSONObject.Create;
    Exit;
  end;

  try
    if not Assigned(ContentStream) then // Should not happen if logic above is correct
    begin
      LogMessage('GetRequestBody: Internal error - ContentStream not assigned after checks.', logError);
      Result := TJSONObject.Create;
      Exit;
    end;

    JsonString := ContentStream.DataString;
    if JsonString.Trim.IsEmpty then
    begin
      LogMessage('GetRequestBody: Body stream contains only whitespace.', logDebug);
      Result := TJSONObject.Create;
      Exit;
    end;

    try
      Result := TJSONObject.ParseJSONValue(JsonString);
      if not Assigned(Result) then
      begin
        LogMessage('GetRequestBody: Parsed JSON value is nil (e.g., input was JSON "null" or malformed). Original: ' + Copy(JsonString,1,100), logWarning);
        Result := TJSONObject.Create;
      end
      else
        LogMessage(Format('GetRequestBody: Successfully parsed JSON of type %s.', [Result.ClassName]), logDebug);
    except
      on E: Exception do
      begin
        LogMessage(Format('Error parsing request body as JSON: %s - %s. Body: %s', [E.ClassName, E.Message, Copy(JsonString, 1, 200)]), logError);
        FreeAndNil(Result);
        raise EInvalidRequestException.CreateFmt('Error parsing request body as JSON: %s. Ensure valid JSON format.', [E.Message]);
      end;
    end;
  finally
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
  LogMessage(Format('TBaseController (and descendants) initialized with RouteManager: %s', [RouteManager.ClassName]), logInfo);
end;

class procedure TBaseController.HandleError(E: Exception; Response: TIdHTTPResponseInfo; Request: TIdHTTPRequestInfo = nil);
var
  ErrorResponse: TJSONObject;
  StatusCode: Integer;
  ErrorMessage, LoggedErrorMessage: string;
  RequestInfo: string;
begin
  RequestInfo := '';
  if Assigned(Request) then
    RequestInfo := Format(' (Request: %s %s)', [Request.Command, Request.Document]);

  LoggedErrorMessage := Format('Controller HandleError: Exception %s: %s%s', [E.ClassName, E.Message, RequestInfo]);
  LogMessage(LoggedErrorMessage, logError);

  StatusCode := 500; // Default
  ErrorMessage := 'An unexpected internal server error occurred.';

  if Assigned(Response) and (not Response.HeaderHasBeenWritten) and (Response.ResponseNo < 400) then
  begin
    if E is EDBPoolError then
    begin
      StatusCode := 503;
      ErrorMessage := 'Database service is temporarily unavailable. Details: ' + E.Message;
    end
    else if E is EDBConnectionError then
    begin
      StatusCode := 503;
      ErrorMessage := 'Database connection error. Details: ' + E.Message;
    end
    else if E is EDBCommandError then
    begin
      StatusCode := 500;
      ErrorMessage := 'Error processing database command. Details: ' + E.Message;
    end
    else if E is EConfigurationError then
    begin
      StatusCode := 500;
      ErrorMessage := 'Server configuration error: ' + E.Message;
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

    ErrorResponse := TJSONObject.Create;
    try
      ErrorResponse.AddPair('success', TJSONBool.Create(False));
      ErrorResponse.AddPair('message', ErrorMessage);
      {$IFDEF DEBUG}
      ErrorResponse.AddPair('exception_type', E.ClassName);
      if E.Message <> ErrorMessage then
         ErrorResponse.AddPair('original_message', E.Message);
      {$ENDIF}

      Response.ResponseNo := StatusCode;
      Response.ContentType := 'application/json';
      Response.ContentText := ErrorResponse.ToJSON;
      LogMessage(Format('Sending error response: Code %d, Message: %s', [StatusCode, ErrorMessage]), logInfo);
    finally
      ErrorResponse.Free;
    end;
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
  if not Assigned(FControllers) then
    FControllers := TList<TControllerClass>.Create
  else
    LogMessage('TControllerRegistry.CreateRegistry called but FControllers already assigned (class constructor logic).', logSpam);
end;

class destructor TControllerRegistry.DestroyRegistry;
begin
  FreeAndNil(FControllers);
  LogMessage('TControllerRegistry.DestroyRegistry: FControllers freed.', logDebug);
end;

class procedure TControllerRegistry.RegisterController(ControllerClass: TControllerClass);
begin
  if not Assigned(FControllers) then
  begin
    LogMessage('CRITICAL: TControllerRegistry.RegisterController: FControllers is nil! '+
               'Class constructor issue. Attempting to create.', logFatal);
    Create;
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
end;

class procedure TControllerRegistry.InitializeAll(RouteManager: TRouteManager);
var
  ControllerClass: TControllerClass;
begin
  if not Assigned(FControllers) then
  begin
    LogMessage('Controller registry (FControllers) not initialized. Cannot initialize controllers.', logError);
    Exit;
  end;
  if not Assigned(RouteManager) then
  begin
    LogMessage('RouteManager not provided to TControllerRegistry.InitializeAll. Cannot register routes.', logError);
    Exit;
  end;

  LogMessage(Format('TControllerRegistry: Initializing %d registered controllers...', [FControllers.Count]), logInfo);
  for ControllerClass in FControllers do
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
end;

initialization
  // Class constructor TControllerRegistry.CreateRegistry is called automatically.
finalization
  // Class destructor TControllerRegistry.DestroyRegistry is called automatically.
end.

