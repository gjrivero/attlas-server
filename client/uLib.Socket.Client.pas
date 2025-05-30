unit uLib.Socket.Client;

interface

uses
  System.Classes,
  System.SyncObjs,
  System.SysUtils,
  System.Math,
  System.Threading,
  System.DateUtils,
  IdURI,
  IdGlobal, // Para IndyTextEncoding_UTF8
  IdTCPClient,
  IdSSLOpenSSL,
  IdCoderMIME, // Para TIdEncoderMIME
  IdHashSHA,   // Para TIdHashSHA1
  System.Generics.Collections,
  System.JSON;

type
  TEventListener = reference to procedure(const AText: string); // Para mensajes de texto o binarios como string
  TEventListenerError = reference to procedure(const AException: Exception; var AForceDisconnect: Boolean);

{$SCOPEDENUMS ON}
  TOperationCode = (CONTINUE, TEXT_FRAME, BINARY_FRAME, CONNECTION_CLOSE, PING, PONG);
  TEventType = (OPEN, &MESSAGE, ERROR, CLOSE, UPGRADE, HEART_BEAT_TIMER); // &MESSAGE es un keyword, podría ser solo MESSAGE
{$SCOPEDENUMS OFF}

  TOperationCodeHelper = record helper for TOperationCode
    function ToByte: Byte;
  end;

  TSocketClient = class(TIdTCPClient)
  private
    FInternalLock: TCriticalSection;
    FHeader: TDictionary<string, string>; // Cabeceras HTTP personalizadas para el handshake inicial
    FURL: string;
    FSecWebSocketAcceptExpectedResponse: string; // Para validar la respuesta del servidor
    FHeartBeatInterval: Cardinal; // en milisegundos
    FAutoCreateHandler: Boolean; // Para crear TIdSSLIOHandlerSocketOpenSSL automáticamente para wss
    FUpgraded: Boolean; // Flag: True si el handshake HTTP Upgrade fue exitoso
    FClosingEventLocalHandshake: Boolean; // Flag para manejar el cierre iniciado localmente
    FOnMessage: TEventListener;
    FOnOpen: TEventListener; // Podría ser TNotifyEvent si no pasa texto
    FOnClose: TNotifyEvent;
    FOnError: TEventListenerError;
    FOnHeartBeatTimer: TNotifyEvent;
    FOnUpgrade: TNotifyEvent; // Se dispara después de un handshake exitoso
    FSubProtocol: string;     // Subprotocolo WebSocket solicitado/acordado
    FTaskReadFromWebSocket, FTaskHeartBeat: ITask; // Tareas para lectura y heartbeat

    function GenerateWebSocketKey: string;
    function IsValidWebSocketConnection: Boolean; // Renombrado de IsValidWebSocket para claridad
    function ValidateHandshakeHeaders(const AHeaders: TStrings): Boolean; // Renombrado de IsValidHeaders
    function EncodeWebSocketFrame(const AMessage: RawByteString; const AOperationCode: TOperationCode = TOperationCode.TEXT_FRAME): TIdBytes; // Renombrado
    function GetBitFromCardinal(const AValue: Cardinal; const ABitIndex: Byte): Boolean; // Renombrado
    // function SetBitInCardinal(const AValue: Cardinal; const ABitIndex: Byte): Cardinal; // Renombrado y uso
    // function ClearBitFromCardinal(const AValue: Cardinal; const ABitIndex: Byte): Cardinal; // Renombrado y uso
    procedure CalculateExpectedSecWebSocketAccept(const AClientKey: string); // Renombrado de SetSecWebSocketAcceptExpectedResponse
    procedure PerformReadFromWebSocket; virtual; // Renombrado de ReadFromWebSocket
    procedure SendCloseFrame; // Renombrado de SendCloseHandshake
    procedure InternalHandleException(const AException: Exception); // Renombrado de HandleException
    procedure StartHeartBeatTask; // Renombrado de StartHeartBeat
    procedure DoClose; // Renombrado de Close para evitar colisión con TIdTCPClient.Close (aunque es override)

    // Helper para ReplaceOnlyFirst si no está en uLib.Base
    function ReplaceFirst(const S, OldPattern, NewPattern: string): string;

    constructor CreateWithURL(const AURL: string); reintroduce; // Renombrado de Create
  protected
    // Exponer eventos a través de propiedades es una buena práctica
    property OnMessage: TEventListener read FOnMessage write FOnMessage;
    property OnOpen: TEventListener read FOnOpen write FOnOpen;
    property OnCloseEvent: TNotifyEvent read FOnClose write FOnClose; // Renombrado para evitar confusión con método Close
    property OnErrorEvent: TEventListenerError read FOnError write FOnError; // Renombrado
    property OnHeartBeatTimerEvent: TNotifyEvent read FOnHeartBeatTimer write FOnHeartBeatTimer; // Renombrado
    property OnUpgradeEvent: TNotifyEvent read FOnUpgrade write FOnUpgrade; // Renombrado
  public
    class function New(const AURL: string): TSocketClient; // Factory method
    property HeartBeatInterval: Cardinal read FHeartBeatInterval write FHeartBeatInterval;
    property AutoCreateSSLHandler: Boolean read FAutoCreateHandler write FAutoCreateHandler; // Renombrado
    function IsConnected: Boolean; override; // Renombrado de Connected para Delphi
    procedure Connect; override; // Conectar e iniciar handshake WebSocket
    procedure DisconnectSocket; // Nuevo método para desconexión explícita, llama a DoClose
    procedure SetCustomHeader(const AKey: string; const AValue: string); // Renombrado de SetHeader
    procedure AddEventListener(const AEventType: TEventType; const AEvent: TEventListener); overload;
    procedure AddEventListener(const AEventType: TEventType; const AEvent: TEventListenerError); overload;
    procedure AddEventListener(const AEventType: TEventType; const AEvent: TNotifyEvent); overload;
    procedure SetRequestedSubProtocol(const AValue: string); // Renombrado de SetSubProtocol
    procedure SendText(const AMessage: string); overload; // Renombrado de Send
    procedure SendBinary(const AMessage: RawByteString); overload; // Renombrado de Send
    procedure SendJSON(const AJSONObject: TJSONObject; const AOwnsObject: Boolean = True); overload; // Renombrado y AOwns
    destructor Destroy; override;
  end;

// Const
// TOpCodeByte: array[TOperationCode] of Byte = ($0, $1, $2, $8, $9, $A); // Ya usado por TOperationCodeHelper

implementation

// Helper para TOperationCode
function TOperationCodeHelper.ToByte: Byte;
begin
  case Self of
    TOperationCode.CONTINUE: Result := $0;
    TOperationCode.TEXT_FRAME: Result := $1;
    TOperationCode.BINARY_FRAME: Result := $2;
    TOperationCode.CONNECTION_CLOSE: Result := $8;
    TOperationCode.PING: Result := $9;
    TOperationCode.PONG: Result := $A;
  else
    Result := $0; // Default o error
  end;
end;

// TSocketClient implementation

constructor TSocketClient.CreateWithURL(const AURL: string);
begin
  inherited Create(nil); // TIdTCPClient no tiene owner en su constructor público
  FInternalLock := TCriticalSection.Create;
  FAutoCreateHandler := True;
  FHeartBeatInterval := 30000; // 30 segundos
  FURL := AURL;
  FSubProtocol := EmptyStr;
  FHeader := TDictionary<string, string>.Create;
  Randomize; // Para la Masking Key
  FUpgraded := False;
  FClosingEventLocalHandshake := False;
end;

class function TSocketClient.New(const AURL: string): TSocketClient;
begin
  Result := TSocketClient.CreateWithURL(AURL);
end;

destructor TSocketClient.Destroy;
var
  TasksToWaitFor: TArray<ITask>;
begin
  // Señalar a las tareas que deben terminar y esperar
  DoClose; // Inicia el proceso de cierre, establece FClosingEventLocalHandshake

  // Construir array de tareas activas
  SetLength(TasksToWaitFor, 0);
  if Assigned(FTaskReadFromWebSocket) and (FTaskReadFromWebSocket.Status <> TTaskStatus.Completed) then
    System.Generics.Collections.TArray.Add<ITask>(TasksToWaitFor, FTaskReadFromWebSocket);
  if Assigned(FTaskHeartBeat) and (FTaskHeartBeat.Status <> TTaskStatus.Completed) then
    System.Generics.Collections.TArray.Add<ITask>(TasksToWaitFor, FTaskHeartBeat);

  if Length(TasksToWaitFor) > 0 then
  begin
    // Esperar un tiempo prudencial, no indefinidamente para evitar bloqueos en la destrucción
    TTask.WaitForAll(TasksToWaitFor, 5000); // Esperar hasta 5 segundos
  end;

  FTaskReadFromWebSocket := nil; // Liberar referencias a interfaces de tareas
  FTaskHeartBeat := nil;

  // FIOHandler se libera por TIdTCPClient si fue asignado a su propiedad IOHandler.
  // Si FAutoCreateHandler es true y creamos uno, TIdTCPClient lo posee si se asignó a Self.IOHandler.
  // No es necesario liberarlo explícitamente aquí si se asignó a Self.IOHandler.
  // if FAutoCreateHandler and Assigned(FIOHandler) then // Esta lógica es de TIdTCPClient
  //   FreeAndNil(FIOHandler);

  FreeAndNil(FHeader);
  FreeAndNil(FInternalLock);
  inherited;
end;

procedure TSocketClient.SetCustomHeader(const AKey: string; const AValue: string);
begin
  FInternalLock.Acquire;
  try
    FHeader.AddOrSetValue(AKey, AValue);
  finally
    FInternalLock.Release;
  end;
end;

procedure TSocketClient.AddEventListener(const AEventType: TEventType; const AEvent: TNotifyEvent);
begin
  case AEventType of
    TEventType.CLOSE:
      begin
        // if Assigned(FOnClose) then
        //   raise Exception.Create('The CLOSE event listener is already assigned!');
        FOnClose := AEvent; // Permitir reasignación
      end;
    TEventType.UPGRADE:
      begin
        // if Assigned(FOnUpgrade) then
        //   raise Exception.Create('The UPGRADE event listener is already assigned!');
        FOnUpgrade := AEvent;
      end;
    TEventType.HEART_BEAT_TIMER:
      begin
        // if Assigned(FOnHeartBeatTimer) then
        //   raise Exception.Create('The HEART_BEAT_TIMER event listener is already assigned!');
        FOnHeartBeatTimer := AEvent;
      end;
  else
    raise Exception.Create('Event type not compatible with TNotifyEvent listener for WebSocket.');
  end;
end;

procedure TSocketClient.AddEventListener(const AEventType: TEventType; const AEvent: TEventListenerError);
begin
  if (AEventType <> TEventType.ERROR) then
    raise Exception.Create('Event type not compatible with TEventListenerError for WebSocket.');
  // if Assigned(FOnError) then
  //   raise Exception.Create('The ERROR event listener is already assigned!');
  FOnError := AEvent; // Permitir reasignación
end;

procedure TSocketClient.AddEventListener(const AEventType: TEventType; const AEvent: TEventListener);
begin
  case AEventType of
    TEventType.OPEN:
      begin
        // if Assigned(FOnOpen) then
        //   raise Exception.Create('The OPEN event listener is already assigned!');
        FOnOpen := AEvent; // Permitir reasignación
      end;
    TEventType.&MESSAGE: // Usar &MESSAGE si es keyword, o solo MESSAGE
      begin
        // if Assigned(FOnMessage) then
        //   raise Exception.Create('The MESSAGE event listener is already assigned!');
        FOnMessage := AEvent;
      end;
  else
    raise Exception.Create('Event type not compatible with TEventListener for WebSocket.');
  end;
end;

function TSocketClient.GetBitFromCardinal(const AValue: Cardinal; const ABitIndex: Byte): Boolean;
begin
  Result := (AValue and (1 shl ABitIndex)) <> 0;
end;

// function TSocketClient.SetBitInCardinal(const AValue: Cardinal; const ABitIndex: Byte): Cardinal;
// begin
//   Result := AValue or (1 shl ABitIndex);
// end;

// function TSocketClient.ClearBitFromCardinal(const AValue: Cardinal; const ABitIndex: Byte): Cardinal;
// begin
//   Result := AValue and not (1 shl ABitIndex);
// end;

procedure TSocketClient.DoClose;
var
  AlreadyClosing: Boolean;
begin
  FInternalLock.Acquire;
  AlreadyClosing := FClosingEventLocalHandshake;
  if not AlreadyClosing then
     FClosingEventLocalHandshake := True; // Indicar que el cierre es iniciado localmente
  FInternalLock.Release;

  if AlreadyClosing and Connected then // Si ya se está cerrando y aún conectado, podría ser reentrada
  begin
     LogMessage('TSocketClient.DoClose: Already in closing process.', logDebug);
     Exit;
  end;

  if not Connected then
  begin
    LogMessage('TSocketClient.DoClose: Not connected.', logDebug);
    // Asegurar que las tareas se cancelen si aún no lo están
    if Assigned(FTaskReadFromWebSocket) then FTaskReadFromWebSocket.Cancel;
    if Assigned(FTaskHeartBeat) then FTaskHeartBeat.Cancel;
    Exit;
  end;

  LogMessage('TSocketClient.DoClose: Initiating WebSocket closure.', logInfo);
  try
    if Assigned(FTaskReadFromWebSocket) then
       FTaskReadFromWebSocket.Cancel;
    if Assigned(FTaskHeartBeat) then
       FTaskHeartBeat.Cancel;

    SendCloseFrame; // Enviar frame de cierre WebSocket
    if Assigned(IOHandler) then // IOHandler es propiedad de TIdTCPClient
    begin
      IOHandler.InputBuffer.Clear;
    end;
    Disconnect; // Desconectar el socket TCP
    LogMessage('TSocketClient.DoClose: TCP Disconnected.', logDebug);

    if Assigned(FOnClose) then
    begin
      try
        FOnClose(Self);
      except
        on E: Exception do
          LogMessage(Format('TSocketClient: Exception in OnClose event handler: %s', [E.Message]), logError);
      end;
    end;
  except
    on E: Exception do
      InternalHandleException(E); // Usar el manejador interno para errores durante el cierre
  end;
  FUpgraded := False; // Marcar como no actualizado después del cierre
end;

procedure TSocketClient.DisconnectSocket;
begin
  DoClose;
end;

// Helper para reemplazar solo la primera ocurrencia
function TSocketClient.ReplaceFirst(const S, OldPattern, NewPattern: string): string;
var
  PosPattern: Integer;
begin
  PosPattern := Pos(OldPattern, S);
  if PosPattern > 0 then
  begin
    Result := Copy(S, 1, PosPattern - 1) + NewPattern + Copy(S, PosPattern + Length(OldPattern), MaxInt);
  end
  else
    Result := S;
end;

procedure TSocketClient.Connect;
var
  LURI: TIdURI;
  LSecure: Boolean;
  LHeaderKey: string; // Para iterar el diccionario de headers
  LInitialHeaders: TStringList;
begin
  if IsConnected then // Usa la propiedad IsConnected
    raise Exception.Create('WebSocket is already connected or connecting.');

  LURI := TIdURI.Create(FURL);
  LInitialHeaders := TStringList.Create;
  try
    FInternalLock.Acquire; // Proteger acceso a FClosingEventLocalHandshake y otros
    FClosingEventLocalHandshake := False;
    FUpgraded := False;
    FInternalLock.Release;

    Self.Host := LURI.Host; // Propiedad de TIdTCPClient
    LSecure := SameText(LURI.Protocol, 'wss');

    if LURI.PortText.IsEmpty then
      Self.Port := IfThen(LSecure, 443, 80) // Propiedad de TIdTCPClient
    else
      Self.Port := StrToIntDef(LURI.PortText, IfThen(LSecure, 443, 80));

    if LSecure then
    begin
      if not Assigned(Self.IOHandler) or not (Self.IOHandler is TIdSSLIOHandlerSocketOpenSSL) then
      begin
        if FAutoCreateSSLHandler then // Renombrada la propiedad
        begin
          var LSSLIOHandler := TIdSSLIOHandlerSocketOpenSSL.Create(Self); // Self como owner
          LSSLIOHandler.SSLOptions.Mode := TIdSSLMode.sslmClient;
          // Configurar versiones TLS seguras. Evitar SSLv2, SSLv3.
          LSSLIOHandler.SSLOptions.SSLVersions := [TLS1_2_PROTOCOL_VERSION, TLS1_1_PROTOCOL_VERSION, TLS1_PROTOCOL_VERSION]; //sslVersionsOpenSSL.pas
          // LSSLIOHandler.SSLOptions.Method := sslvTLSv1_2; // O dejar que negocie la mejor de las SSLVersions
          LSSLIOHandler.PassThrough := False;
          Self.IOHandler := LSSLIOHandler; // Asignar a la propiedad de TIdTCPClient
        end
        else
          raise Exception.Create('Secure WebSocket (wss) requires a TIdSSLIOHandlerSocketOpenSSL IOHandler to be assigned.');
      end;
    end
    else // No es wss, asegurar que no haya un IOHandler SSL
    begin
       if Assigned(Self.IOHandler) and (Self.IOHandler is TIdSSLIOHandlerSocketOpenSSL) then
       begin
          LogMessage('TSocketClient.Connect: URL is not wss, but SSLIOHandler is present. Removing it.', logWarning);
          FreeAndNil(Self.IOHandler); // O Self.IOHandler := nil; si TIdTCPClient lo libera.
                                     // Es más seguro que TIdTCPClient lo gestione si se asignó a su propiedad.
                                     // Si FAutoCreateSSLHandler lo creó y no se asignó, hay que liberarlo.
                                     // Si se asignó, TIdTCPClient lo libera.
       end;
    end;

    LogMessage(Format('TSocketClient: Attempting TCP connection to %s:%d', [Self.Host, Self.Port]), logDebug);
    inherited Connect; // Conectar el socket TCP (TIdTCPClient.Connect)
    LogMessage('TSocketClient: TCP connection established. Starting WebSocket handshake.', logDebug);

    // Construir Headers HTTP para el Upgrade
    var LRequestPath := IfThen(LURI.Path.Trim.IsEmpty, '/', LURI.Path) + LURI.Document;
    if LURI.Params <> '' then
      LRequestPath := LRequestPath + '?' + LURI.Params;

    LInitialHeaders.Add(Format('GET %s HTTP/1.1', [LRequestPath]));
    LInitialHeaders.Add(Format('Host: %s', [LURI.Host])); // Host puede incluir puerto si no es estándar

    FInternalLock.Acquire;
    try
      for LHeaderKey in FHeader.Keys do // Añadir cabeceras personalizadas
        LInitialHeaders.Add(Format('%s: %s', [LHeaderKey, FHeader.ValueOrDefault(LHeaderKey, '')]));
    finally
      FInternalLock.Release;
    end;

    LInitialHeaders.Add('Connection: Upgrade'); // Corregido: Connection: Upgrade (no keep-alive)
    LInitialHeaders.Add('Upgrade: websocket'); // Corregido: websocket en minúsculas
    LInitialHeaders.Add('Sec-WebSocket-Version: 13');
    CalculateExpectedSecWebSocketAccept(GenerateWebSocketKey); // Genera la clave y calcula la respuesta esperada
    LInitialHeaders.Add(Format('Sec-WebSocket-Key: %s', [FSecWebSocketKey])); // FSecWebSocketKey debe ser un campo donde GenerateWebSocketKey lo guarde

    if not FSubProtocol.Trim.IsEmpty then
      LInitialHeaders.Add(Format('Sec-WebSocket-Protocol: %s', [FSubProtocol]));
    LInitialHeaders.Add(''); // Línea vacía final

    // Enviar los headers del handshake
    Self.Socket.Write(LInitialHeaders); // Socket es TIdIOHandler.Socket
    LogMessage('TSocketClient: Handshake request sent.', logDebug);

    // Leer la respuesta del handshake y validar
    if not IsValidWebSocketConnection then // Esto lee la respuesta y actualiza FUpgraded
    begin
      // IsValidWebSocketConnection ya debería haber manejado el error o cerrado la conexión.
      // Si no lo hizo, forzar desconexión.
      if IsConnected then DisconnectSocket;
      raise Exception.Create('WebSocket handshake failed or server did not upgrade connection.');
    end;

    // Si FUpgraded es True, el handshake fue exitoso
    if FUpgraded then
    begin
      LogMessage('TSocketClient: WebSocket handshake successful. Connection upgraded.', logInfo);
      if Assigned(FOnOpen) then
      begin
        try FOnOpen(''); except on E:Exception do InternalHandleException(E); end; // OnOpen no suele pasar texto
      end;
      if Assigned(FOnUpgradeEvent) then // Usar el nombre de propiedad correcto
      begin
        try FOnUpgradeEvent(Self); except on E:Exception do InternalHandleException(E); end;
      end;

      // Iniciar tareas de lectura y heartbeat
      PerformReadFromWebSocket; // Inicia la tarea de lectura
      StartHeartBeatTask;     // Inicia la tarea de heartbeat
    end
    else // No debería llegar aquí si IsValidWebSocketConnection lanzó excepción en fallo
    begin
      if IsConnected then DisconnectSocket;
      raise Exception.Create('WebSocket connection failed: Handshake was not successful (FUpgraded is false).');
    end;

  except
    on E: Exception do
    begin
      LogMessage(Format('TSocketClient.Connect: Exception - %s: %s', [E.ClassName, E.Message]), logError);
      if IsConnected then
         DisconnectSocket; // Asegurar desconexión en caso de error
      InternalHandleException(E); // Propagar o manejar el error
      raise; // Re-lanzar para que el llamador sepa del fallo
    end;
  finally
    LURI.Free;
    LInitialHeaders.Free;
  end;
end;

function TSocketClient.IsConnected: Boolean;
begin
  // Una conexión WebSocket está "conectada" si el socket TCP está conectado Y el handshake de Upgrade fue exitoso.
  FInternalLock.Acquire;
  try
    Result := inherited Connected and FUpgraded; // inherited Connected es de TIdTCPClient
  finally
    FInternalLock.Release;
  end;
end;

// Renombrado de SetSecWebSocketAcceptExpectedResponse
procedure TSocketClient.CalculateExpectedSecWebSocketAccept(const AClientKey: string);
var
  LHash: TIdHashSHA1; // SHA1 es requerido por el RFC6455 para el handshake
  LKeyWithGUID: AnsiString; // Debe ser AnsiString para el HashBytes
begin
  // FSecWebSocketKey (nuevo campo necesario) debe almacenar AClientKey
  Self.FSecWebSocketKey := AClientKey; // Asumir que FSecWebSocketKey es un campo de la clase

  LKeyWithGUID := AnsiString(AClientKey + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11');
  LHash := TIdHashSHA1.Create;
  try
    // HashBytes devuelve TIdBytes, EncodeBytes lo convierte a Base64 string
    FSecWebSocketAcceptExpectedResponse := TIdEncoderMIME.EncodeBytes(LHash.HashBytes(ToBytes(LKeyWithGUID)));
  finally
    LHash.Free;
  end;
end;

// Renombrado de IsValidWebSocket
function TSocketClient.IsValidWebSocketConnection: Boolean;
var
  LResponseHeaders: TStringList;
  LLine: string;
  LHttpStatusLine: string;
begin
  Result := False;
  FUpgraded := False; // Resetear FUpgraded
  LResponseHeaders := TStringList.Create;
  try
    LogMessage('TSocketClient: Reading handshake response...', logDebug);
    // Leer la línea de estado HTTP (ej. "HTTP/1.1 101 Switching Protocols")
    LHttpStatusLine := Trim(Self.Socket.ReadLn(#10, IndyInfiniteTimeout, MaxLineLength, True)); // True para incluir #10
    LogMessage(Format('TSocketClient: Status Line: "%s"', [LHttpStatusLine]), logSpam);
    LResponseHeaders.Add(LHttpStatusLine);

    // Leer el resto de los headers hasta la línea vacía
    repeat
      LLine := Trim(Self.Socket.ReadLn(#10, IndyInfiniteTimeout, MaxLineLength, True));
      LogMessage(Format('TSocketClient: Header Line: "%s"', [LLine]), logSpam);
      if LLine = '' then Break; // Fin de los headers
      LResponseHeaders.Add(LLine);
    until False;

    if ValidateHandshakeHeaders(LResponseHeaders) then // ValidateHandshakeHeaders verifica el código 101 y las cabeceras Upgrade
    begin
      FUpgraded := True;
      Result := True;
      LogMessage('TSocketClient: Handshake headers validated successfully. FUpgraded=True.', logDebug);
    end
    else
    begin
      LogMessage('TSocketClient: Handshake header validation failed.', logWarning);
      // ValidateHandshakeHeaders debería haber lanzado una excepción si algo es incorrecto.
      // Si no lo hizo pero devolvió False, es un fallo silencioso.
      raise Exception.Create('Invalid WebSocket handshake response headers.');
    end;
  except
    on E: Exception do
    begin
      LogMessage(Format('TSocketClient.IsValidWebSocketConnection: Exception - %s: %s', [E.ClassName, E.Message]), logError);
      InternalHandleException(E); // Esto podría llamar a DoClose
      Result := False;
    end;
  end;
  FreeAndNil(LResponseHeaders);
end;

// Renombrado de IsValidHeaders
function TSocketClient.ValidateHandshakeHeaders(const AHeaders: TStrings): Boolean;
var
  LConnectionHeader, LUpgradeHeader, LAcceptHeader: string;
begin
  Result := False;
  if (AHeaders.Count = 0) then
  begin
    LogMessage('ValidateHandshakeHeaders: No headers received.', logError);
    Exit;
  end;

  // Verificar línea de estado HTTP/1.1 101
  // TIdHTTPResponseInfo.ProcessResponse lo hace más robusto, pero aquí leemos manualmente.
  if not AHeaders[0].Contains('HTTP/1.1 101') then // Podría ser más específico como StartsText
  begin
    if AHeaders[0].Contains('HTTP/1.1') then // Es una respuesta HTTP, pero no 101
      raise Exception.Create('WebSocket Handshake Error: Server responded with HTTP status: ' + AHeaders[0].Substring(9))
    else
      raise Exception.Create('WebSocket Handshake Error: Invalid HTTP response status line: ' + AHeaders[0]);
  end;

  // Indy TStrings ya separa Name=Value con NameValueSeparator, que por defecto es '='.
  // Para HTTP Headers, es ': '.
  AHeaders.NameValueSeparator := ':'; // Asegurar el separador correcto

  LUpgradeHeader := Trim(AHeaders.Values['Upgrade']);
  LConnectionHeader := Trim(AHeaders.Values['Connection']);
  LAcceptHeader := Trim(AHeaders.Values['Sec-WebSocket-Accept']);

  if not SameText(LUpgradeHeader, 'websocket') then // RFC6455 especifica 'websocket' en minúsculas
    raise Exception.Create('WebSocket Handshake Error: Missing or invalid "Upgrade: websocket" header. Got: ' + LUpgradeHeader);

  if PosText('upgrade', LConnectionHeader) = 0 then // Connection header debe contener 'Upgrade' (case-insensitive)
    raise Exception.Create('WebSocket Handshake Error: Missing or invalid "Connection: Upgrade" header. Got: ' + LConnectionHeader);

  if not SameText(LAcceptHeader, FSecWebSocketAcceptExpectedResponse) then
    raise Exception.Create(Format('WebSocket Handshake Error: Invalid Sec-WebSocket-Accept. Expected: "%s", Got: "%s"',
      [FSecWebSocketAcceptExpectedResponse, LAcceptHeader]));

  // Verificar subprotocolo si se solicitó
  if FSubProtocol.Trim <> '' then
  begin
    var ServerSubProtocol := Trim(AHeaders.Values['Sec-WebSocket-Protocol']);
    if SameText(ServerSubProtocol, FSubProtocol) then
      LogMessage(Format('WebSocket subprotocol "%s" agreed by server.', [FSubProtocol]), logInfo)
    else
    begin
      LogMessage(Format('WebSocket Handshake Warning: Requested subprotocol "%s", server responded with "%s" or none. Continuing without agreed subprotocol.',
        [FSubProtocol, ServerSubProtocol]), logWarning);
      // El RFC permite esto, pero la aplicación cliente podría querer tratarlo como un error.
      // FSubProtocol = ServerSubProtocol; // Actualizar al protocolo del servidor o dejarlo como estaba.
    end;
  end;

  Result := True; // Si todas las validaciones pasan
end;

// Renombrado de EncodeFrame
function TSocketClient.EncodeWebSocketFrame(const AMessage: RawByteString; const AOperationCode: TOperationCode): TIdBytes;
var
  LFinBit, LMaskBit: Byte;
  LMaskingKey: array[0..3] of Byte; // Máscara de 4 bytes
  LPayloadLengthPart: Byte;
  LHeaderBytes: TBytes; // Para construir la cabecera
  LMessageBytes: TBytes; // Bytes del mensaje (UTF-8 para texto)
  I: Integer;
  LPayloadLength: Int64;
begin
  if AOperationCode = TOperationCode.TEXT_FRAME then
    LMessageBytes := TEncoding.UTF8.GetBytes(string(AMessage)) // Convertir string a UTF-8 bytes
  else
    LMessageBytes := BytesOf(AMessage); // Para BINARY_FRAME, AMessage ya son RawByteString (bytes)

  LPayloadLength := Length(LMessageBytes);

  // FIN bit (bit 7 del primer byte) = 1 (este es el frame final o único)
  LFinBit := $80; // 10000000b

  // Primer byte: FIN + RSV1-3 (000) + Opcode
  SetLength(LHeaderBytes, 2); // Mínimo 2 bytes de cabecera (FIN+Opcode, Mask+Len)
  LHeaderBytes[0] := LFinBit or TOpCodeByte[AOperationCode]; // Usar el array constante o TOperationCodeHelper

  // Segundo byte: Mask bit (bit 7) + Payload length
  LMaskBit := $80; // Cliente DEBE enmascarar

  if LPayloadLength <= 125 then
  begin
    LPayloadLengthPart := Byte(LPayloadLength);
    LHeaderBytes[1] := LMaskBit or LPayloadLengthPart;
  end
  else if LPayloadLength <= $FFFF then // Max 65535 (Word)
  begin
    LPayloadLengthPart := 126;
    LHeaderBytes[1] := LMaskBit or LPayloadLengthPart;
    SetLength(LHeaderBytes, Length(LHeaderBytes) + 2); // Añadir 2 bytes para la longitud extendida
    LHeaderBytes[2] := Byte(LPayloadLength shr 8);   // Big-endian
    LHeaderBytes[3] := Byte(LPayloadLength and $FF);
  end
  else // LPayloadLength > 65535 (Int64)
  begin
    LPayloadLengthPart := 127;
    LHeaderBytes[1] := LMaskBit or LPayloadLengthPart;
    SetLength(LHeaderBytes, Length(LHeaderBytes) + 8); // Añadir 8 bytes para la longitud extendida
    // Escribir Int64 en big-endian
    for I := 0 to 7 do // Escribir los 8 bytes del Int64 en orden big-endian
      LHeaderBytes[2 + I] := Byte(LPayloadLength shr ((7 - I) * 8));
  end;

  // Generar Masking Key (4 bytes)
  for I := 0 to 3 do
    LMaskingKey[I] := Byte(Random(256));

  // Añadir Masking Key a la cabecera
  var CurrentHeaderLength := Length(LHeaderBytes);
  SetLength(LHeaderBytes, CurrentHeaderLength + 4);
  Move(LMaskingKey[0], LHeaderBytes[CurrentHeaderLength], 4);

  // Enmascarar el payload
  for I := 0 to Pred(Length(LMessageBytes)) do
    LMessageBytes[I] := LMessageBytes[I] xor LMaskingKey[I mod 4];

  // Combinar cabecera y payload enmascarado
  Result := BytesCat(LHeaderBytes, LMessageBytes);
end;

// Renombrado de ReadFromWebSocket
procedure TSocketClient.PerformReadFromWebSocket;
var
  LFinBit, LRsvBits, LMaskBit: Boolean;
  LPayloadLen64: Int64;
  LMaskingKey: array[0..3] of Byte;
  LPayload: TIdBytes;
  LReceivedOpCode: TOperationCode;
  LByte1, LByte2: Byte;
  I: Integer;
  LDecodedText: string;
  LRawPayload: RawByteString;
begin
  if not IsValidWebSocketConnection then // Llama a IsValidWebSocketConnection para asegurar el handshake
  begin
     LogMessage('PerformReadFromWebSocket: WebSocket connection not (or no longer) valid. Read task will not start.', logWarning);
     Exit;
  end;

  if not IsConnected then // Doble chequeo
  begin
    LogMessage('PerformReadFromWebSocket: Not connected. Read task will not start.', logWarning);
    Exit;
  end;

  LogMessage('PerformReadFromWebSocket: Starting read task.', logDebug);
  FTaskReadFromWebSocket := TTask.Run(
    procedure
    begin
      try
        while IsConnected and (not FClosingEventLocalHandshake) do // Continuar mientras esté conectado y no cerrando
        begin
          // Leer el primer byte (FIN, RSV, Opcode)
          LByte1 := Self.Socket.ReadByte(IndyInfiniteTimeout); // Bloquea hasta que haya datos

          LFinBit := GetBitFromCardinal(LByte1, 7);
          // LRsvBits := GetBitFromCardinal(LByte1, 6) or GetBitFromCardinal(LByte1, 5) or GetBitFromCardinal(LByte1, 4);
          // if LRsvBits then raise Exception.Create('RSV bits must be 0'); // Validación estricta
          LReceivedOpCode := TOperationCode(LByte1 and $0F); // Extraer Opcode (últimos 4 bits)

          // Leer el segundo byte (Mask, Payload len)
          LByte2 := Self.Socket.ReadByte(IndyInfiniteTimeout);
          LMaskBit := GetBitFromCardinal(LByte2, 7); // El servidor NO DEBE enmascarar frames al cliente

          if LMaskBit then // Si el servidor enmascara, es un error de protocolo
          begin
            InternalHandleException(Exception.Create('Protocol Error: Server sent a masked frame.'));
            Break; // Salir del bucle de lectura
          end;

          LPayloadLen64 := LByte2 and $7F; // Payload length (primeros 7 bits)

          if LPayloadLen64 = 126 then // Si es 126, los siguientes 2 bytes son la longitud
            LPayloadLen64 := ReadWordNetwork(Self.Socket.ReadInt16(False)) // Leer UInt16 big-endian
          else if LPayloadLen64 = 127 then // Si es 127, los siguientes 8 bytes son la longitud
            LPayloadLen64 := ReadInt64Network(Self.Socket.ReadInt64(False)); // Leer UInt64 big-endian

          // Leer el payload
          if LPayloadLen64 > 0 then
            LPayload := Self.Socket.ReadBytes(LPayloadLen64, False)
          else
            SetLength(LPayload, 0);

          // Procesar el frame según el Opcode
          case LReceivedOpCode of
            TOperationCode.TEXT_FRAME:
            begin
              LDecodedText := IndyTextEncoding_UTF8.GetString(LPayload);
              LogMessage(Format('WebSocket Message Received (TEXT): "%s"', [LDecodedText]), logDebug);
              if Assigned(FOnMessage) then
              begin
                try FOnMessage(LDecodedText); except on E:Exception do InternalHandleException(E); end;
              end;
            end;
            TOperationCode.BINARY_FRAME:
            begin
              SetString(LRawPayload, PAnsiChar(Pointer(LPayload)), Length(LPayload)); // Copiar bytes a RawByteString
              LogMessage(Format('WebSocket Message Received (BINARY): %d bytes', [Length(LRawPayload)]), logDebug);
              if Assigned(FOnMessage) then // El mismo handler para texto y binario, el cliente diferencia o se usan handlers separados
              begin
                try FOnMessage(LRawPayload); except on E:Exception do InternalHandleException(E); end;
              end;
            end;
            TOperationCode.CONNECTION_CLOSE:
            begin
              LogMessage('WebSocket CLOSE frame received from server.', logInfo);
              // El servidor quiere cerrar. Si no hemos iniciado el cierre, responder.
              FInternalLock.Acquire;
              var IsLocalClose := FClosingEventLocalHandshake;
              FInternalLock.Release;

              if not IsLocalClose then // Si el cierre no fue iniciado por nosotros
                 SendCloseFrame; // Enviar un frame de cierre en respuesta

              DoClose; // Proceder a cerrar la conexión localmente
              Break; // Salir del bucle de lectura
            end;
            TOperationCode.PING:
            begin
              LogMessage('WebSocket PING frame received. Sending PONG.', logDebug);
              // Enviar PONG con el mismo payload del PING
              FInternalLock.Acquire;
              try
                Self.Socket.Write(EncodeWebSocketFrame(BytesToRawByteString(LPayload), TOperationCode.PONG));
              finally
                FInternalLock.Release;
              end;
            end;
            TOperationCode.PONG:
            begin
              LogMessage('WebSocket PONG frame received.', logDebug);
              // Actualizar el temporizador de último PONG recibido si se está usando un mecanismo de timeout de heartbeat
            end;
            TOperationCode.CONTINUE:
            begin
              LogMessage('WebSocket CONTINUE frame received (fragmentation not fully supported in this example).', logWarning);
              // La fragmentación requeriría ensamblar frames.
            end;
          else // Opcodes desconocidos o reservados
            LogMessage(Format('WebSocket: Received frame with unknown or reserved opcode: $%X', [Byte(LReceivedOpCode)]), logWarning);
            // Podría ser necesario cerrar la conexión por error de protocolo.
          end;
        end; // while IsConnected
      except
        on E: EIdConnClosedGracefully do
        begin
          LogMessage('WebSocket read task: Connection closed gracefully by peer.', logInfo);
          DoClose; // Asegurar que nuestro estado de cierre se actualice
        end;
        on E: EIdSocketError do // Errores de socket (ej. conexión reseteada)
        begin
          LogMessage(Format('WebSocket read task: Socket Error %d: %s', [E.LastError, E.Message]), logError);
          InternalHandleException(E); // Esto llamará a DoClose
        end;
        on E: Exception do // Otras excepciones durante la lectura
        begin
          LogMessage(Format('WebSocket read task: Exception - %s: %s', [E.ClassName, E.Message]), logError);
          InternalHandleException(E); // Esto llamará a DoClose
        end;
      end;
      LogMessage('WebSocket read task finished.', logDebug);
    end); // TTask.Run
end;

// Renombrado de SendCloseHandshake
procedure TSocketClient.SendCloseFrame;
begin
  if not IsConnected then Exit; // No enviar si no está conectado (o ya no está FUpgraded)

  LogMessage('Sending WebSocket CLOSE frame...', logDebug);
  FInternalLock.Acquire;
  try
    // El RFC6455 permite un cuerpo opcional con código de estado y razón.
    // Por simplicidad, enviamos un frame de cierre sin cuerpo.
    Self.Socket.Write(EncodeWebSocketFrame('', TOperationCode.CONNECTION_CLOSE));
  except
    on E: Exception do
      LogMessage(Format('Error sending WebSocket CLOSE frame: %s', [E.Message]), logError);
      // No llamar a InternalHandleException aquí para evitar bucles si el error es por la conexión ya cerrada.
  finally
    FInternalLock.Release;
  end;
  // TThread.Sleep(200); // Este Sleep podría ser problemático, mejor manejar la respuesta del servidor.
end;

procedure TSocketClient.SetRequestedSubProtocol(const AValue: string);
begin
  FSubProtocol := AValue;
end;

// Renombrado de Send
procedure TSocketClient.SendText(const AMessage: string);
begin
  if not IsConnected then // Usa la propiedad IsConnected (que verifica FUpgraded)
    raise Exception.Create('WebSocket not connected or handshake not completed.');
  LogMessage(Format('Sending TEXT frame: "%s"', [Copy(AMessage,1,100)]), logDebug);
  FInternalLock.Acquire;
  try
    Self.Socket.Write(EncodeWebSocketFrame(RawByteString(AMessage), TOperationCode.TEXT_FRAME));
  finally
    FInternalLock.Release;
  end;
end;

// Renombrado de Send
procedure TSocketClient.SendBinary(const AMessage: RawByteString);
begin
  if not IsConnected then
    raise Exception.Create('WebSocket not connected or handshake not completed.');
  LogMessage(Format('Sending BINARY frame: %d bytes', [Length(AMessage)]), logDebug);
  FInternalLock.Acquire;
  try
    Self.Socket.Write(EncodeWebSocketFrame(AMessage, TOperationCode.BINARY_FRAME));
  finally
    FInternalLock.Release;
  end;
end;

// Renombrado de Send
procedure TSocketClient.SendJSON(const AJSONObject: TJSONObject; const AOwnsObject: Boolean = True);
begin
  if not Assigned(AJSONObject) then Exit;
  try
    SendText(AJSONObject.ToJSON); // Enviar el JSON como un frame de texto UTF-8
  finally
    if AOwnsObject then
      FreeAndNil(AJSONObject);
  end;
end;

// Renombrado de StartHeartBeat
procedure TSocketClient.StartHeartBeatTask;
var
  LNextHeartBeat: TDateTime;
begin
  if (FHeartBeatInterval = 0) or (not IsConnected) then Exit; // No iniciar si intervalo es 0 o no conectado

  LogMessage(Format('Starting WebSocket HeartBeat task. Interval: %d ms.', [FHeartBeatInterval]), logDebug);
  FTaskHeartBeat := TTask.Run(
    procedure
    begin
      LNextHeartBeat := Now + (FHeartBeatInterval / MSecsPerDay);
      try
        while IsConnected and (not FClosingEventLocalHandshake) do
        begin
          if Now >= LNextHeartBeat then
          begin
            LogMessage('HeartBeat: Sending PING.', logSpam);
            FInternalLock.Acquire;
            try
              if IsConnected then // Doble chequeo dentro del lock
                Self.Socket.Write(EncodeWebSocketFrame('', TOperationCode.PING)); // PING sin payload
            except
              on E: Exception do
                LogMessage(Format('HeartBeat: Error sending PING: %s', [E.Message]), logError);
                // No llamar a InternalHandleException aquí para no cerrar por un fallo de PING,
                // a menos que sea un error de socket grave. El bucle principal de lectura lo detectará.
            finally
              FInternalLock.Release;
            end;

            if Assigned(FOnHeartBeatTimerEvent) then // Usar nombre de propiedad correcto
            begin
              try FOnHeartBeatTimerEvent(Self); except on E:Exception do InternalHandleException(E); end;
            end;
            LNextHeartBeat := Now + (FHeartBeatInterval / MSecsPerDay);
          end;
          TThread.Sleep(Min(1000, FHeartBeatInterval div 3 + 1)); // Dormir un tercio del intervalo o 1s
        end;
      except
        on E: Exception do // Excepción en la tarea de HeartBeat
        begin
          LogMessage(Format('WebSocket HeartBeat task: Exception - %s: %s', [E.ClassName, E.Message]), logError);
          InternalHandleException(E); // Esto podría cerrar la conexión
        end;
      end;
      LogMessage('WebSocket HeartBeat task finished.', logDebug);
    end);
end;

// Renombrado de HandleException
procedure TSocketClient.InternalHandleException(const AException: Exception);
var
  LForceDisconnect: Boolean;
begin
  LForceDisconnect := True; // Por defecto, los errores fuerzan la desconexión
  LogMessage(Format('TSocketClient InternalHandleException: %s - %s', [AException.ClassName, AException.Message]), logError);

  if Assigned(FOnErrorEvent) then // Usar nombre de propiedad correcto
  begin
    try
      FOnErrorEvent(AException, LForceDisconnect); // Permitir al listener decidir si se fuerza la desconexión
    except
      on E: Exception do
        LogMessage(Format('TSocketClient: Exception in OnErrorEvent handler itself: %s', [E.Message]), logError);
    end;
  end;

  if LForceDisconnect then
  begin
    LogMessage('TSocketClient InternalHandleException: Forcing disconnect due to error.', logDebug);
    DoClose; // Usar el método de cierre interno
  end;
end;

end.

