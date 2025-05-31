unit uLib.Session.Manager;

interface

uses
  System.SysUtils, System.Classes, System.Generics.Collections, System.SyncObjs,
  System.DateUtils, uLib.Thread.Timer;

type
  TSessionData = class
  private
    FID: string;
    FCreatedAt: TDateTime;
    FLastAccess: TDateTime;
    FUserData: TDictionary<string, string>; // Almacena datos clave-valor para la sesi�n
    FLock: TCriticalSection; // Protege FUserData

  public
    constructor Create(const AID: string);
    destructor Destroy; override;

    procedure SetValue(const AKey, AValue: string);
    function GetValue(const AKey: string; const ADefaultValue: string = ''): string; // A�adido ADefaultValue
    function IsExpired(ATimeoutMinutes: Integer): Boolean;
    procedure UpdateLastAccessTime; // Renombrado de UpdateLastAccess para claridad

    property ID: string read FID;
    property CreatedAt: TDateTime read FCreatedAt;
    property LastAccessTime: TDateTime read FLastAccess; // Renombrado de LastAccess
  end;

  TSessionManager = class
  private
    class var FInstance: TSessionManager; // Para el patr�n Singleton
    class var FSingletonLock: TCriticalSection; // Lock para la creaci�n del Singleton

    FSessions: TObjectDictionary<string, TSessionData>; // Clave: SessionID, Valor: TSessionData
    FLock: TCriticalSection; // Protege FSessions

    var FSessionTimeoutMinutes: Integer;
    FCleanupTimer: TThreadTimer; // <--- Agrega el timer

    class constructor CreateClassLock;
    class destructor DestroyClassLock;

    constructor CreateInternal; // Constructor privado para el Singleton
    procedure SetSessionTimeoutMinutes(const AValue: Integer); // Renombrado de SetTimeout
    procedure DoCleanupTimer(Sender: TObject);
  public
    destructor Destroy; override;
    class function GetInstance: TSessionManager;

    function SessionCount: Integer; // Renombrado de Count
    function CreateNewSession: TSessionData; // Renombrado de CreateSession
    function GetSessionByID(const ASessionID: string): TSessionData; // Renombrado de GetSession
    procedure InvalidateSession(const ASessionID: string); // Renombrado de RemoveSession
    procedure CleanupAllExpiredSessions; // Renombrado de CleanupExpiredSessions

    property SessionTimeoutMinutes: Integer read FSessionTimeoutMinutes write SetSessionTimeoutMinutes;
  end;


implementation

uses
  System.Hash, // Para THashSHA2
  uLib.Logger,

  uLib.Utils;

{ TSessionData }

constructor TSessionData.Create(const AID: string);
begin
  inherited Create;
  FID := AID;
  FCreatedAt := NowUTC; // Usar UTC para consistencia
  FLastAccess := NowUTC;
  FUserData := TDictionary<string, string>.Create;
  FLock := TCriticalSection.Create;
  LogMessage(Format('TSessionData created. ID: %s', [FID]), logDebug);
end;

destructor TSessionData.Destroy;
begin
  LogMessage(Format('TSessionData destroying. ID: %s', [FID]), logDebug);
  FreeAndNil(FUserData);
  FreeAndNil(FLock);
  inherited;
end;

procedure TSessionData.SetValue(const AKey, AValue: string);
begin
  FLock.Acquire;
  try
    FUserData.AddOrSetValue(AKey, AValue);
    LogMessage(Format('Session %s: Value set for key "%s"', [FID, AKey]), logSpam);
  finally
    FLock.Release;
  end;
end;

function TSessionData.GetValue(const AKey: string; const ADefaultValue: string = ''): string;
begin
  FLock.Acquire;
  try
    if not FUserData.TryGetValue(AKey, Result) then
      Result := ADefaultValue;
  finally
    FLock.Release;
  end;
end;

function TSessionData.IsExpired(ATimeoutMinutes: Integer): Boolean;
begin
  FLock.Acquire;
  try
    // MinutesBetween puede devolver negativo si Now < FLastAccess (improbable si se actualiza bien)
    Result := Abs(MinutesBetween(NowUTC, FLastAccess)) > ATimeoutMinutes;
    if Result then
      LogMessage(Format('Session %s determined as expired. LastAccess: %s, Timeout: %d min',
        [FID, DateToISO8601(FLastAccess), ATimeoutMinutes]), logDebug);
  finally
    FLock.Release;
  end;
end;

procedure TSessionData.UpdateLastAccessTime;
begin
  FLock.Acquire;
  try
    FLastAccess := NowUTC;
  finally
    FLock.Release;
  end;
end;

{ TSessionManager }

class constructor TSessionManager.CreateClassLock;
begin
  if not Assigned(FSingletonLock) then
    FSingletonLock := TCriticalSection.Create;
end;

class destructor TSessionManager.DestroyClassLock;
begin
  FreeAndNil(FSingletonLock);
end;

constructor TSessionManager.CreateInternal;
begin
  inherited Create;
  FSessions := TObjectDictionary<string, TSessionData>.Create([doOwnsValues]); // doOwnsValues es importante
  FLock := TCriticalSection.Create;
  SetSessionTimeoutMinutes(30); // Default 30 minutos
  LogMessage(Format('TSessionManager instance (CreateInternal) created. Default timeout: %d min.',
               [FSessionTimeoutMinutes]), logInfo);

  FCleanupTimer := CreateThreadTimer(5 * 60 * 1000, DoCleanupTimer); // 5 minutos en milisegundos
  FCleanupTimer.Start;
end;

destructor TSessionManager.Destroy;
begin
  if Assigned(FCleanupTimer) then
  begin
    // FCleanupTimer.Stop; // Opcional, ya que TerminateAndWait lo detendrá.
    // Llamar explícitamente a TerminateAndWait para asegurar que el thread
    // del timer haya finalizado antes de continuar con la destrucción de TSessionManager.
    FCleanupTimer.TerminateAndWait;
    FreeAndNil(FCleanupTimer);
  end;
  LogMessage('TSessionManager instance destroying...', logInfo);
  FreeAndNil(FSessions); // TObjectDictionary con doOwnsValues liberar las TSessionData
  FreeAndNil(FLock);
  LogMessage('TSessionManager instance destroyed.', logInfo);
  inherited;
end;

procedure TSessionManager.DoCleanupTimer(Sender: TObject);
begin
  // CORRECCIÓN: Verificar que la instancia siga siendo válida
  if not Assigned(Self) then
  begin
    LogMessage('TSessionManager.DoCleanupTimer: Self is nil, timer should be stopped.', logError);
    Exit;
  end;

  // Verificar que no estemos en proceso de destrucción
  if not Assigned(FSessions) then
  begin
    LogMessage('TSessionManager.DoCleanupTimer: Sessions collection is nil, cleanup skipped.', logDebug);
    Exit;
  end;

  try
    CleanupAllExpiredSessions;
  except
    on E: Exception do
    begin
      LogMessage('Error in automatic session cleanup: ' + E.Message, logError);
      // No re-raise para evitar que el timer se detenga por una excepción
    end;
  end;
end;

class function TSessionManager.GetInstance: TSessionManager;
begin
  // FSingletonLock debe haber sido creado por el class constructor
  if not Assigned(FInstance) then
  begin
    if not Assigned(FSingletonLock) then
    begin
      LogMessage('CRITICAL: TSessionManager.FSingletonLock is nil in GetInstance! Class constructor issue.', logFatal);
      //CreateClassLock; // Intento de recuperaci�n
      FSingletonLock := TCriticalSection.Create;
      if not Assigned(FSingletonLock) then
        raise Exception.Create('TSessionManager SingletonLock could not be initialized.');
    end;
    FSingletonLock.Acquire;
    try
      if not Assigned(FInstance) then
        FInstance := TSessionManager.CreateInternal;
    finally
      FSingletonLock.Release;
    end;
  end;
  Result := FInstance;
end;

function TSessionManager.CreateNewSession: TSessionData;
var
  SessionID: string;
  LGUID: TGuid;
  LGUIDString: String;
begin
  FLock.Acquire;
  try
    // Generar un nuevo GUID.
    LGUID := TGuid.NewGuid;
    LGUIDString:= LGUID.ToString;
    // Hashear los bytes del GUID usando SHA256 para obtener el SessionID.
    // Esto produce un ID de longitud fija, opaco y con muy baja probabilidad de colisi�n.
    SessionID := THashSHA2.GetHashString(LGUIDString);

    Result := TSessionData.Create(SessionID);
    FSessions.Add(SessionID, Result); // TObjectDictionary tomar� posesi�n si doOwnsValues est� activo
    LogMessage(Format('New session created. ID: %s. Total sessions: %d', [SessionID, FSessions.Count]), logInfo);
  finally
    FLock.Release;
  end;
end;

function TSessionManager.GetSessionByID(const ASessionID: string): TSessionData;
begin
  Result := nil;
  if ASessionID.IsEmpty then
  begin
    LogMessage('GetSessionByID called with empty SessionID.', logDebug);
    Exit;
  end;

  FLock.Acquire;
  try
    if FSessions.TryGetValue(ASessionID, Result) then // Result es TSessionData
    begin
      if Result.IsExpired(FSessionTimeoutMinutes) then
      begin
        LogMessage(Format('Session %s found but is expired. Removing.', [ASessionID]), logInfo);
        FSessions.Remove(ASessionID); // TObjectDictionary con doOwnsValues liberar� la instancia de TSessionData
        Result := nil; // No devolver sesi�n expirada
      end
      else
      begin
        Result.UpdateLastAccessTime;
        LogMessage(Format('Session %s retrieved and last access time updated.', [ASessionID]), logDebug);
      end;
    end
    else
      LogMessage(Format('Session %s not found.', [ASessionID]), logDebug);
  finally
    FLock.Release;
  end;
end;

procedure TSessionManager.InvalidateSession(const ASessionID: string);
begin
  if ASessionID.IsEmpty then Exit;

  FLock.Acquire;
  try
    if FSessions.ContainsKey(ASessionID) then
    begin
      FSessions.Remove(ASessionID); // TObjectDictionary con doOwnsValues liberar� la instancia
      LogMessage(Format('Session %s invalidated and removed. Total sessions: %d', [ASessionID, FSessions.Count]), logInfo);
    end
    else
      LogMessage(Format('Attempted to invalidate non-existent session %s.', [ASessionID]), logDebug);
  finally
    FLock.Release;
  end;
end;

function TSessionManager.SessionCount: Integer;
begin
  FLock.Acquire;
  try
    Result := FSessions.Count;
  finally
    FLock.Release;
  end;
end;

procedure TSessionManager.SetSessionTimeoutMinutes(const AValue: Integer);
begin
  FLock.Acquire;
  try
    if AValue > 0 then
      FSessionTimeoutMinutes := AValue
    else
      FSessionTimeoutMinutes := 30; // Default si el valor es inv�lido
    LogMessage(Format('Session timeout set to %d minutes.', [FSessionTimeoutMinutes]), logInfo);
  finally
    FLock.Release;
  end;
end;

procedure TSessionManager.CleanupAllExpiredSessions;
var
  ExpiredSessionIDs: TList<string>;
  SessionPair: TPair<string, TSessionData>; // Para iterar TObjectDictionary
  SessionID: string;
  RemovedCount: Integer;
begin
  LogMessage('Starting cleanup of expired sessions...', logInfo);
  ExpiredSessionIDs := TList<string>.Create;
  RemovedCount := 0;
  try
    FLock.Acquire; // Bloquear mientras se identifican las sesiones a eliminar
    try
      // No se puede modificar FSessions mientras se itera directamente con for-in si se usa Remove.
      // Primero recolectar IDs.
      for SessionPair in FSessions do
      begin
        if SessionPair.Value.IsExpired(FSessionTimeoutMinutes) then
          ExpiredSessionIDs.Add(SessionPair.Key);
      end;

      // Ahora eliminar las sesiones recolectadas
      for SessionID in ExpiredSessionIDs do
      begin
        if FSessions.ContainsKey(SessionID) then // Doble chequeo por si acaso
        begin
           FSessions.Remove(SessionID); // TObjectDictionary con doOwnsValues libera la instancia
           Inc(RemovedCount);
           LogMessage(Format('Expired session %s removed during cleanup.', [SessionID]), logDebug);
        end;
      end;
    finally
      FLock.Release;
    end;
  finally
    ExpiredSessionIDs.Free;
  end;
  if RemovedCount > 0 then
    LogMessage(Format('Cleanup complete. Removed %d expired sessions. Current total: %d', [RemovedCount, SessionCount]), logInfo)
  else
    LogMessage('Cleanup complete. No expired sessions found to remove.', logDebug);
end;

initialization
  TSessionManager.FInstance := nil;
  if not Assigned(TSessionManager.FSingletonLock) then
    TSessionManager.FSingletonLock := TCriticalSection.Create;
  // La variable global 'SessionManager' ya no se crea aqu�; se accede a trav�s de GetInstance.
finalization
  FreeAndNil(TSessionManager.FSingletonLock); // Liberar el lock del singleton
  if Assigned(TSessionManager.FInstance) then
    FreeAndNil(TSessionManager.FInstance);
end.

