unit uLib.Database.Pool;

interface

uses
  System.SysUtils, System.Classes, System.Generics.Collections, System.SyncObjs,
  System.JSON, Data.DB, System.Diagnostics, System.Threading, System.Generics.Defaults,

  uLib.Thread.Timer, // External dependency or custom unit. Assumed to be available.
  uLib.Database.Types,
  uLib.Database.Connection,
  uLib.Database.MSSQL,
  uLib.Database.PostgreSQL,
  uLib.Database.MySQL,
  uLib.Logger,

  uLib.Utils;

type
  TSingleDBConnectionPool = class; // Forward declaration

  TPooledDBConnection = class
  private
    FID: string;
    FDBConnectionIntf: IDBConnection;
    FUnderlyingConnectionObject: TObject; // The concrete TBaseConnection descendant
    FPoolName: string;
    FConnectionStateInPool: TConnectionState;
    FLastUsedTime: TDateTime;
    FCreatedTime: TDateTime;
    FUsageCount: Int64;
    FPoolOwner: TSingleDBConnectionPool;

    procedure SetConnectionStateInPool(AValue: TConnectionState);
    function GetUnderlyingBaseConnection: TBaseConnection; // Helper
  public
    constructor Create(const APoolName: string; ADBCoFactory: TFunc<IDBConnection>; AUnderlyingObject: TObject; APoolOwner: TSingleDBConnectionPool);
    destructor Destroy; override;

    function ConnectIfNecessary: Boolean;
    procedure DisconnectAndClose; // Disconnects and frees the underlying TBaseConnection
    function IsValidForPool: Boolean;

    property ID: string read FID;
    property DBConnectionIntf: IDBConnection read FDBConnectionIntf;
    property UnderlyingConnectionObject: TObject read FUnderlyingConnectionObject;
    property PoolName: string read FPoolName;
    property ConnectionStateInPool: TConnectionState read FConnectionStateInPool write SetConnectionStateInPool;
    property LastUsedTime: TDateTime read FLastUsedTime write FLastUsedTime;
    property CreatedTime: TDateTime read FCreatedTime;
    property UsageCount: Int64 read FUsageCount;
  end;

  TSingleDBConnectionPool = class
  private
    FConfig: TDBConnectionConfig;
    FMonitor: IDBMonitor;
    FConnections: TList<TPooledDBConnection>;
    FLock: TCriticalSection;

    FCurrentSize: Integer;
    FActiveConnections: Integer;
    FTotalCreated: Int64;
    FTotalAcquired: Int64;
    FTotalReleased: Int64;
    FTotalValidatedOK: Int64;
    FTotalFailedCreations: Int64;
    FTotalFailedValidations: Int64;
    FWaitCount: Integer;        // Number of requests currently waiting for a connection
    FWaitTimeAccumulatedMs: Int64;

    FCleanupTimer: TThreadTimer;
    FConnectionAvailableEvent: TEvent;

    function CreateAndConnectNewDBConnection: IDBConnection; // Helper for direct connection when pooling disabled
    function CreateNewPooledConnectionWrapper: TPooledDBConnection; // Renamed from CreateNewPooledConnection
    procedure RemoveConnectionFromPool(APooledConn: TPooledDBConnection; ADestroyWrapperAndUnderlying: Boolean); // Renamed param
    procedure CleanupIdleConnectionsTimerEvent(Sender: TObject);
    procedure EnsureMinPoolSize;
    procedure LogPoolStatus(const AReason: string);

  public
    constructor Create(const AConfig: TDBConnectionConfig; AMonitor: IDBMonitor = nil);
    destructor Destroy; override;

    function AcquireConnection(AAcquireTimeoutMs: Integer = -1): IDBConnection;
    procedure ReleaseConnection(ADBIntfToRelease: IDBConnection);
    procedure ValidateAllIdleConnections;
    procedure TrimIdleConnections; // Renamed from CleanupIdleConnections for clarity
    procedure CloseAllConnections;

    function GetPoolStats: TJSONObject;

    property Config: TDBConnectionConfig read FConfig;
    property CurrentSize: Integer read FCurrentSize;
    property ActiveConnections: Integer read FActiveConnections;
    function GetIdleConnectionsCount: Integer; // Renamed from GetIdleConnections
    property IdleConnectionsCount: Integer read GetIdleConnectionsCount; // Renamed
    property Name: string read FConfig.Name;
  end;

  TDBConnectionPoolManager = class
  private
    FMonitor: IDBMonitor;
    class var FInstance: TDBConnectionPoolManager;
    class var FSingletonLock: TCriticalSection;

    FPools: TObjectDictionary<string, TSingleDBConnectionPool>; // Cambiado de TDictionary
    FManagerLock: TCriticalSection;

    constructor CreatePrivate;
    class constructor CreateClassLock;
    class destructor DestroyClassLock;
  public
    destructor Destroy; override;
    class function GetInstance: TDBConnectionPoolManager;

    procedure ConfigurePoolsFromJSONArray(AConfigArray: TJSONArray; AMonitor: IDBMonitor = nil);
    procedure ConfigureSinglePool(AConfig: TDBConnectionConfig; AMonitor: IDBMonitor = nil);

    function AcquireConnection(const APoolName: string; AAcquireTimeoutMs: Integer = -1): IDBConnection;
    procedure ReleaseConnection(ADBConnection: IDBConnection; const APoolNameHint: string = '');

    function GetPool(const APoolName: string): TSingleDBConnectionPool;
    function GetPoolStats(const APoolName: string): TJSONObject;
    function GetAllPoolsStats: TJSONArray;
    procedure ShutdownAllPools;
    procedure ValidateAllPools;
    procedure TrimAllPools;

    property Monitor: IDBMonitor read FMonitor write FMonitor;
  end;

implementation

uses
  System.DateUtils, System.Math, System.Variants,
  System.StrUtils, System.Rtti, FireDAC.Stan.Def;

{ TPooledDBConnection }

constructor TPooledDBConnection.Create(const APoolName: string; ADBCoFactory: TFunc<IDBConnection>;
  AUnderlyingObject: TObject; APoolOwner: TSingleDBConnectionPool);
begin
  inherited Create;
  FID := TGuid.NewGuid.ToString;
  FPoolName := APoolName;
  FPoolOwner := APoolOwner;
  FDBConnectionIntf := ADBCoFactory(); // Call factory to get the interface to the underlying object
  FUnderlyingConnectionObject := AUnderlyingObject; // Store the concrete object

  if not Assigned(FDBConnectionIntf) then
    raise EDBPoolError.CreateFmt('Failed to create DBConnectionIntf in TPooledDBConnection for pool "%s". Factory returned nil.', [APoolName]);
  if not Assigned(FUnderlyingConnectionObject) then // Should be guaranteed if factory worked and returned itself
    raise EDBPoolError.CreateFmt('Underlying connection object is nil in TPooledDBConnection for pool "%s".', [APoolName]);

  FCreatedTime := NowUTC; // Use UTC
  FLastUsedTime := FCreatedTime;
  FUsageCount := 0;
  FConnectionStateInPool := csNew;
  LogMessage(Format('TPooledDBConnection %s created for pool %s.', [FID, FPoolName]), logDebug);
end;

destructor TPooledDBConnection.Destroy;
begin
  LogMessage(Format('Destroying TPooledDBConnection %s for pool %s. StateInPool: %s',
    [FID, FPoolName, TRttiEnumerationType.GetName<TConnectionState>(FConnectionStateInPool)]), logDebug);
  DisconnectAndClose; // This will free FUnderlyingConnectionObject
  inherited;
end;

function TPooledDBConnection.GetUnderlyingBaseConnection: TBaseConnection;
begin
  Result := FUnderlyingConnectionObject as TBaseConnection; // Assumes it's always a TBaseConnection
end;

procedure TPooledDBConnection.SetConnectionStateInPool(AValue: TConnectionState);
begin
  if FConnectionStateInPool <> AValue then
  begin
    var OldState := FConnectionStateInPool;
    FConnectionStateInPool := AValue;
    LogMessage(Format('PooledConnection %s (Pool: %s) state change: %s -> %s',
      [FID, FPoolName,
       TRttiEnumerationType.GetName<TConnectionState>(OldState),
       TRttiEnumerationType.GetName<TConnectionState>(AValue)]), logSpam); // Changed to logSpam
  end;
end;

function TPooledDBConnection.ConnectIfNecessary: Boolean;
begin
  Result := False;
  if Assigned(FDBConnectionIntf) then
  begin
    if FDBConnectionIntf.IsConnected then
    begin
      Result := True;
      Exit;
    end;

    LogMessage(Format('Attempting to connect PooledConnection %s (Pool: %s)...', [FID, FPoolName]), logDebug);
    Result := FDBConnectionIntf.Connect;
    if Result then
    begin
      LogMessage(Format('PooledConnection %s (Pool: %s) connected successfully.', [FID, FPoolName]), logInfo)
    end
    else
    begin
      ConnectionStateInPool := csInvalid;
      LogMessage(Format('Failed to connect PooledConnection %s (Pool: %s). LastError: %s',
        [FID, FPoolName, FDBConnectionIntf.GetLastError]), logError);
    end;
  end
  else
    LogMessage(Format('Cannot connect PooledConnection %s (Pool: %s): DBConnectionIntf is nil.', [FID, FPoolName]), logError);
end;

procedure TPooledDBConnection.DisconnectAndClose;
begin
  if Assigned(FUnderlyingConnectionObject) then
  begin
    var LBaseConn := GetUnderlyingBaseConnection; // Use helper for clarity
    if Assigned(LBaseConn) and LBaseConn.IsConnected then
    begin
      LogMessage(Format('Disconnecting underlying TBaseConnection for PooledConnection %s (Pool: %s)...', [FID, FPoolName]), logDebug);
      try
        LBaseConn.Disconnect;
      except
        on E: Exception do
          LogMessage(Format('Error disconnecting underlying connection for %s: %s', [FID, E.Message]), logError);
      end;
    end;
    FreeAndNil(FUnderlyingConnectionObject); // This TPooledDBConnection owns the underlying object
  end;
  FDBConnectionIntf := nil; // Clear the interface reference
  ConnectionStateInPool := csClosed;
end;

function TPooledDBConnection.IsValidForPool: Boolean;
var
  OriginalTimeout: Integer;
  LBaseConn: TBaseConnection;
begin
  Result := False;
  LBaseConn := GetUnderlyingBaseConnection;
  if not Assigned(LBaseConn) or not LBaseConn.IsConnected then
  begin
    LogMessage(Format('Validation of PooledConnection %s (Pool: %s) failed: Not connected or underlying object nil/invalid.', [FID, FPoolName]), logDebug);
    ConnectionStateInPool := csInvalid;
    Exit;
  end;

  try
    //OriginalTimeout := LBaseConn.QueryTimeout; // Access QueryTimeout via TBaseConnection
    LBaseConn.SetQueryTimeout(Max(1, FPoolOwner.Config.ConnectionTimeout div 10));

    LBaseConn.ExecuteScalar('SELECT 1'); // Use the concrete class method
    Result := True;
    LogMessage(Format('PooledConnection %s (Pool: %s) validated successfully with ping.', [FID, FPoolName]), logSpam); // Changed to logSpam
  except
    on E: Exception do
    begin
      LogMessage(Format('PING validation of PooledConnection %s (Pool: %s) FAILED: %s', [FID, FPoolName, E.Message]), logWarning);
      Result := False;
      ConnectionStateInPool := csInvalid;
    end;
  end;
  if Assigned(LBaseConn) then // Check again as it might have been invalidated
    LBaseConn.SetQueryTimeout(OriginalTimeout);
end;

{ TSingleDBConnectionPool }

constructor TSingleDBConnectionPool.Create(const AConfig: TDBConnectionConfig; AMonitor: IDBMonitor = nil);
begin
  inherited Create;
  FConfig := AConfig;
  FConfig.Validate;
  FMonitor := AMonitor;

  FConnections := TList<TPooledDBConnection>.Create;
  FLock := TCriticalSection.Create;

  FConnectionAvailableEvent := TEvent.Create(nil, True, False, '', False); // AutoReset = True, InitialState = False

  FCurrentSize := 0; FActiveConnections := 0; FTotalCreated := 0; FTotalAcquired := 0;
  FTotalReleased := 0; FTotalValidatedOK := 0; FTotalFailedCreations := 0;
  FTotalFailedValidations := 0; FWaitCount := 0; FWaitTimeAccumulatedMs := 0;

  LogMessage(Format('TSingleDBConnectionPool "%s" created. MinSize: %d, MaxSize: %d, DBType: %s, Pooling: %s',
    [FConfig.Name, FConfig.MinPoolSize, FConfig.MaxPoolSize,
     TRttiEnumerationType.GetName<TDBType>(FConfig.DBType), BoolToStr(FConfig.PoolingEnabled, True)]), logInfo);

  if FConfig.PoolingEnabled and (FConfig.MinPoolSize > 0) then
    EnsureMinPoolSize;

  if FConfig.PoolingEnabled and (FConfig.IdleTimeout > 0) then
  begin
    FCleanupTimer := TThreadTimer.Create(nil);
    FCleanupTimer.Interval := Max(15000, FConfig.IdleTimeout * 1000 div 3);
    FCleanupTimer.OnTimer := CleanupIdleConnectionsTimerEvent;
    FCleanupTimer.Enabled := True;
    LogMessage(Format('Pool "%s": Cleanup timer started. Interval: %dms', [FConfig.Name, FCleanupTimer.Interval]), logInfo);
  end
  else
    FCleanupTimer := nil;
end;

destructor TSingleDBConnectionPool.Destroy;
begin
  LogMessage(Format('Destroying TSingleDBConnectionPool "%s"...', [FConfig.Name]), logInfo);
  if Assigned(FCleanupTimer) then
  begin
    FCleanupTimer.Enabled := False; // Stop the timer
    // Wait for timer thread to finish if it's a TThreadTimer that needs explicit waiting,
    // or ensure its OnTimer event won't fire after this point.
    // For simplicity, assuming TThreadTimer handles its own thread shutdown cleanly when freed.
    FreeAndNil(FCleanupTimer);
  end;
  CloseAllConnections;
  FreeAndNil(FConnections);
  FreeAndNil(FLock);
  FreeAndNil(FConnectionAvailableEvent);
  LogMessage(Format('TSingleDBConnectionPool "%s" destroyed.', [FConfig.Name]), logInfo);
  inherited;
end;

function TSingleDBConnectionPool.GetIdleConnectionsCount: Integer;
begin
  FLock.Acquire;
  try
    Result := FCurrentSize - FActiveConnections;
  finally
    FLock.Release;
  end;
end;

// Helper for creating direct, unmanaged connections when pooling is disabled
function TSingleDBConnectionPool.CreateAndConnectNewDBConnection: IDBConnection;
var
  LBaseConn: TBaseConnection;
begin
  Result := nil;
  LBaseConn := nil;
  LogMessage(Format('Pool "%s" (Pooling Disabled): Creating direct DB connection...', [FConfig.Name]), logDebug);
  try
    case FConfig.DBType of
      dbtMSSQL:      LBaseConn := TMSSQLConnection.Create(FConfig, FMonitor);
      dbtPostgreSQL: LBaseConn := TPostgreSQLConnection.Create(FConfig, FMonitor);
      dbtMySQL:      LBaseConn := TMySQLConnection.Create(FConfig, FMonitor);
    else
      raise EDBPoolError.CreateFmt('Pool "%s": Unsupported DBType for direct connection: %s',
        [FConfig.Name, TRttiEnumerationType.GetName<TDBType>(FConfig.DBType)]);
    end;

    if LBaseConn.Connect then
    begin
      Result := LBaseConn; // Return the interface to the TBaseConnection
      Inc(FTotalCreated); // Still track creations for stats, even if not pooled
      LogMessage(Format('Pool "%s" (Pooling Disabled): Direct DB connection created and connected.', [FConfig.Name]), logInfo);
    end
    else
    begin
      LogMessage(Format('Pool "%s" (Pooling Disabled): Failed to connect direct DB instance. LastError: %s',
        [FConfig.Name, LBaseConn.GetLastError]), logError);
      Inc(FTotalFailedCreations);
      FreeAndNil(LBaseConn); // Free if connect failed
    end;
  except
    on E: Exception do
    begin
      LogMessage(Format('Pool "%s" (Pooling Disabled): Exception during CreateAndConnectNewDBConnection: %s', [FConfig.Name, E.Message]), logError);
      Inc(FTotalFailedCreations);
      FreeAndNil(LBaseConn); // Ensure cleanup on exception
      raise; // Re-raise
    end;
  end;
end;

function TSingleDBConnectionPool.CreateNewPooledConnectionWrapper: TPooledDBConnection;
var
  LUnderlyingObject: TObject;
  LDBConnIntf: IDBConnection;
  LDBConnFactory: TFunc<IDBConnection>;
begin
  Result := nil;
  LUnderlyingObject := nil;
  LDBConnIntf := nil;

  LogMessage(Format('Pool "%s": Attempting to create new pooled DB connection wrapper...', [FConfig.Name]), logDebug);
  try
    case FConfig.DBType of
      dbtMSSQL:      LDBConnIntf := TMSSQLConnection.Create(FConfig, FMonitor);
      dbtPostgreSQL: LDBConnIntf := TPostgreSQLConnection.Create(FConfig, FMonitor);
      dbtMySQL:      LDBConnIntf := TMySQLConnection.Create(FConfig, FMonitor);
    else
      raise EDBPoolError.CreateFmt('Pool "%s": Unsupported DBType specified: %s',
        [FConfig.Name, TRttiEnumerationType.GetName<TDBType>(FConfig.DBType)]);
    end;

    LUnderlyingObject := LDBConnIntf as TObject; // The TBaseConnection descendant is the TObject

    LDBConnFactory := function: IDBConnection begin Result := LDBConnIntf; end;

    Result := TPooledDBConnection.Create(FConfig.Name, LDBConnFactory, LUnderlyingObject, Self);

    if Result.ConnectIfNecessary then
    begin
      Inc(FTotalCreated);
      LogMessage(Format('Pool "%s": New pooled DB connection %s created and connected.', [FConfig.Name, Result.ID]), logInfo);
    end
    else
    begin
      LogMessage(Format('Pool "%s": Failed to connect new pooled DB instance %s. LastError: %s',
        [FConfig.Name, Result.ID, Result.DBConnectionIntf.GetLastError]), logError);
      Inc(FTotalFailedCreations);
      FreeAndNil(Result); // Calls TPooledDBConnection.Destroy -> DisconnectAndClose -> Frees LUnderlyingObject
    end;
  except
    on E: Exception do
    begin
      LogMessage(Format('Pool "%s": Exception during CreateNewPooledConnectionWrapper: %s', [FConfig.Name, E.Message]), logError);
      Inc(FTotalFailedCreations);
      if Assigned(Result) then FreeAndNil(Result)
      else if Assigned(LUnderlyingObject) then FreeAndNil(LUnderlyingObject); // If wrapper creation failed after underlying obj created
      Result := nil; // Ensure result is nil
    end;
  end;
end;

procedure TSingleDBConnectionPool.RemoveConnectionFromPool(APooledConn: TPooledDBConnection; ADestroyWrapperAndUnderlying: Boolean);
begin
  // Assumes FLock is acquired by caller
  if not Assigned(APooledConn) then Exit;

  LogMessage(Format('Pool "%s": Removing connection %s. Destroy: %s. StateInPool: %s',
    [FConfig.Name, APooledConn.ID, BoolToStr(ADestroyWrapperAndUnderlying, True), TRttiEnumerationType.GetName<TConnectionState>(APooledConn.ConnectionStateInPool)]), logDebug);

  if (FConnections.Remove(APooledConn)=0) then // Remove from list by object instance
  begin
    Dec(FCurrentSize);
    if APooledConn.ConnectionStateInPool = csInUse then // Should ideally not happen if being removed due to invalidity/idle
      Dec(FActiveConnections);

    if ADestroyWrapperAndUnderlying then
    begin
      FreeAndNil(APooledConn); // Calls TPooledDBConnection.Destroy, which calls DisconnectAndClose
    end;
  end else
    LogMessage(Format('Pool "%s": Attempted to remove connection %s not found in list.', [FConfig.Name, APooledConn.ID]), logWarning);
end;

procedure TSingleDBConnectionPool.EnsureMinPoolSize;
var
  i, NumToCreate: Integer;
  NewConnWrapper: TPooledDBConnection;
begin
  if not FConfig.PoolingEnabled then Exit;

  FLock.Acquire;
  try
    NumToCreate := FConfig.MinPoolSize - FCurrentSize;
    if NumToCreate > 0 then
    begin
      LogMessage(Format('Pool "%s": Ensuring min pool size. Current: %d, Min: %d. Need to create: %d',
        [FConfig.Name, FCurrentSize, FConfig.MinPoolSize, NumToCreate]), logInfo);
      for i := 1 to NumToCreate do
      begin
        if FCurrentSize >= FConfig.MaxPoolSize then Break;

        NewConnWrapper := CreateNewPooledConnectionWrapper;
        if Assigned(NewConnWrapper) then
        begin
          NewConnWrapper.ConnectionStateInPool := csIdle;
          FConnections.Add(NewConnWrapper);
          Inc(FCurrentSize);
        end
        else
          LogMessage(Format('Pool "%s": Failed to create initial connection during EnsureMinPoolSize.', [FConfig.Name]), logError);
      end;
      LogPoolStatus('After EnsureMinPoolSize');
    end;
  finally
    FLock.Release;
  end;
end;

function TSingleDBConnectionPool.AcquireConnection(AAcquireTimeoutMs: Integer = -1): IDBConnection;
var
  LAcquireTimeoutMs: Integer;
  PooledConnToUse: TPooledDBConnection;
  Stopwatch: TStopwatch;
  Candidate: TPooledDBConnection;
  i: Integer;
  RemainingTimeoutMs: Integer;
  AttemptedToGrowPoolThisCycle: Boolean;
begin
  Result := nil;
  if not FConfig.PoolingEnabled then
  begin
    LogMessage(Format('Pool "%s": Pooling disabled. Creating and returning a new, unmanaged direct connection.', [FConfig.Name]), logWarning);
    Result := CreateAndConnectNewDBConnection; // This creates TBaseConnection, connects, and returns its interface
    if not Assigned(Result) then
      raise EDBPoolError.CreateFmt('Pool "%s": Failed to create direct connection when pooling is disabled.', [FConfig.Name]);
    Exit;
  end;

  LAcquireTimeoutMs := IfThen(AAcquireTimeoutMs < 0, FConfig.AcquireTimeout, AAcquireTimeoutMs);

  LogMessage(Format('Pool "%s": Acquiring connection. Timeout: %dms. Active: %d, Idle: %d, Size: %d/%d',
    [FConfig.Name, LAcquireTimeoutMs, FActiveConnections, GetIdleConnectionsCount, FCurrentSize, FConfig.MaxPoolSize]), logInfo);

  Stopwatch := TStopwatch.StartNew;
  Inc(FWaitCount);

  try
    repeat
      PooledConnToUse := nil;
      AttemptedToGrowPoolThisCycle := False;

      FLock.Acquire;
      try
        // 1. Intentar encontrar una conexión idle válida
        for i := FConnections.Count - 1 downto 0 do
        begin
          Candidate := FConnections[i];
          if Candidate.ConnectionStateInPool = csIdle then
          begin
            Inc(FTotalValidatedOK); // Asumir intento de validación
            if Candidate.IsValidForPool then // IsValidForPool puede tomar tiempo
            begin
              PooledConnToUse := Candidate;
              PooledConnToUse.ConnectionStateInPool := csInUse;
              Inc(FActiveConnections);
              Break; // Encontrada
            end
            else // IsValidForPool marcó como csInvalid y logueó
            begin
              Inc(FTotalFailedValidations);
              RemoveConnectionFromPool(Candidate, True); // Remueve y destruye
            end;
          end;
        end;

        // 2. Si no hay idle válida, y el pool puede crecer, crear nueva
        if not Assigned(PooledConnToUse) and (FCurrentSize < FConfig.MaxPoolSize) then
        begin
          // El lock se mantiene para crear y añadir la conexión de forma segura
          LogMessage(Format('Pool "%s": No valid idle connections. Attempting to grow pool. Current: %d, Max: %d',
            [FConfig.Name, FCurrentSize, FConfig.MaxPoolSize]), logDebug);

          var NewConnWrapper := CreateNewPooledConnectionWrapper; // Esto puede tomar tiempo (conexión a BD)
          if Assigned(NewConnWrapper) then
          begin
            NewConnWrapper.ConnectionStateInPool := csInUse;
            FConnections.Add(NewConnWrapper);
            Inc(FCurrentSize);
            Inc(FActiveConnections);
            PooledConnToUse := NewConnWrapper;
            AttemptedToGrowPoolThisCycle := True;
          end
          else
            LogMessage(Format('Pool "%s": Failed to grow pool (CreateNewPooledConnectionWrapper returned nil).', [FConfig.Name]), logWarning);
        end;
      finally
        FLock.Release;
      end;

      if Assigned(PooledConnToUse) then
      begin
        Inc(PooledConnToUse.FUsageCount);
        PooledConnToUse.FLastUsedTime := NowUTC;
        Result := PooledConnToUse.DBConnectionIntf;
        Inc(FTotalAcquired);
        Dec(FWaitCount); // Decrementar solo al adquirir con éxito
        FWaitTimeAccumulatedMs := FWaitTimeAccumulatedMs + Stopwatch.ElapsedMilliseconds;
        LogMessage(Format('Pool "%s": Connection %s acquired. Active: %d, Idle: %d, Waiters: %d',
          [FConfig.Name, PooledConnToUse.ID, FActiveConnections, GetIdleConnectionsCount, FWaitCount]), logDebug);
        Stopwatch.Stop;
        Exit; // Salir de la función con el resultado
      end;

      // Si no se pudo obtener/crear una conexión, esperar
      RemainingTimeoutMs := LAcquireTimeoutMs - Stopwatch.ElapsedMilliseconds;
      if RemainingTimeoutMs <= 0 then
      begin
        Dec(FWaitCount); // Timeout, decrementar
        FWaitTimeAccumulatedMs := FWaitTimeAccumulatedMs + Stopwatch.ElapsedMilliseconds;
        LogMessage(Format('Pool "%s": Timeout acquiring connection after %dms. MaxSize: %d, Current: %d, Active: %d, Waiters: %d',
          [FConfig.Name, Stopwatch.ElapsedMilliseconds, FConfig.MaxPoolSize, FCurrentSize, FActiveConnections, FWaitCount]), logError);
        Stopwatch.Stop;
        raise EDBPoolError.CreateFmt('Pool "%s": Timeout (%dms) acquiring database connection.', [FConfig.Name, LAcquireTimeoutMs]);
      end;

      // Esperar en el evento en lugar de Sleep, solo si no intentamos crecer el pool en este ciclo
      // o si el intento de crecer falló y aún estamos por debajo del max_size (lo cual es raro que falle si < max_size)
      // La idea es esperar si no hay nada más que hacer activamente (como crear una conexión)
      var CanStillGrowPotential: Boolean;
      FLock.Acquire;
      try
        CanStillGrowPotential := (FCurrentSize < FConfig.MaxPoolSize);
      finally
        FLock.Release;
      end;

      if AttemptedToGrowPoolThisCycle or CanStillGrowPotential then
      begin
         Sleep(20); // Si intentamos crecer o aún podemos, un pequeño sleep para re-evaluar el estado del pool.
                     // Podríamos haber fallado al crecer por una razón temporal.
      end
      else // No se pudo obtener una idle, no se pudo crecer, estamos al máximo o el crecimiento falló -> esperar señal
      begin
        LogMessage(Format('Pool "%s": Waiting for available connection. Timeout left: %dms. Waiters: %d', [FConfig.Name, RemainingTimeoutMs, FWaitCount]), logSpam);
        FConnectionAvailableEvent.WaitFor(Max(10, Min(RemainingTimeoutMs, 250))); // Esperar un tiempo razonable o hasta timeout
      end;

    until False; // El bucle termina por Exit (éxito) o raise (timeout)
  except
    // Asegurar que FWaitCount se decremente si una excepción diferente a EDBPoolError (timeout) ocurre
    // y no se decrementó antes.
    if Stopwatch.IsRunning then // Si aún no se ha detenido por éxito o timeout explícito
    begin
        Dec(FWaitCount);
        FWaitTimeAccumulatedMs := FWaitTimeAccumulatedMs + Stopwatch.ElapsedMilliseconds;
        Stopwatch.Stop;
    end;
    LogPoolStatus('After AcquireConnection exception/failure');
    raise; // Relanzar la excepción
  end;
end;

procedure TSingleDBConnectionPool.ReleaseConnection(ADBIntfToRelease: IDBConnection);
var
  ConnToRelease: TPooledDBConnection;
  i: Integer;
  Found: Boolean;
  ShouldSignalEvent: Boolean;
begin
  if not Assigned(ADBIntfToRelease) then
     Exit;
  ConnToRelease := nil;
  Found := False;
  ShouldSignalEvent := False;

  FLock.Acquire;
  try
    for i := 0 to FConnections.Count - 1 do
    begin
      if FConnections[i].DBConnectionIntf = ADBIntfToRelease then
      begin
        ConnToRelease := FConnections[i];
        Found := True;
        Break;
      end;
    end;

    if Found and Assigned(ConnToRelease) then
    begin
      if ConnToRelease.ConnectionStateInPool = csInUse then
      begin
        ConnToRelease.ConnectionStateInPool := csIdle;
        ConnToRelease.FLastUsedTime := NowUTC;
        Dec(FActiveConnections);
        Inc(FTotalReleased);
        LogMessage(Format('Pool "%s": Connection %s released. Active: %d, Idle: %d, Waiters: %d',
          [FConfig.Name, ConnToRelease.ID, FActiveConnections, GetIdleConnectionsCount, FWaitCount]), logDebug);

        // --- INICIO: Señalar si hay hilos esperando ---
        if FWaitCount > 0 then // Solo señalar si realmente hay alguien esperando
          ShouldSignalEvent := True;
        // --- FIN: Señalar ---
      end
      else
        LogMessage(Format('Pool "%s": Attempt to release connection %s not in "csInUse" state (State: %s). Ignored.',
          [FConfig.Name, ConnToRelease.ID, TRttiEnumerationType.GetName<TConnectionState>(ConnToRelease.ConnectionStateInPool)]), logWarning);
    end
    else
    begin
      // ... (logging de conexión no encontrada, sin cambios) ...
    end;
    // No llamar a LogPoolStatus aquí dentro del lock si ShouldSignalEvent es true,
    // para evitar posible reentrada si SetEvent despierta un hilo que loguea.
  finally
    FLock.Release;
  end;

  if ShouldSignalEvent then
  begin
    FConnectionAvailableEvent.SetEvent; // Señalar fuera del lock principal
    LogMessage(Format('Pool "%s": Signaled FConnectionAvailableEvent due to release. Waiters: %d', [FConfig.Name, FWaitCount]), logSpam);
  end;

  LogPoolStatus('After ReleaseConnection'); // Loguear estado después de cualquier señalización
end;

procedure TSingleDBConnectionPool.ValidateAllIdleConnections;
var
  ConnectionsToValidateCopy: TList<TPooledDBConnection>;
  Conn: TPooledDBConnection;
  i: Integer;
begin
  LogMessage(Format('Pool "%s": Validating all idle connections...', [FConfig.Name]), logInfo);
  ConnectionsToValidateCopy := TList<TPooledDBConnection>.Create;
  try
    FLock.Acquire;
    try
      for Conn in FConnections do
        if Conn.ConnectionStateInPool = csIdle then
          ConnectionsToValidateCopy.Add(Conn); // Add to copy for validation outside lock
    finally
      FLock.Release;
    end;

    for Conn in ConnectionsToValidateCopy do // Validate outside main pool lock
    begin
      Inc(FTotalValidatedOK);
      if not Conn.IsValidForPool then // This marks Conn as csInvalid if it fails
        Inc(FTotalFailedValidations);
    end;

    // Re-acquire lock to remove invalid connections
    FLock.Acquire;
    try
      for i := FConnections.Count - 1 downto 0 do // Iterate backwards for safe removal
      begin
        Conn := FConnections[i];
        if Conn.ConnectionStateInPool = csInvalid then
        begin
          LogMessage(Format('Pool "%s": Removing connection %s due to failed validation during ValidateAllIdleConnections.', [FConfig.Name, Conn.ID]), logWarning);
          RemoveConnectionFromPool(Conn, True);
        end;
      end;
    finally
      FLock.Release;
    end;
  finally
    ConnectionsToValidateCopy.Free;
  end;

  if FConfig.PoolingEnabled then EnsureMinPoolSize;
  LogPoolStatus('After ValidateAllIdleConnections');
end;

procedure TSingleDBConnectionPool.TrimIdleConnections;
var
  NumToRemoveTarget, ActualRemovedCount, i: Integer;
  IdleConnList: TList<TPooledDBConnection>;
  Conn: TPooledDBConnection;
begin
  if not FConfig.PoolingEnabled then Exit;

  FLock.Acquire;
  try
    LogMessage(Format('Pool "%s": Trimming idle connections. Current: %d, Idle: %d, MinSize: %d, Active: %d',
      [FConfig.Name, FCurrentSize, GetIdleConnectionsCount, FConfig.MinPoolSize, FActiveConnections]), logInfo);

    if FCurrentSize <= FConfig.MinPoolSize then
    begin
      LogMessage(Format('Pool "%s": Current size (%d) <= MinPoolSize (%d). No trimming needed.', [FConfig.Name, FCurrentSize, FConfig.MinPoolSize]), logDebug);
      Exit;
    end;

    NumToRemoveTarget := FCurrentSize - FConfig.MinPoolSize;
    if NumToRemoveTarget <= 0 then Exit;

    IdleConnList := TList<TPooledDBConnection>.Create;
    try
      for Conn in FConnections do
        if Conn.ConnectionStateInPool = csIdle then
          IdleConnList.Add(Conn);

      if IdleConnList.Count = 0 then Exit;

      IdleConnList.Sort(TComparer<TPooledDBConnection>.Construct(
        function(const L, R: TPooledDBConnection): Integer
        begin // Sort by LastUsedTime ascending (oldest first)
          if L.LastUsedTime < R.LastUsedTime then Result := -1
          else if L.LastUsedTime > R.LastUsedTime then Result := 1
          else Result := 0;
        end));

      ActualRemovedCount := 0;
      for i := 0 to IdleConnList.Count - 1 do
      begin
        if ActualRemovedCount >= NumToRemoveTarget then Break;
        if FCurrentSize <= FConfig.MinPoolSize then Break;

        Conn := IdleConnList[i]; // Use the sorted list item
        // Re-check if it's still in FConnections and idle, as state might have changed
        if (FConnections.IndexOf(Conn) > -1) and (Conn.ConnectionStateInPool = csIdle) then
        begin
          LogMessage(Format('Pool "%s": Trimming idle connection %s (LastUsed: %s).',
            [FConfig.Name, Conn.ID, DateTimeToStr(Conn.LastUsedTime)]), logDebug);
          RemoveConnectionFromPool(Conn, True);
          Inc(ActualRemovedCount);
        end;
      end;
      LogMessage(Format('Pool "%s": Trimmed %d idle connections.', [FConfig.Name, ActualRemovedCount]), logInfo);
    finally
      IdleConnList.Free;
    end;
    LogPoolStatus('After TrimIdleConnections');
  finally
    FLock.Release;
  end;
end;

procedure TSingleDBConnectionPool.CloseAllConnections;
var
  TempList: TList<TPooledDBConnection>;
  Conn: TPooledDBConnection;
begin
  LogMessage(Format('Pool "%s": Closing all connections...', [FConfig.Name]), logInfo);
  TempList := TList<TPooledDBConnection>.Create;
  FLock.Acquire;
  try
    for Conn in FConnections do TempList.Add(Conn);
    FConnections.Clear;
    FCurrentSize := 0;
    FActiveConnections := 0;
  finally
    FLock.Release;
  end;

  for Conn in TempList do
  begin
    try
      FreeAndNil(Conn); // Calls TPooledDBConnection.Destroy -> DisconnectAndClose
    except
      on E: Exception do
        LogMessage(Format('Pool "%s": Error destroying connection %s during CloseAllConnections: %s', [FConfig.Name, Conn.ID, E.Message]), logError);
    end;
  end;
  TempList.Free;
  LogMessage(Format('Pool "%s": All connections closed and pool cleared.', [FConfig.Name]), logInfo);
end;

procedure TSingleDBConnectionPool.CleanupIdleConnectionsTimerEvent(Sender: TObject);
var
  IdleToRemove: TList<TPooledDBConnection>;
  Conn: TPooledDBConnection;
  NumCanBeRemoved: Integer;
  NowTime: TDateTime;
begin
  if not FConfig.PoolingEnabled or not Assigned(FLock) or not Assigned(FCleanupTimer) then Exit;

  FCleanupTimer.Enabled := False; // Prevent re-entrancy
  try
    LogMessage(Format('Pool "%s": Periodic cleanup of idle connections by timer...', [FConfig.Name]), logDebug);
    IdleToRemove := TList<TPooledDBConnection>.Create;
    NowTime := NowUTC; // Use UTC for comparison with LastUsedTime

    FLock.Acquire;
    try
      if FCurrentSize > FConfig.MinPoolSize then
      begin
        NumCanBeRemoved := FCurrentSize - FConfig.MinPoolSize;
        for Conn in FConnections do
        begin
          if IdleToRemove.Count >= NumCanBeRemoved then Break;
          if (Conn.ConnectionStateInPool = csIdle) and
             (SecondsBetween(NowTime, Conn.LastUsedTime) > FConfig.IdleTimeout) then
          begin
            IdleToRemove.Add(Conn);
          end;
        end;
      end;

      if IdleToRemove.Count > 0 then
      begin
        LogMessage(Format('Pool "%s": Timer found %d idle connections to remove due to timeout.', [FConfig.Name, IdleToRemove.Count]), logInfo);
        for Conn in IdleToRemove do
        begin
          if (FConnections.IndexOf(Conn) > -1) and (Conn.ConnectionStateInPool = csIdle) then
          begin
            RemoveConnectionFromPool(Conn, True);
          end;
        end;
      end;
    finally
      FLock.Release;
      IdleToRemove.Free;
    end;

    if FConfig.PoolingEnabled then EnsureMinPoolSize; // Re-check min pool size
    LogPoolStatus('After CleanupIdleConnectionsTimerEvent');
  finally
    if Assigned(FCleanupTimer) then FCleanupTimer.Enabled := True; // Re-enable timer
  end;
end;

procedure TSingleDBConnectionPool.LogPoolStatus(const AReason: string);
begin
  FLock.Acquire;
  try
    var AvgWait: Double := 0;
    if FTotalAcquired > 0 then // Avoid division by zero if no connections were acquired yet
      AvgWait := FWaitTimeAccumulatedMs / FTotalAcquired;

    LogMessage(Format('Pool "%s" Status (%s): Size=%d (Active=%d, Idle=%d), Max=%d, Min=%d, CreatedTotal=%d, AcquiredTotal=%d, ReleasedTotal=%d, ValidatedOK=%d, FailedCreate=%d, FailedValidate=%d, WaitingNow=%d, AvgWait(ms)=%.2f',
      [FConfig.Name, AReason, FCurrentSize, FActiveConnections, GetIdleConnectionsCount, FConfig.MaxPoolSize, FConfig.MinPoolSize,
       FTotalCreated, FTotalAcquired, FTotalReleased, FTotalValidatedOK, FTotalFailedCreations, FTotalFailedValidations,
       FWaitCount, AvgWait]), logDebug);
  finally
    FLock.Release;
  end;
end;

function TSingleDBConnectionPool.GetPoolStats: TJSONObject;
begin
  Result := TJSONObject.Create;
  FLock.Acquire;
  try
    Result.AddPair('pool_name', FConfig.Name);
    Result.AddPair('db_type', TRttiEnumerationType.GetName<TDBType>(FConfig.DBType));
    Result.AddPair('min_size', FConfig.MinPoolSize);
    Result.AddPair('max_size', FConfig.MaxPoolSize);
    Result.AddPair('current_size', FCurrentSize);
    Result.AddPair('active_connections', FActiveConnections);
    Result.AddPair('idle_connections', GetIdleConnectionsCount);
    Result.AddPair('total_created_physical', FTotalCreated);
    Result.AddPair('total_acquired_from_pool', FTotalAcquired);
    Result.AddPair('total_released_to_pool', FTotalReleased);
    Result.AddPair('total_validated_ok', FTotalValidatedOK);
    Result.AddPair('total_failed_creations', FTotalFailedCreations);
    Result.AddPair('total_failed_validations', FTotalFailedValidations);
    Result.AddPair('waiting_requests_now', FWaitCount);
    var AvgWait: Double := 0;
    if FTotalAcquired > 0 then AvgWait := FWaitTimeAccumulatedMs / FTotalAcquired;
    Result.AddPair('avg_acquire_wait_time_ms', AvgWait);
  finally
    FLock.Release;
  end;
end;

{ TDBConnectionPoolManager }

class constructor TDBConnectionPoolManager.CreateClassLock;
begin
  if not Assigned(FSingletonLock) then // Ensure it's only created once
    FSingletonLock := TCriticalSection.Create;
end;

class destructor TDBConnectionPoolManager.DestroyClassLock;
begin
  FreeAndNil(FSingletonLock);
end;

constructor TDBConnectionPoolManager.CreatePrivate;
begin
  inherited Create;
  FPools := TObjectDictionary<string, TSingleDBConnectionPool>.Create([doOwnsValues]);
  FManagerLock := TCriticalSection.Create;
  FMonitor := nil;
  LogMessage('TDBConnectionPoolManager instance created (Singleton).', logInfo);
end;

destructor TDBConnectionPoolManager.Destroy;
begin
  LogMessage('Destroying TDBConnectionPoolManager...', logInfo);
  ShutdownAllPools; // This will free individual pool objects due to doOwnsValues in FPools
  FreeAndNil(FPools); // FPools dictionary itself
  FreeAndNil(FManagerLock);
  LogMessage('TDBConnectionPoolManager destroyed.', logInfo);
  inherited;
end;

class function TDBConnectionPoolManager.GetInstance: TDBConnectionPoolManager;
begin
  if not Assigned(FInstance) then
  begin
    // FSingletonLock should have been created by class constructor
    if not Assigned(FSingletonLock) then
       raise EDBPoolError.Create('TDBConnectionPoolManager SingletonLock not initialized.');
    FSingletonLock.Acquire;
    try
      if not Assigned(FInstance) then
        FInstance := TDBConnectionPoolManager.CreatePrivate;
    finally
      FSingletonLock.Release;
    end;
  end;
  Result := FInstance;
end;

procedure TDBConnectionPoolManager.ConfigurePoolsFromJSONArray(AConfigArray: TJSONArray; AMonitor: IDBMonitor = nil);
var
  i: Integer;
  PoolConfigJSON: TJSONObject;
  DBConfig: TDBConnectionConfig;
begin
  if not Assigned(AConfigArray) then
  begin
    LogMessage('DBConnectionPoolManager.ConfigurePoolsFromJSONArray called with nil ConfigArray.', logError);
    Exit;
  end;

  FManagerLock.Acquire;
  try
    LogMessage(Format('DBConnectionPoolManager configuring with %d pool definitions...', [AConfigArray.Count]), logInfo);
    if Assigned(AMonitor) then Self.FMonitor := AMonitor;

    ShutdownAllPools; // Clears FPools and frees existing pool objects

    for i := 0 to AConfigArray.Count - 1 do
    begin
      if AConfigArray.Items[i] is TJSONObject then
      begin
        PoolConfigJSON := AConfigArray.Items[i] as TJSONObject;
        try
          DBConfig.LoadFromJSON(PoolConfigJSON);

          // FPools.ContainsKey es seguro, pero la duplicación no debería ocurrir si ShutdownAllPools funciona.
          if FPools.ContainsKey(DBConfig.Name) then
          begin
            LogMessage(Format('Pool "%s" seems to exist after ShutdownAllPools. This is unexpected. Overwriting.', [DBConfig.Name]), logWarning);
            // TObjectDictionary reemplazará y liberará el valor antiguo si se usa AddOrSetValue,
            // o fallará en Add si la clave existe. Es mejor Remove explícito si esto puede pasar.
            // Dado que ShutdownAllPools limpia, esto no debería ser un problema.
          end;

          LogMessage(Format('Creating connection pool "%s" for DBType: %s, Server: %s, DB: %s',
            [DBConfig.Name, TRttiEnumerationType.GetName<TDBType>(DBConfig.DBType),
             DBConfig.Server, DBConfig.Database]), logInfo);

          var Pool := TSingleDBConnectionPool.Create(DBConfig, Self.FMonitor);
          FPools.Add(DBConfig.Name, Pool); // TObjectDictionary ahora maneja la propiedad
        except
          on E: Exception do
            LogMessage(Format('Error configuring pool from JSON for entry %d: %s. JSON: %s',
             [i, E.Message, PoolConfigJSON.ToString]), logError);
        end;
      end
      else
        LogMessage(Format('DBConnectionPoolManager.ConfigurePoolsFromJSONArray: Item at index %d is not a TJSONObject.',[i]), logWarning);
    end;
    LogMessage('DBConnectionPoolManager configuration complete.', logInfo);
  finally
    FManagerLock.Release;
  end;
end;

procedure TDBConnectionPoolManager.ConfigureSinglePool(AConfig: TDBConnectionConfig; AMonitor: IDBMonitor = nil);
var
  Pool: TSingleDBConnectionPool;
begin
  AConfig.Validate;
  FManagerLock.Acquire;
  try
    LogMessage(Format('DBConnectionPoolManager configuring single pool "%s"...', [AConfig.Name]), logInfo);
    if Assigned(AMonitor) then Self.FMonitor := AMonitor;

    if FPools.ContainsKey(AConfig.Name) then
    begin
      LogMessage(Format('Pool "%s" already exists. Removing old pool before reconfiguring.', [AConfig.Name]), logWarning);
      FPools.Remove(AConfig.Name); // Esto liberará la instancia TSingleDBConnectionPool anterior
    end;

    Pool := TSingleDBConnectionPool.Create(AConfig, Self.FMonitor);
    FPools.Add(AConfig.Name, Pool); // TObjectDictionary toma posesión
    LogMessage(Format('Pool "%s" configured and created.', [AConfig.Name]), logInfo);
  finally
    FManagerLock.Release;
  end;
end;

function TDBConnectionPoolManager.AcquireConnection(const APoolName: string; AAcquireTimeoutMs: Integer = -1): IDBConnection;
var
  Pool: TSingleDBConnectionPool;
begin
  Result := nil;
  FManagerLock.Acquire; // Lock to safely access FPools
  try
    if not FPools.TryGetValue(APoolName, Pool) then
      raise EDBPoolError.CreateFmt('Connection pool "%s" not found.', [APoolName]);
  finally
    FManagerLock.Release;
  end;
  // Pool instance is now safely obtained. Call its AcquireConnection method, which is internally thread-safe.
  Result := Pool.AcquireConnection(AAcquireTimeoutMs);
  // Logging of success/failure is handled within TSingleDBConnectionPool.AcquireConnection
end;

procedure TDBConnectionPoolManager.ReleaseConnection(ADBConnection: IDBConnection; const APoolNameHint: string = '');
var
  Pool: TSingleDBConnectionPool;
  ActualPoolName: string;
  LBaseConn: TBaseConnection;
begin
  if not Assigned(ADBConnection) then Exit;

  ActualPoolName := APoolNameHint;

  // Try to determine the actual pool name from the connection object itself
  // This makes the APoolNameHint less critical if the connection "knows" its origin.
  if ActualPoolName.IsEmpty then
  begin
    if (ADBConnection is TBaseConnection) then
    begin
      LBaseConn := ADBConnection as TBaseConnection;
      ActualPoolName := LBaseConn.ConnectionConfigName; // Assumes TBaseConnection exposes this
    end;
  end;

  if ActualPoolName <> '' then
  begin
    FManagerLock.Acquire;
    try
      if FPools.TryGetValue(ActualPoolName, Pool) then
      begin
        // Pool found, release manager lock before calling pool's method
      end else Pool := nil;
    finally
      FManagerLock.Release;
    end;

    if Assigned(Pool) then
    begin
      Pool.ReleaseConnection(ADBConnection); // Pool's ReleaseConnection is thread-safe
      // LogMessage(Format('Connection released to pool "%s" (hint/actual).', [ActualPoolName]), logInfo); // Pool logs this
      Exit; // Successfully released to the (hinted or determined) pool
    end
    else if APoolNameHint <> '' then // Hint was given but pool not found by that name
      LogMessage(Format('ReleaseConnection: Hinted pool "%s" not found. Will try iterating all pools.', [APoolNameHint]), logWarning);
  end;

  // If no hint, or hinted pool not found, or could not determine pool from connection: iterate all pools (less efficient)
  LogMessage('ReleaseConnection: No specific pool identified. Iterating all pools to find owner (this is inefficient).', logWarning);
  FManagerLock.Acquire;
  try
    for var PairValue in FPools.Values do
    begin
      // TSingleDBConnectionPool.ReleaseConnection will check if the connection belongs to it.
      // If it does, it will handle it. If not, it will log a warning.
      PairValue.ReleaseConnection(ADBConnection);
      // We can't easily know here if it was *actually* released by one of them without more complex feedback.
      // The first pool that accepts it will do.
    end;
  finally
    FManagerLock.Release;
  end;
end;

function TDBConnectionPoolManager.GetPool(const APoolName: string): TSingleDBConnectionPool;
begin
  FManagerLock.Acquire;
  try
    if not FPools.TryGetValue(APoolName, Result) then
      raise EDBPoolError.CreateFmt('Connection pool "%s" not found in manager.', [APoolName]);
  finally
    FManagerLock.Release;
  end;
end;

function TDBConnectionPoolManager.GetPoolStats(const APoolName: string): TJSONObject;
var
  Pool: TSingleDBConnectionPool;
begin
  Result := nil;
  FManagerLock.Acquire;
  try
    if FPools.TryGetValue(APoolName, Pool) then
    begin
      // Pool instance obtained, release manager lock
    end else
      raise EDBPoolError.CreateFmt('Cannot get stats: Connection pool "%s" not found.', [APoolName]);
  finally
    FManagerLock.Release;
  end;
  Result := Pool.GetPoolStats; // Pool's method is thread-safe
end;

function TDBConnectionPoolManager.GetAllPoolsStats: TJSONArray;
var
  PoolListCopy: TList<TSingleDBConnectionPool>; // Create a copy to iterate outside lock
  Pool: TSingleDBConnectionPool;
begin
  Result := TJSONArray.Create;
  PoolListCopy := TList<TSingleDBConnectionPool>.Create;
  try
    FManagerLock.Acquire;
    try
      for Pool in FPools.Values do
        PoolListCopy.Add(Pool); // Add reference to the copy
    finally
      FManagerLock.Release;
    end;

    for Pool in PoolListCopy do
      Result.Add(Pool.GetPoolStats); // Pool's GetPoolStats is thread-safe
  finally
    PoolListCopy.Free;
  end;
end;

procedure TDBConnectionPoolManager.ShutdownAllPools;
begin
  LogMessage('DBConnectionPoolManager: Shutting down all connection pools...', logInfo);
  FManagerLock.Acquire;
  try
    // Con TObjectDictionary([doOwnsValues]), Clear llamará al destructor de cada TSingleDBConnectionPool.
    // El destructor de TSingleDBConnectionPool ya se encarga de CloseAllConnections.
    FPools.Clear;
  finally
    FManagerLock.Release;
  end;
  LogMessage('DBConnectionPoolManager: All pools cleared and instances destroyed.', logInfo);
end;

procedure TDBConnectionPoolManager.ValidateAllPools;
var
  PoolListCopy: TList<TSingleDBConnectionPool>;
begin
  LogMessage('DBConnectionPoolManager: Validating idle connections in all pools...', logInfo);
  PoolListCopy := TList<TSingleDBConnectionPool>.Create;
  try
    FManagerLock.Acquire;
    try
      for var Pool in FPools.Values do PoolListCopy.Add(Pool);
    finally
      FManagerLock.Release;
    end;
    for var Pool in PoolListCopy do Pool.ValidateAllIdleConnections;
  finally
    PoolListCopy.Free;
  end;
end;

procedure TDBConnectionPoolManager.TrimAllPools;
var
  PoolListCopy: TList<TSingleDBConnectionPool>;
begin
  LogMessage('DBConnectionPoolManager: Trimming idle connections in all pools...', logInfo);
  PoolListCopy := TList<TSingleDBConnectionPool>.Create;
  try
    FManagerLock.Acquire;
    try
      for var Pool in FPools.Values do PoolListCopy.Add(Pool);
    finally
      FManagerLock.Release;
    end;
    for var Pool in PoolListCopy do Pool.TrimIdleConnections;
  finally
    PoolListCopy.Free;
  end;
end;

initialization
  TDBConnectionPoolManager.FInstance := nil;
  // FSingletonLock is created by class constructor TDBConnectionPoolManager.CreateClassLock
finalization
  if Assigned(TDBConnectionPoolManager.FInstance) then
    FreeAndNil(TDBConnectionPoolManager.FInstance);
  // FSingletonLock is freed by class destructor TDBConnectionPoolManager.DestroyClassLock
end.
