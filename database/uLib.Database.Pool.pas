unit uLib.Database.Pool;

interface

uses
  System.SysUtils, System.Classes, System.Generics.Collections, System.SyncObjs,
  System.JSON, Data.DB, System.Diagnostics, System.Threading, System.Generics.Defaults,
  {$IFDEF MSWINDOWS}
  WinApi.Windows,
  {$ENDIF}
  uLib.Thread.Timer, // External dependency or custom unit. Assumed to be available.
  uLib.Database.Types,
  uLib.Database.Connection,
  uLib.Database.MSSQL,
  uLib.Database.PostgreSQL,
  uLib.Database.MySQL,
  uLib.Logger,

  uLib.Utils;

type
  /// <summary>
  /// Thread-safe database connection pool for a single database configuration.
  ///
  /// THREAD-SAFETY:
  /// - All public methods are thread-safe and can be called concurrently
  /// - Internal synchronization uses multiple locks to minimize contention
  /// - AcquireConnection and ReleaseConnection are optimized for high concurrency
  ///
  /// OWNERSHIP:
  /// - Pool owns all TPooledDBConnection instances
  /// - Caller receives IDBConnection interfaces (reference counted)
  /// - Pool manages connection lifecycle automatically
  ///
  /// PERFORMANCE CHARACTERISTICS:
  /// - Lock-free operations where possible using atomic operations
  /// - Separate locks for idle queue, active connections, and metrics
  /// - Optimized for high-frequency acquire/release operations
  ///
  /// RESOURCE MANAGEMENT:
  /// - Automatic cleanup of idle connections based on IdleTimeout
  /// - Periodic validation of pooled connections
  /// - Graceful shutdown with configurable grace period
  /// </summary>


  TSingleDBConnectionPool = class; // Forward declaration

  TPooledDBConnection = class
  private
    FID: string;
    FDBConnectionIntf: IDBConnection;
    FUnderlyingConnectionObject: TObject;
    FPoolName: string;
    FConnectionStateInPool: TConnectionState;
    FLastUsedTime: TDateTime;
    FCreatedTime: TDateTime;
    FUsageCount: Int64;
    FPoolOwner: TSingleDBConnectionPool;
    FLastValidationTime: TDateTime; // Para validación lazy
    FValidationInterval: Integer;   // En segundos
    FStateLock: TCriticalSection;

    function GetUnderlyingBaseConnection: TBaseConnection;
    function NeedsValidation: Boolean; // Validación lazy optimizada
    procedure SetConnectionStateInPool(AValue: TConnectionState);
  public
    constructor Create(const APoolName: string; ADBCoFactory: TFunc<IDBConnection>; AUnderlyingObject: TObject; APoolOwner: TSingleDBConnectionPool);
    destructor Destroy; override;

    function ConnectIfNecessary: Boolean;
    procedure DisconnectAndClose;
    function IsValidForPool: Boolean;
    procedure MarkAsUsed; // Optimizado para threading

    property ID: string read FID;
    property DBConnectionIntf: IDBConnection read FDBConnectionIntf;
    property UnderlyingConnectionObject: TObject read FUnderlyingConnectionObject;
    property PoolName: string read FPoolName;
    property ConnectionStateInPool: TConnectionState read FConnectionStateInPool write SetConnectionStateInPool;
    property LastUsedTime: TDateTime read FLastUsedTime;
    property CreatedTime: TDateTime read FCreatedTime;
    property UsageCount: Int64 read FUsageCount;
  end;

  // Optimized circular queue for idle connections
  TConnectionQueue = class
  private
    FConnections: TArray<TPooledDBConnection>;
    FHead, FTail: Integer;
    FCount: Integer;
    FCapacity: Integer;
    FLock: TCriticalSection;
    FIsDestroying: Boolean;
  public
    constructor Create(ACapacity: Integer);
    destructor Destroy; override;

    function TryDequeue(out AConnection: TPooledDBConnection): Boolean;
    function TryEnqueue(AConnection: TPooledDBConnection): Boolean;
    function Count: Integer;
    function IsEmpty: Boolean;
    function IsFull: Boolean;
    procedure Clear; // Para cleanup
  end;

  TPoolSnapshot = record
    CurrentSize: Integer;
    ActiveConnectionCount: Integer;
    IdleConnectionCount: Integer;
    WaitingRequestsCount: Integer;
    TotalCreated: Int64;
    TotalAcquired: Int64;
    TotalReleased: Int64;
    SnapshotTime: TDateTime;

    function ToJSON: TJSONObject;
  end;

  TSingleDBConnectionPool = class
  private
    FConfig: TDBConnectionConfig;
    FMonitor: IDBMonitor;

    // Separated locks for better concurrency
    FIdleQueue: TConnectionQueue;           // Queue optimizada para idle connections
    FActiveConnections: TThreadList<TPooledDBConnection>; // Solo conexiones activas
    FMetricsLock: TCriticalSection;         // Lock solo para métricas

    // Metrics with atomic operations where possible
    FCurrentSize: Integer;
    FActiveConnectionCount: Integer;
    FTotalCreated: Int64;
    FTotalAcquired: Int64;
    FTotalReleased: Int64;
    FTotalValidatedOK: Int64;
    FTotalFailedCreations: Int64;
    FTotalFailedValidations: Int64;
    FWaitCount: Integer;
    FWaitTimeAccumulatedMs: Int64;

    FCleanupTimer: TThreadTimer;
    FConnectionAvailableEvent: TEvent;
    FShuttingDown: Boolean;

    function GetPoolSnapshot: TPoolSnapshot;
    function CreateAndConnectNewDBConnection: IDBConnection;
    /// <summary>
    /// THREAD-SAFETY: Must be called under appropriate locks
    /// OWNERSHIP: Creates new TPooledDBConnection, pool takes ownership
    /// </summary>
    function CreateNewPooledConnectionWrapper: TPooledDBConnection;
    /// <summary>
    /// THREAD-SAFETY: Thread-safe, uses internal locking
    /// PERFORMANCE: Optimized for frequent calls during high concurrency
    /// </summary>
    function GetConnectionFromQueue: TPooledDBConnection; // Optimizado
    procedure RemoveConnectionFromActive(APooledConn: TPooledDBConnection; ADestroyWrapper: Boolean);
    procedure CleanupIdleConnectionsTimerEvent(Sender: TObject);
    procedure EnsureMinPoolSize;
    procedure LogPoolActivity(const AMessage: string; ALevel: TLogLevel = logDebug);
    procedure LogPoolStatus(const AReason: string);
    procedure ReturnConnectionToQueue(AConnection: TPooledDBConnection); // Optimizado
    procedure UpdateMetricsAtomic(AOperation: TProc);
    procedure TrimIdleConnections;
    procedure CloseAllConnections;
  public
    constructor Create(const AConfig: TDBConnectionConfig; AMonitor: IDBMonitor = nil);
    destructor Destroy; override;

    /// <summary>
    /// THREAD-SAFETY: Thread-safe, can be called concurrently by multiple threads
    /// BLOCKING: May block up to AAcquireTimeoutMs waiting for available connection
    /// OWNERSHIP: Returns IDBConnection interface, caller does not own underlying object
    /// EXCEPTIONS: Raises EDBPoolError on timeout or configuration errors
    /// </summary>
    function AcquireConnection(AAcquireTimeoutMs: Integer = -1): IDBConnection;
    /// <summary>
    /// THREAD-SAFETY: Thread-safe, can be called concurrently
    /// PERFORMANCE: Non-blocking operation, returns immediately
    /// OWNERSHIP: Pool reclaims ownership of connection for reuse
    /// </summary>
    procedure ReleaseConnection(ADBIntfToRelease: IDBConnection);
    /// <summary>
    /// THREAD-SAFETY: Thread-safe but may impact performance during execution
    /// BLOCKING: Temporarily blocks new acquisitions while validating
    /// SIDE-EFFECTS: May remove invalid connections from pool
    /// </summary>
    procedure ValidateAllIdleConnections;
    /// <summary>
    /// THREAD-SAFETY: Thread-safe
    /// OWNERSHIP: Returns JSON object copy, caller must free
    /// PERFORMANCE: Lightweight operation using atomic snapshots
    /// </summary>
    function GetPoolStats: TJSONObject;
    function GetIdleConnectionsCount: Integer;

    property Config: TDBConnectionConfig read FConfig;
    property CurrentSize: Integer read FCurrentSize;
    property ActiveConnections: Integer read FActiveConnectionCount;
    property IdleConnectionsCount: Integer read GetIdleConnectionsCount;
    property Name: string read FConfig.Name;
  end;

  TDBConnectionPoolManager = class
  private
    FMonitor: IDBMonitor;
    class var FInstance: TDBConnectionPoolManager;
    class var FSingletonLock: TCriticalSection;
    class var FInstanceDestroyed: Boolean;

    FPools: TObjectDictionary<string, TSingleDBConnectionPool>;
    FManagerLock: TCriticalSection;

    constructor CreatePrivate;
    class constructor CreateClassLock;
    class destructor DestroyClassLock;
  public
    destructor Destroy; override;
    class function GetInstance: TDBConnectionPoolManager;
    class procedure DestroyInstance;

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
  System.StrUtils, System.Rtti, FireDAC.Stan.Def,

  uLib.Config.Manager;

const
  // Timeouts configurables para operaciones del pool
  DEFAULT_CONNECTION_TIMEOUT_SEC = 15;
  DEFAULT_VALIDATION_TIMEOUT_SEC = 5;
  MAX_CONNECTION_TIMEOUT_SEC = 30;
  MAX_VALIDATION_TIMEOUT_SEC = 10;
  SLOW_OPERATION_THRESHOLD_MS = 2000;  //
  MAX_CLEANUP_TIME_MS = 30000; // 30 segundos máximo para cleanup
  VALIDATION_WARNING_THRESHOLD = 0.8;     // 80% del timeout para warning
  POOL_PRESSURE_THRESHOLD = 0.8;         // 80% capacidad = alta presión
  POOL_TRIM_THRESHOLD = 0.7;             // 70% capacidad = considerar trim
  MIN_QUEUE_CAPACITY = 10;               // Capacidad mínima de queue
  SAFETY_COUNTER_MULTIPLIER = 2;



// En uLib.Database.Pool.pas - remover constantes y agregar variables
var
  // Configuración dinámica del pool - se inicializa desde Config.Manager
  GDefaultConnectionTimeoutSec: Integer = DEFAULT_CONNECTION_TIMEOUT_SEC;
  GDefaultValidationTimeoutSec: Integer = DEFAULT_VALIDATION_TIMEOUT_SEC;
  GMaxConnectionTimeoutSec: Integer = MAX_CONNECTION_TIMEOUT_SEC;
  GMaxValidationTimeoutSec: Integer = MAX_VALIDATION_TIMEOUT_SEC;
  GSlowOperationThresholdMs: Integer = SLOW_OPERATION_THRESHOLD_MS;
  GMaxCleanupTimeMs: Integer = MAX_CLEANUP_TIME_MS;
  GValidationWarningThreshold: Double = VALIDATION_WARNING_THRESHOLD;
  GPoolPressureThreshold: Double = POOL_PRESSURE_THRESHOLD;
  GPoolTrimThreshold: Double = POOL_TRIM_THRESHOLD;
  GMinQueueCapacity: Integer = MIN_QUEUE_CAPACITY;
  GSafetyCounterMultiplier: Integer = SAFETY_COUNTER_MULTIPLIER;
  GPoolConfigInitialized: Boolean = False;

procedure InitializePoolConfiguration;
var
  ConfigMgr: TConfigManager;
begin
  if GPoolConfigInitialized then Exit;

  try
    ConfigMgr := TConfigManager.GetInstance;
    if Assigned(ConfigMgr) and Assigned(ConfigMgr.ConfigData) then
    begin
      // Timeouts de conexión
      GDefaultConnectionTimeoutSec := TJSONHelper.GetInteger(ConfigMgr.ConfigData,
        'database.defaults.connectionTimeoutSeconds', GDefaultConnectionTimeoutSec);
      GDefaultValidationTimeoutSec := TJSONHelper.GetInteger(ConfigMgr.ConfigData,
        'database.defaults.defaultValidationTimeoutSec', GDefaultValidationTimeoutSec);
      GMaxConnectionTimeoutSec := TJSONHelper.GetInteger(ConfigMgr.ConfigData,
        'database.defaults.maxConnectionTimeoutSec', GMaxConnectionTimeoutSec);
      GMaxValidationTimeoutSec := TJSONHelper.GetInteger(ConfigMgr.ConfigData,
        'database.defaults.maxValidationTimeoutSec', GMaxValidationTimeoutSec);

      // Thresholds de rendimiento
      GSlowOperationThresholdMs := TJSONHelper.GetInteger(ConfigMgr.ConfigData,
        'database.defaults.slowOperationThresholdMs', GSlowOperationThresholdMs);
      GMaxCleanupTimeMs := TJSONHelper.GetInteger(ConfigMgr.ConfigData,
        'database.defaults.maxCleanupTimeMs', GMaxCleanupTimeMs);

      // Thresholds de pool (como porcentajes decimales)
      GValidationWarningThreshold := TJSONHelper.GetDouble(ConfigMgr.ConfigData,
        'database.pool.validationWarningThreshold', GValidationWarningThreshold);
      GPoolPressureThreshold := TJSONHelper.GetDouble(ConfigMgr.ConfigData,
        'database.pool.pressureThreshold', GPoolPressureThreshold);
      GPoolTrimThreshold := TJSONHelper.GetDouble(ConfigMgr.ConfigData,
        'database.pool.trimThreshold', GPoolTrimThreshold);

      // Configuración de estructuras internas
      GMinQueueCapacity := TJSONHelper.GetInteger(ConfigMgr.ConfigData,
        'database.pool.minQueueCapacity', GMinQueueCapacity);
      GSafetyCounterMultiplier := TJSONHelper.GetInteger(ConfigMgr.ConfigData,
        'database.pool.safetyCounterMultiplier', GSafetyCounterMultiplier);

      LogMessage('Database pool configuration loaded from Config.Manager', logInfo);
    end
    else
    begin
      LogMessage('Config.Manager not available, using hardcoded database pool defaults', logWarning);
    end;
  except
    on E: Exception do
    begin
      LogMessage(Format('Error loading database pool configuration: %s. Using defaults.', [E.Message]), logWarning);
    end;
  end;

  // Validar rangos
  if GDefaultConnectionTimeoutSec < 1 then GDefaultConnectionTimeoutSec := 15;
  if GDefaultValidationTimeoutSec < 1 then GDefaultValidationTimeoutSec := 5;
  if GMaxConnectionTimeoutSec < GDefaultConnectionTimeoutSec then GMaxConnectionTimeoutSec := GDefaultConnectionTimeoutSec;
  if GSlowOperationThresholdMs < 100 then GSlowOperationThresholdMs := 2000;
  if GMaxCleanupTimeMs < 5000 then GMaxCleanupTimeMs := 30000;
  if GValidationWarningThreshold < 0.1 then GValidationWarningThreshold := 0.8;
  if GPoolPressureThreshold < 0.1 then GPoolPressureThreshold := 0.8;
  if GPoolTrimThreshold < 0.1 then GPoolTrimThreshold := 0.7;
  if GMinQueueCapacity < 5 then GMinQueueCapacity := 10;
  if GSafetyCounterMultiplier < 1 then GSafetyCounterMultiplier := 2;

  GPoolConfigInitialized := True;
end;

function TPoolSnapshot.ToJSON: TJSONObject;
begin
  Result := TJSONObject.Create;
  Result.AddPair('current_size', CurrentSize);
  Result.AddPair('active_connections', ActiveConnectionCount);
  Result.AddPair('idle_connections', IdleConnectionCount);
  Result.AddPair('waiting_requests', WaitingRequestsCount);
  Result.AddPair('total_created', TotalCreated);
  Result.AddPair('total_acquired', TotalAcquired);
  Result.AddPair('total_released', TotalReleased);
  Result.AddPair('snapshot_time_utc', DateToISO8601(SnapshotTime));
end;

{ TConnectionQueue }

constructor TConnectionQueue.Create(ACapacity: Integer);
begin
  inherited Create;
  InitializePoolConfiguration; // Asegurar que la configuración esté cargada
  FCapacity := Max(ACapacity, GMinQueueCapacity); // Usar variable global
  SetLength(FConnections, FCapacity);
  FHead := 0;
  FTail := 0;
  FCount := 0;
  FLock := TCriticalSection.Create;
  FIsDestroying := False;
end;

destructor TConnectionQueue.Destroy;
begin
  FIsDestroying := True;
  // Pequeña pausa para permitir que otros threads vean el flag
  Sleep(1);
  Clear;
  FreeAndNil(FLock);
  SetLength(FConnections, 0);
  inherited;
end;

// En uLib.Database.Pool.pas - TConnectionQueue methods
function TConnectionQueue.TryDequeue(out AConnection: TPooledDBConnection): Boolean;
begin
  Result := False;
  AConnection := nil;

  if FIsDestroying or not Assigned(FLock) then
  begin
    {$IFDEF DEBUG}
    LogMessage(Format('TConnectionQueue.TryDequeue: Queue@%p is destroying or lock not assigned. Thread=%d',
      [Pointer(Self), TThread.CurrentThread.ThreadID]), logSpam);
    {$ENDIF}
    Exit;
  end;

  FLock.Acquire;
  try
    if FCount > 0 then
    begin
      AConnection := FConnections[FHead];
      FConnections[FHead] := nil; // Clear reference for debugging
      FHead := (FHead + 1) mod FCapacity;
      Dec(FCount);
      Result := True;

      {$IFDEF DEBUG}
      LogMessage(Format('TConnectionQueue.TryDequeue: Connection@%p dequeued by Thread=%d. Remaining count: %d',
        [Pointer(AConnection), TThread.CurrentThread.ThreadID, FCount]), logSpam);
      {$ENDIF}
    end
    else
    begin
      {$IFDEF DEBUG}
      LogMessage(Format('TConnectionQueue.TryDequeue: Queue@%p empty. Thread=%d',
        [Pointer(Self), TThread.CurrentThread.ThreadID]), logSpam);
      {$ENDIF}
    end;
  finally
    FLock.Release;
  end;
end;

function TConnectionQueue.TryEnqueue(AConnection: TPooledDBConnection): Boolean;
begin
  Result := False;
  if FIsDestroying or
     not Assigned(AConnection) or
     not Assigned(FLock) then
  begin
    {$IFDEF DEBUG}
    LogMessage(Format('TConnectionQueue.TryEnqueue: Cannot enqueue. Destroying=%s, Connection@%p, Lock@%p, Thread=%d',
      [BoolToStr(FIsDestroying, True), Pointer(AConnection), Pointer(FLock), TThread.CurrentThread.ThreadID]), logSpam);
    {$ENDIF}
    Exit;
  end;

  FLock.Acquire;
  try
    if FCount < FCapacity then
    begin
      FConnections[FTail] := AConnection;
      FTail := (FTail + 1) mod FCapacity;
      Inc(FCount);
      Result := True;

      {$IFDEF DEBUG}
      LogMessage(Format('TConnectionQueue.TryEnqueue: Connection@%p enqueued by Thread=%d. New count: %d',
        [Pointer(AConnection), TThread.CurrentThread.ThreadID, FCount]), logSpam);
      {$ENDIF}
    end
    else
    begin
      LogMessage(Format('TConnectionQueue.TryEnqueue: Queue@%p full (capacity=%d). Thread=%d cannot enqueue Connection@%p',
        [Pointer(Self), FCapacity, TThread.CurrentThread.ThreadID, Pointer(AConnection)]), logWarning);
    end;
  finally
    FLock.Release;
  end;
end;

function TConnectionQueue.Count: Integer;
begin
  if FIsDestroying or not Assigned(FLock) then
  begin
    Result := 0;
    Exit;
  end;
  FLock.Acquire;
  try
    Result := FCount;
  finally
    FLock.Release;
  end;
end;

function TConnectionQueue.IsEmpty: Boolean;
begin
  Result := (Count = 0);
end;

function TConnectionQueue.IsFull: Boolean;
begin
  Result := (Count >= FCapacity);
end;

procedure TConnectionQueue.Clear;
var
  Conn: TPooledDBConnection;
  SafetyCounter: Integer;
  LocalLock: TCriticalSection;
begin
  InitializePoolConfiguration;
  LocalLock := FLock;

  if not Assigned(LocalLock) or FIsDestroying then
  begin
    // Limpieza sin lock durante destrucción
    SafetyCounter := 0;
    while (FCount > 0) and (SafetyCounter < FCapacity * GSafetyCounterMultiplier) do // Usar variable global
    begin
      Conn := FConnections[FHead];
      FConnections[FHead] := nil;
      FHead := (FHead + 1) mod FCapacity;
      Dec(FCount);
      Inc(SafetyCounter);

      if Assigned(Conn) then
      begin
        try
          FreeAndNil(Conn);
        except
          // Silenciar errores durante destrucción
        end;
      end;
    end;

    // Reset final
    FHead := 0;
    FTail := 0;
    FCount := 0;
    Exit;
  end;

  // Operación normal con lock, pero verificar nuevamente dentro del lock
  LocalLock.Acquire;
  try
    // Re-verificar estado bajo lock
    if FIsDestroying then
    begin
      // Si entró en modo destrucción mientras esperaba el lock, salir
      Exit;
    end;

    SafetyCounter := 0;
    while (FCount > 0) and (SafetyCounter < FCapacity * SAFETY_COUNTER_MULTIPLIER) do
    begin
      Conn := FConnections[FHead];
      FConnections[FHead] := nil;
      FHead := (FHead + 1) mod FCapacity;
      Dec(FCount);
      Inc(SafetyCounter);

      if Assigned(Conn) then
      begin
        try
          FreeAndNil(Conn);
        except
          on E: Exception do
            LogMessage(Format('Error freeing connection in queue clear: %s', [E.Message]), logError);
        end;
      end;
    end;

    // Reset final de índices
    FHead := 0;
    FTail := 0;
    FCount := 0;

  finally
    LocalLock.Release;
  end;
end;

{ TPooledDBConnection }

constructor TPooledDBConnection.Create(const APoolName: string; ADBCoFactory: TFunc<IDBConnection>;
  AUnderlyingObject: TObject; APoolOwner: TSingleDBConnectionPool);
begin
  inherited Create;
  FStateLock := TCriticalSection.Create;
  FID := TGuid.NewGuid.ToString;
  FPoolName := APoolName;
  FPoolOwner := APoolOwner;
  FValidationInterval := 300; // 5 minutos por defecto
  try
    FDBConnectionIntf := ADBCoFactory();

    FDBConnectionIntf := ADBCoFactory();
    FUnderlyingConnectionObject := AUnderlyingObject;

    if not Assigned(FDBConnectionIntf) then
      raise EDBPoolError.CreateFmt('Failed to create DBConnectionIntf in TPooledDBConnection for pool "%s". Factory returned nil.', [APoolName]);
    if not Assigned(FUnderlyingConnectionObject) then
      raise EDBPoolError.CreateFmt('Underlying connection object is nil in TPooledDBConnection for pool "%s".', [APoolName]);

    FCreatedTime := NowUTC;
    FLastUsedTime := FCreatedTime;
    FLastValidationTime := FCreatedTime;
    FUsageCount := 0;
    FConnectionStateInPool := csNew;
  except
    FreeAndNil(FStateLock);
    raise;
  end;
end;

destructor TPooledDBConnection.Destroy;
begin
  try
    DisconnectAndClose;
  except
    on E: Exception do
      LogMessage(Format('Error in TPooledDBConnection.Destroy for %s: %s', [FID, E.Message]), logError);
  end;
  FreeAndNil(FStateLock);
  inherited;
end;

function TPooledDBConnection.GetUnderlyingBaseConnection: TBaseConnection;
begin
  if Assigned(FUnderlyingConnectionObject) and (FUnderlyingConnectionObject is TBaseConnection) then
    Result := FUnderlyingConnectionObject as TBaseConnection
  else
    Result := nil;
end;

procedure TPooledDBConnection.SetConnectionStateInPool(AValue: TConnectionState);
var
  OldState: TConnectionState;
begin
  if not Assigned(FStateLock) then
  begin
    FConnectionStateInPool := AValue;
    Exit;
  end;

  FStateLock.Acquire;
  try
    if FConnectionStateInPool <> AValue then
    begin
      OldState := FConnectionStateInPool;
      FConnectionStateInPool := AValue;
      if not ((OldState = csIdle) and (AValue = csInUse)) and
         not ((OldState = csInUse) and (AValue = csIdle)) then
      begin
        LogMessage(Format('PooledConnection %s (Pool: %s) state change: %s -> %s',
          [FID, FPoolName,
           TRttiEnumerationType.GetName<TConnectionState>(OldState),
           TRttiEnumerationType.GetName<TConnectionState>(AValue)]), logDebug);
      end;
    end;
  finally
    FStateLock.Release;
  end;
end;

function TPooledDBConnection.NeedsValidation: Boolean;
begin
  Result := (SecondsBetween(NowUTC, FLastValidationTime) >= FValidationInterval);
end;

procedure TPooledDBConnection.MarkAsUsed;
begin
  if Assigned(FStateLock) then
  begin
    FStateLock.Acquire;
    try
      FLastUsedTime := NowUTC;
    finally
      FStateLock.Release;
    end;
  end
  else
    FLastUsedTime := NowUTC;

  TInterlocked.Increment(FUsageCount);
end;

function TPooledDBConnection.ConnectIfNecessary: Boolean;
var
  LBaseConn: TBaseConnection;
  OriginalTimeout: Integer;
  ConnectionTimeout: Integer;
begin
  InitializePoolConfiguration;
  Result := False;

  if not Assigned(FDBConnectionIntf) then
  begin
    LogMessage(Format('Cannot connect PooledConnection %s (Pool: %s): DBConnectionIntf is nil.', [FID, FPoolName]), logError);
    Exit;
  end;

  if FDBConnectionIntf.IsConnected then
  begin
    Result := True;
    Exit;
  end;

  LogMessage(Format('Connecting PooledConnection %s (Pool: %s)...', [FID, FPoolName]), logDebug);

  try
    LBaseConn := GetUnderlyingBaseConnection;
    if Assigned(LBaseConn) then
    begin
      OriginalTimeout := -1;
      try
        // Establecer timeout de conexión
        try
          OriginalTimeout := LBaseConn.GetQueryTimeout;

          // Timeout de conexión (máximo 30 segundos)
          if Assigned(FPoolOwner) then
             ConnectionTimeout := Max(5, Min(GMaxConnectionTimeoutSec, FPoolOwner.Config.ConnectionTimeout)) // Usar variable global
          else
             ConnectionTimeout := GDefaultConnectionTimeoutSec;
          LBaseConn.SetQueryTimeout(ConnectionTimeout);
        except
          on E: Exception do
            LogMessage(Format('Warning: Could not set connection timeout for %s: %s', [FID, E.Message]), logWarning);
        end;

        // Intentar conectar con timeout
        Result := FDBConnectionIntf.Connect;

      finally
        // Restaurar timeout original
        if (OriginalTimeout >= 0) and Assigned(LBaseConn) then
        begin
          try
            LBaseConn.SetQueryTimeout(OriginalTimeout);
          except
            on E: Exception do
              LogMessage(Format('Warning: Could not restore original timeout for %s: %s', [FID, E.Message]), logWarning);
          end;
        end;
      end;
    end
    else
    begin
      // Fallback sin timeout específico
      Result := FDBConnectionIntf.Connect;
    end;

    if Result then
    begin
      if FUsageCount = 0 then
        LogMessage(Format('PooledConnection %s (Pool: %s) connected successfully.', [FID, FPoolName]), logDebug);
    end
    else
    begin
      ConnectionStateInPool := csInvalid;
      LogMessage(Format('Failed to connect PooledConnection %s (Pool: %s). LastError: %s',
        [FID, FPoolName, FDBConnectionIntf.GetLastError]), logError);
    end;

  except
    on E: Exception do
    begin
      ConnectionStateInPool := csInvalid;
      LogMessage(Format('Exception connecting PooledConnection %s (Pool: %s): %s', [FID, FPoolName, E.Message]), logError);
      Result := False;
    end;
  end;
end;

procedure TPooledDBConnection.DisconnectAndClose;
var
  LBaseConn: TBaseConnection;
begin
  if Assigned(FUnderlyingConnectionObject) then
  begin
    LBaseConn := GetUnderlyingBaseConnection;
    if Assigned(LBaseConn) and LBaseConn.IsConnected then
    begin
      try
        LBaseConn.Disconnect;
      except
        on E: Exception do
          LogMessage(Format('Error disconnecting underlying connection for %s: %s', [FID, E.Message]), logError);
      end;
    end;

    try
      FUnderlyingConnectionObject:=Nil;
    except
      on E: Exception do
        LogMessage(Format('Error freeing underlying connection object for %s: %s', [FID, E.Message]), logError);
    end;
  end;

  FDBConnectionIntf := nil;
  ConnectionStateInPool := csClosed;
end;

function TPooledDBConnection.IsValidForPool: Boolean;
var
  LBaseConn: TBaseConnection;
  OriginalTimeout: Integer;
  ValidationTimeout: Integer;
begin
  Result := False;

  if not NeedsValidation then
  begin
    Result := (FConnectionStateInPool <> csInvalid) and
              Assigned(FDBConnectionIntf) and
              FDBConnectionIntf.IsConnected;

    // Verificación adicional para conexiones que parecen válidas pero pueden estar muertas
    if Result and Assigned(FDBConnectionIntf) then
    begin
      try
        // Verificación rápida del estado de la conexión subyacente
        var ABaseConn := GetUnderlyingBaseConnection;
        if Assigned(ABaseConn) then
          Result := ABaseConn.IsConnected;
      except
        // Si falla la verificación básica, marcar como inválida
        ConnectionStateInPool := csInvalid;
        Result := False;
      end;
    end;
    Exit;
  end;

  LBaseConn := GetUnderlyingBaseConnection;
  if not Assigned(LBaseConn) or not LBaseConn.IsConnected then
  begin
    LogMessage(Format('Validation of PooledConnection %s (Pool: %s) failed: Not connected or underlying object invalid.', [FID, FPoolName]), logDebug);
    ConnectionStateInPool := csInvalid;
    Exit;
  end;

  OriginalTimeout := -1;
  try
    try
      OriginalTimeout := LBaseConn.GetQueryTimeout;
      LBaseConn.SetQueryTimeout(ValidationTimeout);
      // Timeout de validación progresivo basado en el estado de la conexión
      if Assigned(FPoolOwner) then
      begin
        // Timeout más corto para conexiones que han fallado antes
        if FConnectionStateInPool = csInvalid then
          ValidationTimeout := 2 // 2 segundos para conexiones problemáticas
        else if SecondsBetween(NowUTC, FLastUsedTime) > 3600 then
          ValidationTimeout := 3 // 3 segundos para conexiones muy inactivas
        else
          ValidationTimeout := Max(1, Min(5, FPoolOwner.Config.ConnectionTimeout div 10));
      end
      else
        ValidationTimeout := 5;

      LBaseConn.SetQueryTimeout(ValidationTimeout);

      // Realizar ping de validación con timeout
      {$IFDEF MSWINDOWS}
      var StartTime := GetTickCount;
      LBaseConn.ExecuteScalar('SELECT 1');
      var ElapsedMs := GetTickCount - StartTime;
      {$ENDIF}
      // Log si la validación tardó mucho
      if ElapsedMs > (ValidationTimeout * 1000 * VALIDATION_WARNING_THRESHOLD) then
        LogMessage(Format('Validation of connection %s took %dms (timeout was %ds)', [FID, ElapsedMs, ValidationTimeout]), logWarning);
      Result := True;
      FLastValidationTime := NowUTC;
      if FUsageCount <= 1 then
         LogMessage(Format('PooledConnection %s (Pool: %s) validated successfully with ping.', [FID, FPoolName]), logDebug);
    finally
      // Restaurar timeout original solo si lo obtuvimos exitosamente
      if (OriginalTimeout >= 0) and Assigned(LBaseConn) then
      begin
        try
          LBaseConn.SetQueryTimeout(OriginalTimeout);
        except
          on E: Exception do
            LogMessage(Format('Warning: Could not restore original timeout for connection %s: %s', [FID, E.Message]), logWarning);
        end;
      end;
    end;
  except
    on E: Exception do
    begin
      LogMessage(Format('PING validation of PooledConnection %s (Pool: %s) FAILED: %s', [FID, FPoolName, E.Message]), logWarning);
      Result := False;
      ConnectionStateInPool := csInvalid;
    end;
  end;
end;
{ TSingleDBConnectionPool }

constructor TSingleDBConnectionPool.Create(const AConfig: TDBConnectionConfig; AMonitor: IDBMonitor = nil);
begin
  inherited Create;
  InitializePoolConfiguration; // Asegurar que la configuración esté cargada

  FConfig := AConfig;
  FConfig.Validate;
  FMonitor := AMonitor;
  FShuttingDown := False;

  // Usar configuración dinámica
  FIdleQueue := TConnectionQueue.Create(FConfig.MaxPoolSize);

  FActiveConnections := TThreadList<TPooledDBConnection>.Create;
  FMetricsLock := TCriticalSection.Create;
  FConnectionAvailableEvent := TEvent.Create(nil, True, False, '', False);
  // Inicializar métricas
  FCurrentSize := 0;
  FActiveConnectionCount := 0;
  FTotalCreated := 0;
  FTotalAcquired := 0;
  FTotalReleased := 0;
  FTotalValidatedOK := 0;
  FTotalFailedCreations := 0;
  FTotalFailedValidations := 0;
  FWaitCount := 0;
  FWaitTimeAccumulatedMs := 0;

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
  FShuttingDown := True;

  if Assigned(FCleanupTimer) then
  begin
    FCleanupTimer.Enabled := False;
    FreeAndNil(FCleanupTimer);
  end;

  CloseAllConnections;

  FreeAndNil(FIdleQueue);
  FreeAndNil(FActiveConnections);
  FreeAndNil(FMetricsLock);
  FreeAndNil(FConnectionAvailableEvent);

  LogMessage(Format('TSingleDBConnectionPool "%s" destroyed.', [FConfig.Name]), logInfo);
  inherited;
end;

function TSingleDBConnectionPool.GetIdleConnectionsCount: Integer;
begin
  Result := 0;
  if Assigned(FIdleQueue) then
  begin
    try
      Result := FIdleQueue.Count;
      // NOTA: Este valor puede volverse obsoleto inmediatamente después del retorno
      // debido a operaciones concurrentes en otros threads. Usar solo para logging,
      // métricas o decisiones no críticas.
    except
      on E: Exception do
      begin
        LogMessage(Format('Pool "%s": Error getting idle connections count: %s', [FConfig.Name, E.Message]), logError);
        Result := 0;
      end;
    end;
  end;

  // Log solo si el valor parece inconsistente para debugging
  if Result < 0 then
  begin
    LogMessage(Format('Pool "%s": WARNING - GetIdleConnectionsCount returned negative value: %d', [FConfig.Name, Result]), logWarning);
    Result := 0;
  end;
end;

function TSingleDBConnectionPool.GetPoolSnapshot: TPoolSnapshot;
begin
  // Tomar snapshot atómico de todas las métricas importantes
  FMetricsLock.Acquire;
  try
    Result.CurrentSize := FCurrentSize;
    Result.ActiveConnectionCount := FActiveConnectionCount;
    Result.IdleConnectionCount := GetIdleConnectionsCount; // Llamar dentro del lock
    Result.WaitingRequestsCount := FWaitCount;
    Result.TotalCreated := FTotalCreated;
    Result.TotalAcquired := FTotalAcquired;
    Result.TotalReleased := FTotalReleased;
    Result.SnapshotTime := NowUTC;
  finally
    FMetricsLock.Release;
  end;
end;

function TSingleDBConnectionPool.GetConnectionFromQueue: TPooledDBConnection;
begin
  Result := nil;
  if Assigned(FIdleQueue) then
    FIdleQueue.TryDequeue(Result);
end;

procedure TSingleDBConnectionPool.ReturnConnectionToQueue(AConnection: TPooledDBConnection);
begin
  if Assigned(AConnection) and Assigned(FIdleQueue) then
  begin
    AConnection.ConnectionStateInPool := csIdle;
    if not FIdleQueue.TryEnqueue(AConnection) then
    begin
      // Queue está llena, liberar la conexión
      LogMessage(Format('Pool "%s": Idle queue full, disposing connection %s', [FConfig.Name, AConnection.ID]), logWarning);
      FreeAndNil(AConnection);
      TInterlocked.Decrement(FCurrentSize);
    end;
  end;
end;

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

    // Establecer timeout antes de conectar
    try
      var ConnectionTimeout := Max(10, Min(30, FConfig.ConnectionTimeout));
      LBaseConn.SetQueryTimeout(ConnectionTimeout);
    except
      on E: Exception do
        LogMessage(Format('Pool "%s": Warning - Could not set connection timeout: %s', [FConfig.Name, E.Message]), logWarning);
    end;
    var StartTime := GetTickCount;
    if LBaseConn.Connect then
    begin
      var ConnectTime := GetTickCount - StartTime;
      if ConnectTime > 5000 then // Log si tardó más de 5 segundos
        LogMessage(Format('Pool "%s": Direct connection took %dms to establish', [FConfig.Name, ConnectTime]), logWarning);

      Result := LBaseConn;
      TInterlocked.Increment(FTotalCreated);
      LogMessage(Format('Pool "%s" (Pooling Disabled): Direct DB connection created and connected.', [FConfig.Name]), logInfo);
    end
    else
    begin
      LogMessage(Format('Pool "%s" (Pooling Disabled): Failed to connect direct DB instance. LastError: %s',
        [FConfig.Name, LBaseConn.GetLastError]), logError);
      TInterlocked.Increment(FTotalFailedCreations);
      FreeAndNil(LBaseConn);
    end;

  except
    on E: Exception do
    begin
      LogMessage(Format('Pool "%s" (Pooling Disabled): Exception during CreateAndConnectNewDBConnection: %s', [FConfig.Name, E.Message]), logError);
      TInterlocked.Increment(FTotalFailedCreations);
      FreeAndNil(LBaseConn);
      raise;
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

  LogMessage(Format('Pool "%s": Creating new pooled DB connection wrapper...', [FConfig.Name]), logDebug);
  try
    case FConfig.DBType of
      dbtMSSQL:      LDBConnIntf := TMSSQLConnection.Create(FConfig, FMonitor);
      dbtPostgreSQL: LDBConnIntf := TPostgreSQLConnection.Create(FConfig, FMonitor);
      dbtMySQL:      LDBConnIntf := TMySQLConnection.Create(FConfig, FMonitor);
    else
      raise EDBPoolError.CreateFmt('Pool "%s": Unsupported DBType specified: %s',
        [FConfig.Name, TRttiEnumerationType.GetName<TDBType>(FConfig.DBType)]);
    end;

    LUnderlyingObject := LDBConnIntf as TObject;
    LDBConnFactory := function: IDBConnection begin Result := LDBConnIntf; end;

    Result := TPooledDBConnection.Create(FConfig.Name, LDBConnFactory, LUnderlyingObject, Self);

    if Result.ConnectIfNecessary then
    begin
      TInterlocked.Increment(FTotalCreated);
      LogMessage(Format('Pool "%s": New pooled DB connection %s created and connected.', [FConfig.Name, Result.ID]), logInfo);
    end
    else
    begin
      LogMessage(Format('Pool "%s": Failed to connect new pooled DB instance %s. LastError: %s',
        [FConfig.Name, Result.ID, Result.DBConnectionIntf.GetLastError]), logError);
      TInterlocked.Increment(FTotalFailedCreations);
      FreeAndNil(Result);
    end;
  except
    on E: Exception do
    begin
      LogMessage(Format('Pool "%s": Exception during CreateNewPooledConnectionWrapper: %s', [FConfig.Name, E.Message]), logError);
      TInterlocked.Increment(FTotalFailedCreations);
      if Assigned(Result) then
        FreeAndNil(Result);
      LDBConnIntf := nil;
      Result := nil;
    end;
  end;
end;

procedure TSingleDBConnectionPool.RemoveConnectionFromActive(APooledConn: TPooledDBConnection; ADestroyWrapper: Boolean);
var
  List: TList<TPooledDBConnection>;
begin
  if not Assigned(APooledConn) or not Assigned(FActiveConnections) then Exit;

  List := FActiveConnections.LockList;
  try
    if List.Remove(APooledConn) >= 0 then
    begin
      TInterlocked.Decrement(FCurrentSize);
      TInterlocked.Decrement(FActiveConnectionCount);

      if ADestroyWrapper then
      begin
        try
          FreeAndNil(APooledConn);
        except
          on E: Exception do
            LogMessage(Format('Error destroying connection in RemoveConnectionFromActive: %s', [E.Message]), logError);
        end;
      end;
    end;
  finally
    FActiveConnections.UnlockList;
  end;
end;

procedure TSingleDBConnectionPool.EnsureMinPoolSize;
var
  i, NumToCreate: Integer;
  NewConnWrapper: TPooledDBConnection;
begin
  if not FConfig.PoolingEnabled or FShuttingDown then Exit;

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
        ReturnConnectionToQueue(NewConnWrapper);
        TInterlocked.Increment(FCurrentSize);
      end
      else
        LogMessage(Format('Pool "%s": Failed to create initial connection during EnsureMinPoolSize.', [FConfig.Name]), logError);
    end;
    LogPoolStatus('After EnsureMinPoolSize');
  end;
end;

function TSingleDBConnectionPool.AcquireConnection(AAcquireTimeoutMs: Integer = -1): IDBConnection;
var
  LAcquireTimeoutMs: Integer;
  PooledConnToUse: TPooledDBConnection;
  Stopwatch: TStopwatch;
  RemainingTimeoutMs: Integer;
  NewConnWrapper: TPooledDBConnection;
  ValidationStart: Cardinal;
  IsValid: Boolean;
  ValidationTime: Cardinal;
  List: TList<TPooledDBConnection>;
begin
  Result := nil;

  if FShuttingDown then
  begin
    LogMessage(Format('Pool "%s": Cannot acquire connection - pool is shutting down.', [FConfig.Name]), logWarning);
    raise EDBPoolError.CreateFmt('Pool "%s": Cannot acquire connection - pool is shutting down.', [FConfig.Name]);
  end;

  // Si el pooling está deshabilitado, crear conexión directa
  if not FConfig.PoolingEnabled then
  begin
    LogMessage(Format('Pool "%s": Pooling disabled. Creating direct connection.', [FConfig.Name]), logDebug);
    Result := CreateAndConnectNewDBConnection;
    if not Assigned(Result) then
      raise EDBPoolError.CreateFmt('Pool "%s": Failed to create direct connection when pooling is disabled.', [FConfig.Name]);
    Exit;
  end;

  LAcquireTimeoutMs := IfThen(AAcquireTimeoutMs < 0, FConfig.AcquireTimeout, AAcquireTimeoutMs);
  Stopwatch := TStopwatch.StartNew;
  TInterlocked.Increment(FWaitCount);

  try
    repeat
      PooledConnToUse := nil;

      // 1. Intentar obtener conexión de la cola idle
      PooledConnToUse := GetConnectionFromQueue;
      if Assigned(PooledConnToUse) then
      begin
        // Validar con timeout para evitar bloqueos
        ValidationStart := GetTickCount;
        IsValid := False;

        try
          IsValid := PooledConnToUse.IsValidForPool;
          ValidationTime := GetTickCount - ValidationStart;

          // Log validaciones lentas
          if ValidationTime > SLOW_OPERATION_THRESHOLD_MS then
            LogMessage(Format('Pool "%s": Connection validation took %dms', [FConfig.Name, ValidationTime]), logWarning);

        except
          on E: Exception do
          begin
            LogMessage(Format('Pool "%s": Connection validation failed with exception: %s', [FConfig.Name, E.Message]), logError);
            IsValid := False;
          end;
        end;

        if IsValid then
        begin
          PooledConnToUse.ConnectionStateInPool := csInUse;
          PooledConnToUse.MarkAsUsed;

          // Agregar a conexiones activas usando TThreadList
          List := FActiveConnections.LockList;
          try
            List.Add(PooledConnToUse);
            TInterlocked.Increment(FActiveConnectionCount);
          finally
            FActiveConnections.UnlockList;
          end;

          Result := PooledConnToUse.DBConnectionIntf;
          TInterlocked.Increment(FTotalAcquired);
          TInterlocked.Increment(FTotalValidatedOK);
          Break; // Salir del loop
        end
        else
        begin
          // Conexión inválida, desecharla
          TInterlocked.Increment(FTotalFailedValidations);
          TInterlocked.Decrement(FCurrentSize);

          try
            FreeAndNil(PooledConnToUse);
          except
            on E: Exception do
              LogMessage(Format('Pool "%s": Error freeing invalid connection: %s', [FConfig.Name, E.Message]), logError);
          end;

          LogMessage(Format('Pool "%s": Removed invalid idle connection', [FConfig.Name]), logDebug);
        end;
      end;

      // 2. Si no hay idle válida y el pool puede crecer, crear nueva
      if not Assigned(Result) and (FCurrentSize < FConfig.MaxPoolSize) then
      begin
        LogMessage(Format('Pool "%s": Growing pool. Current: %d, Max: %d',
          [FConfig.Name, FCurrentSize, FConfig.MaxPoolSize]), logDebug);

        NewConnWrapper := CreateNewPooledConnectionWrapper;
        if Assigned(NewConnWrapper) then
        begin
          NewConnWrapper.ConnectionStateInPool := csInUse;
          NewConnWrapper.MarkAsUsed;

          // Agregar a conexiones activas usando TThreadList
          List := FActiveConnections.LockList;
          try
            List.Add(NewConnWrapper);
            TInterlocked.Increment(FActiveConnectionCount);
            TInterlocked.Increment(FCurrentSize);
          finally
            FActiveConnections.UnlockList;
          end;

          Result := NewConnWrapper.DBConnectionIntf;
          TInterlocked.Increment(FTotalAcquired);
          Break; // Salir del loop, conexión creada exitosamente
        end
        else
          LogMessage(Format('Pool "%s": Failed to grow pool.', [FConfig.Name]), logWarning);
      end;

      // 3. Si no se pudo obtener/crear una conexión, esperar
      if not Assigned(Result) then
      begin
        RemainingTimeoutMs := LAcquireTimeoutMs - Stopwatch.ElapsedMilliseconds;
        if RemainingTimeoutMs <= 0 then
        begin
          // Actualizar métricas de forma atómica
          UpdateMetricsAtomic(
            procedure
            begin
              TInterlocked.Decrement(FWaitCount);
              TInterlocked.ExChange(FWaitTimeAccumulatedMs, Stopwatch.ElapsedMilliseconds);
            end);

          Stopwatch.Stop;
          LogMessage(Format('Pool "%s": Timeout after %dms. Max: %d, Current: %d, Active: %d, Idle: %d',
            [FConfig.Name, Stopwatch.ElapsedMilliseconds, FConfig.MaxPoolSize, FCurrentSize,
             FActiveConnectionCount, GetIdleConnectionsCount]), logError);

          raise EDBPoolError.CreateFmt('Pool "%s": Timeout (%dms) acquiring database connection.',
            [FConfig.Name, LAcquireTimeoutMs]);
        end;

        // Solo log cada 5 segundos para evitar spam
        if (RemainingTimeoutMs mod 5000) < 250 then
          LogMessage(Format('Pool "%s": Waiting for connection. Timeout left: %dms',
            [FConfig.Name, RemainingTimeoutMs]), logDebug);

        FConnectionAvailableEvent.WaitFor(Max(10, Min(RemainingTimeoutMs, 250)));
      end;

    until Assigned(Result) or FShuttingDown;

    if Assigned(Result) then
    begin
      // Actualizar métricas de forma atómica
      UpdateMetricsAtomic(
        procedure
        begin
          TInterlocked.Decrement(FWaitCount);
          TInterlocked.Exchange(FWaitTimeAccumulatedMs, Stopwatch.ElapsedMilliseconds);
        end);

      // Solo log si hay waiters o si el pool está casi lleno
      if (TInterlocked.CompareExchange(FWaitCount,0,0)>0) or
        (FActiveConnectionCount > (FConfig.MaxPoolSize * POOL_PRESSURE_THRESHOLD)) then
        LogMessage(Format('Pool "%s": Connection acquired. Active: %d, Idle: %d, Waiters: %d',
          [FConfig.Name, FActiveConnectionCount, GetIdleConnectionsCount, FWaitCount]), logDebug);
    end;

    Stopwatch.Stop;

  except
    on E: Exception do
    begin
      if Stopwatch.IsRunning then
      begin
        // Actualizar métricas de forma atómica en caso de excepción
        UpdateMetricsAtomic(
          procedure
          begin
            TInterlocked.Decrement(FWaitCount);
            TInterlocked.Exchange(FWaitTimeAccumulatedMs, Stopwatch.ElapsedMilliseconds);
          end);

        Stopwatch.Stop;
      end;

      LogPoolStatus('After AcquireConnection exception');
      raise;
    end;
  end;
end;

procedure TSingleDBConnectionPool.ReleaseConnection(ADBIntfToRelease: IDBConnection);
var
  ConnToRelease: TPooledDBConnection;
  Found: Boolean;
  ShouldSignalEvent: Boolean;
  i: Integer;
begin
  if not Assigned(ADBIntfToRelease) or FShuttingDown then Exit;

  ConnToRelease := nil;
  Found := False;
  ShouldSignalEvent := False;

  // Buscar en conexiones activas
  var List := FActiveConnections.LockList;
  try
    for i := 0 to List.Count - 1 do
    begin
      if List[i].DBConnectionIntf = ADBIntfToRelease then
      begin
        ConnToRelease := List[i];
        List.Delete(i);
        TInterlocked.Decrement(FActiveConnectionCount);
        Found := True;
        Break;
      end;
    end;
  finally
    FActiveConnections.UnlockList;
  end;

  if Found and Assigned(ConnToRelease) then
  begin
    if ConnToRelease.ConnectionStateInPool = csInUse then
    begin
      // Intentar devolver a la cola idle
      ReturnConnectionToQueue(ConnToRelease);
      TInterlocked.Increment(FTotalReleased);
      if (TInterlocked.CompareExchange(FWaitCount,0,0)>0) then
        ShouldSignalEvent := True;
    end
    else
    begin
      LogMessage(Format('Pool "%s": Attempt to release connection %s not in "csInUse" state (State: %s). Disposing.',
        [FConfig.Name, ConnToRelease.ID, TRttiEnumerationType.GetName<TConnectionState>(ConnToRelease.ConnectionStateInPool)]), logWarning);
      FreeAndNil(ConnToRelease);
      TInterlocked.Decrement(FCurrentSize);
    end;
  end
  else
  begin
    LogMessage( Format('Pool "%s": Release attempt for unknown connection (possibly direct connection).',
                  [FConfig.Name]), logDebug);
  end;

  if ShouldSignalEvent and Assigned(FConnectionAvailableEvent) then
  begin
    FConnectionAvailableEvent.SetEvent;
    LogMessage(Format('Pool "%s": Signaled FConnectionAvailableEvent due to release. Waiters: %d', [FConfig.Name, FWaitCount]), logSpam);
  end;
end;

procedure TSingleDBConnectionPool.ValidateAllIdleConnections;
var
  ConnectionsToValidate: TArray<TPooledDBConnection>;
  Conn: TPooledDBConnection;
  i: Integer;
  ValidCount: Integer;
  ValidatedCount, InvalidatedCount: Integer;
begin
  if FShuttingDown then Exit;

  LogMessage(Format('Pool "%s": Validating all idle connections...', [FConfig.Name]), logInfo);
  ValidatedCount := 0;
  InvalidatedCount := 0;

  // Obtener snapshot de conexiones idle para validar
  SetLength(ConnectionsToValidate, GetIdleConnectionsCount);
  ValidCount := 0;

  // Extraer conexiones para validar
  while ValidCount < Length(ConnectionsToValidate) do
  begin
    if not FIdleQueue.TryDequeue(Conn) then Break;
    ConnectionsToValidate[ValidCount] := Conn;
    Inc(ValidCount);
  end;

  // Redimensionar array al tamaño real
  SetLength(ConnectionsToValidate, ValidCount);

  // Validar cada conexión con manejo de excepciones
  for i := 0 to High(ConnectionsToValidate) do
  begin
    Conn := ConnectionsToValidate[i];
    if Assigned(Conn) then
    begin
      try
        if Conn.IsValidForPool then
        begin
          // Devolver conexión válida a la cola
          ReturnConnectionToQueue(Conn);
          Inc(ValidatedCount);
          TInterlocked.Increment(FTotalValidatedOK);
        end
        else
        begin
          // Desechar conexión inválida
          Inc(InvalidatedCount);
          TInterlocked.Increment(FTotalFailedValidations);
          TInterlocked.Decrement(FCurrentSize);
          LogMessage(Format('Pool "%s": Removing connection %s due to failed validation.', [FConfig.Name, Conn.ID]), logWarning);

          try
            FreeAndNil(Conn);
          except
            on E: Exception do
              LogMessage(Format('Pool "%s": Error freeing invalid connection %s: %s', [FConfig.Name, Conn.ID, E.Message]), logError);
          end;
        end;
      except
        on E: Exception do
        begin
          // Si falla la validación por excepción, marcar como inválida
          Inc(InvalidatedCount);
          TInterlocked.Increment(FTotalFailedValidations);
          TInterlocked.Decrement(FCurrentSize);
          LogMessage(Format('Pool "%s": Connection %s validation failed with exception: %s. Removing.', [FConfig.Name, Conn.ID, E.Message]), logError);

          try
            FreeAndNil(Conn);
          except
            on E2: Exception do
              LogMessage(Format('Pool "%s": Error freeing connection after validation exception: %s', [FConfig.Name, E2.Message]), logError);
          end;
        end;
      end;
    end;
  end;

  // Asegurar tamaño mínimo si el pooling está habilitado
  if FConfig.PoolingEnabled then
  begin
    try
      EnsureMinPoolSize;
    except
      on E: Exception do
        LogMessage(Format('Pool "%s": Error in EnsureMinPoolSize after validation: %s', [FConfig.Name, E.Message]), logError);
    end;
  end;

  LogMessage(Format('Pool "%s": Validation complete. Valid: %d, Invalid: %d', [FConfig.Name, ValidatedCount, InvalidatedCount]), logInfo);
  LogPoolStatus('After ValidateAllIdleConnections');
end;

procedure TSingleDBConnectionPool.TrimIdleConnections;
var
  NumToRemoveTarget, ActualRemovedCount: Integer;
  IdleConnList: TArray<TPooledDBConnection>;
  Conn: TPooledDBConnection;
  i, ValidCount: Integer;
begin
  if not FConfig.PoolingEnabled or FShuttingDown then Exit;

  LogMessage(Format('Pool "%s": Trimming idle connections. Current: %d, Idle: %d, MinSize: %d, Active: %d',
    [FConfig.Name, FCurrentSize, GetIdleConnectionsCount, FConfig.MinPoolSize, FActiveConnectionCount]), logInfo);

  if FCurrentSize <= FConfig.MinPoolSize then
  begin
    LogMessage(Format('Pool "%s": Current size (%d) <= MinPoolSize (%d). No trimming needed.', [FConfig.Name, FCurrentSize, FConfig.MinPoolSize]), logDebug);
    Exit;
  end;

  NumToRemoveTarget := FCurrentSize - FConfig.MinPoolSize;
  if NumToRemoveTarget <= 0 then Exit;

  // Extraer conexiones idle para análisis
  SetLength(IdleConnList, GetIdleConnectionsCount);
  ValidCount := 0;

  while (ValidCount < Length(IdleConnList)) and (ValidCount < NumToRemoveTarget) do
  begin
    if not FIdleQueue.TryDequeue(Conn) then Break;
    IdleConnList[ValidCount] := Conn;
    Inc(ValidCount);
  end;

  SetLength(IdleConnList, ValidCount);

  // Ordenar por LastUsedTime (más antiguas primero)
  TArray.Sort<TPooledDBConnection>(IdleConnList, TComparer<TPooledDBConnection>.Construct(
    function(const L, R: TPooledDBConnection): Integer
    begin
      if L.LastUsedTime < R.LastUsedTime then Result := -1
      else if L.LastUsedTime > R.LastUsedTime then Result := 1
      else Result := 0;
    end));

  ActualRemovedCount := 0;
  for i := 0 to High(IdleConnList) do
  begin
    Conn := IdleConnList[i];
    if Assigned(Conn) then
    begin
      if (ActualRemovedCount < NumToRemoveTarget) and (FCurrentSize > FConfig.MinPoolSize) then
      begin
        LogMessage(Format('Pool "%s": Trimming idle connection %s (LastUsed: %s).',
          [FConfig.Name, Conn.ID, DateTimeToStr(Conn.LastUsedTime)]), logSpam);
        FreeAndNil(Conn);
        TInterlocked.Decrement(FCurrentSize);
        Inc(ActualRemovedCount);
      end
      else
      begin
        // Devolver conexión a la cola
        ReturnConnectionToQueue(Conn);
      end;
    end;
  end;

  LogMessage(Format('Pool "%s": Trimmed %d idle connections.', [FConfig.Name, ActualRemovedCount]), logInfo);
  LogPoolStatus('After TrimIdleConnections');
end;

procedure TSingleDBConnectionPool.CloseAllConnections;
var
  TempActiveList: TList<TPooledDBConnection>;
  Conn: TPooledDBConnection;
  i: Integer;
begin
  LogMessage(Format('Pool "%s": Closing all connections...', [FConfig.Name]), logInfo);
  FShuttingDown := True;

  // Cerrar conexiones activas
  TempActiveList := TList<TPooledDBConnection>.Create;
  try
    var List := FActiveConnections.LockList;
    try
      for i := 0 to List.Count - 1 do
        TempActiveList.Add(List[i]);
      List.Clear;
      TInterlocked.Exchange(FActiveConnectionCount, 0);
    finally
      FActiveConnections.UnlockList;
    end;

    for Conn in TempActiveList do
    begin
      try
        FreeAndNil(Conn);
      except
        on E: Exception do
          LogMessage(Format('Pool "%s": Error destroying active connection: %s', [FConfig.Name, E.Message]), logError);
      end;
    end;
  finally
    TempActiveList.Free;
  end;

  // Cerrar conexiones idle
  if Assigned(FIdleQueue) then
    FIdleQueue.Clear;

  TInterlocked.Exchange(FCurrentSize, 0);
  LogMessage(Format('Pool "%s": All connections closed and pool cleared.', [FConfig.Name]), logInfo);
end;

// En uLib.Database.Pool.pas - Mejorar logging de cleanup
procedure TSingleDBConnectionPool.CleanupIdleConnectionsTimerEvent(Sender: TObject);
var
  StartTime: Cardinal;
  CleanupTime: Cardinal;
  InitialIdleCount, FinalIdleCount: Integer;
  InitialCurrentSize, FinalCurrentSize: Integer;
  CleanupActions: TStringList;
begin
  if not FConfig.PoolingEnabled or
     FShuttingDown or
     not Assigned(FCleanupTimer) then
     Exit;

  FCleanupTimer.Enabled := False;
  StartTime := GetTickCount;
  CleanupActions := TStringList.Create;

  try
    // Capturar estado inicial
    InitialIdleCount := GetIdleConnectionsCount;
    InitialCurrentSize := FCurrentSize;

    LogMessage(Format('Pool "%s": Starting periodic cleanup. Initial state - Size: %d, Active: %d, Idle: %d',
      [FConfig.Name, InitialCurrentSize, FActiveConnectionCount, InitialIdleCount]), logDebug);

    // Solo hacer cleanup si hay conexiones idle que puedan ser removidas
    if (FCurrentSize > FConfig.MinPoolSize) and (GetIdleConnectionsCount > 0) then
    begin
      try
        CleanupActions.Add('TrimIdleConnections');
        TrimIdleConnections;
      except
        on E: Exception do
        begin
          CleanupActions.Add(Format('TrimIdleConnections_ERROR: %s', [E.Message]));
          LogMessage(Format('Pool "%s": Error during TrimIdleConnections: %s', [FConfig.Name, E.Message]), logError);
        end;
      end;
    end
    else
    begin
      CleanupActions.Add('SkippedTrim_NoExcessConnections');
    end;

    // Asegurar tamaño mínimo si el pooling está habilitado
    if FConfig.PoolingEnabled and (FCurrentSize < FConfig.MinPoolSize) then
    begin
      try
        CleanupActions.Add('EnsureMinPoolSize');
        EnsureMinPoolSize;
      except
        on E: Exception do
        begin
          CleanupActions.Add(Format('EnsureMinPoolSize_ERROR: %s', [E.Message]));
          LogMessage(Format('Pool "%s": Error during EnsureMinPoolSize: %s', [FConfig.Name, E.Message]), logError);
        end;
      end;
    end
    else if FCurrentSize >= FConfig.MinPoolSize then
    begin
      CleanupActions.Add('SkippedEnsureMin_SizeOK');
    end;

    // Capturar estado final
    FinalIdleCount := GetIdleConnectionsCount;
    FinalCurrentSize := FCurrentSize;
    CleanupTime := GetTickCount - StartTime;

    // Logging basado en resultados
    if CleanupTime > MAX_CLEANUP_TIME_MS then
    begin
      LogMessage(Format('Pool "%s": Cleanup took %dms (max allowed: %dms). Actions: [%s]',
        [FConfig.Name, CleanupTime, MAX_CLEANUP_TIME_MS, CleanupActions.CommaText]), logWarning);
    end
    else if CleanupTime > 5000 then
    begin
      LogMessage(Format('Pool "%s": Cleanup took %dms. Actions: [%s]',
        [FConfig.Name, CleanupTime, CleanupActions.CommaText]), logDebug);
    end;

    // Log cambios significativos
    if (FinalCurrentSize <> InitialCurrentSize) or (FinalIdleCount <> InitialIdleCount) then
    begin
      LogMessage(Format('Pool "%s": Cleanup completed in %dms. Size: %d->%d, Idle: %d->%d. Actions: [%s]',
        [FConfig.Name, CleanupTime, InitialCurrentSize, FinalCurrentSize,
         InitialIdleCount, FinalIdleCount, CleanupActions.CommaText]), logInfo);
    end;

    // Verificar problemas potenciales
    if FinalCurrentSize > FConfig.MaxPoolSize then
    begin
      LogMessage(Format('Pool "%s": WARNING - Pool size (%d) exceeds maximum (%d) after cleanup',
        [FConfig.Name, FinalCurrentSize, FConfig.MaxPoolSize]), logWarning);
    end;

    if (FActiveConnectionCount + FinalIdleCount) <> FinalCurrentSize then
    begin
      LogMessage(Format('Pool "%s": WARNING - Connection count mismatch after cleanup. Total: %d, Active: %d, Idle: %d',
        [FConfig.Name, FinalCurrentSize, FActiveConnectionCount, FinalIdleCount]), logWarning);
    end;

  except
    on E: Exception do
    begin
      CleanupTime := GetTickCount - StartTime;
      LogMessage(Format('Pool "%s": CRITICAL error in cleanup timer after %dms: %s. Actions attempted: [%s]',
        [FConfig.Name, CleanupTime, E.Message, CleanupActions.CommaText]), logError);
    end;
  end;

  // Reactivar el timer si no se está cerrando
  if Assigned(FCleanupTimer) and not FShuttingDown then
    FCleanupTimer.Enabled := True;

  CleanupActions.Free;
end;

procedure TSingleDBConnectionPool.LogPoolActivity(const AMessage: string; ALevel: TLogLevel = logDebug);
begin
  // Solo log actividad del pool si:
  // 1. Hay problemas (warning/error)
  // 2. El pool está bajo presión (>80% de capacidad)
  // 3. Hay waiters
  // 4. Es información importante (info level)

  if (ALevel >= logWarning) or
     (ALevel = logInfo) or
     (TInterlocked.CompareExchange(FWaitCount,0,0)>0) or
     (FActiveConnectionCount > (FConfig.MaxPoolSize * 0.8)) then
  begin
    LogMessage(AMessage, ALevel);
  end;
end;

procedure TSingleDBConnectionPool.LogPoolStatus(const AReason: string);
var
  AvgWait: Double;
  // Snapshot local para atomicidad
  LocalCurrentSize: Integer;
  LocalActiveConnectionCount: Integer;
  LocalTotalCreated: Int64;
  LocalTotalAcquired: Int64;
  LocalTotalReleased: Int64;
  LocalTotalValidatedOK: Int64;
  LocalTotalFailedCreations: Int64;
  LocalTotalFailedValidations: Int64;
  LocalWaitCount: Integer;
  LocalWaitTimeAccumulatedMs: Int64;
  LocalIdleCount: Integer;
begin
  if not Assigned(FMetricsLock) then
  begin
    LogMessage(Format('Pool "%s" Status (%s): Metrics lock not available', [FConfig.Name, AReason]), logWarning);
    Exit;
  end;

  // Tomar snapshot atómico
  FMetricsLock.Acquire;
  try
    LocalCurrentSize := FCurrentSize;
    LocalActiveConnectionCount := FActiveConnectionCount;
    LocalTotalCreated := FTotalCreated;
    LocalTotalAcquired := FTotalAcquired;
    LocalTotalReleased := FTotalReleased;
    LocalTotalValidatedOK := FTotalValidatedOK;
    LocalTotalFailedCreations := FTotalFailedCreations;
    LocalTotalFailedValidations := FTotalFailedValidations;
    LocalWaitCount := FWaitCount;
    LocalWaitTimeAccumulatedMs := FWaitTimeAccumulatedMs;
  finally
    FMetricsLock.Release;
  end;

  // Calcular valores derivados fuera del lock
  LocalIdleCount := GetIdleConnectionsCount;
  AvgWait := 0;
  if LocalTotalAcquired > 0 then
    AvgWait := LocalWaitTimeAccumulatedMs / LocalTotalAcquired;

  // Solo log status detallado si hay actividad significativa o problemas
  if (LocalWaitCount > 0) or
     (LocalTotalFailedCreations > 0) or
     (LocalTotalFailedValidations > 0) or
     (LocalActiveConnectionCount > (FConfig.MaxPoolSize * 0.7)) then
  begin
    LogMessage(Format('Pool "%s" Status (%s): Size=%d (Active=%d, Idle=%d), Waiters=%d, FailedCreate=%d, FailedValidate=%d, AvgWait=%.1fms',
      [FConfig.Name, AReason, LocalCurrentSize, LocalActiveConnectionCount, LocalIdleCount,
       LocalWaitCount, LocalTotalFailedCreations, LocalTotalFailedValidations, AvgWait]), logDebug);
  end;
end;

procedure TSingleDBConnectionPool.UpdateMetricsAtomic(AOperation: TProc);
begin
  if not Assigned(FMetricsLock) or not Assigned(AOperation) then Exit;

  FMetricsLock.Acquire;
  try
    AOperation();
  finally
    FMetricsLock.Release;
  end;
end;

function TSingleDBConnectionPool.GetPoolStats: TJSONObject;
var
  AvgWait: Double;
  // Snapshot local de métricas para atomicidad
  LocalCurrentSize: Integer;
  LocalActiveConnectionCount: Integer;
  LocalTotalCreated: Int64;
  LocalTotalAcquired: Int64;
  LocalTotalReleased: Int64;
  LocalTotalValidatedOK: Int64;
  LocalTotalFailedCreations: Int64;
  LocalTotalFailedValidations: Int64;
  LocalWaitCount: Integer;
  LocalWaitTimeAccumulatedMs: Int64;
  LocalIdleCount: Integer;
begin
  Result := TJSONObject.Create;

  if not Assigned(FMetricsLock) then
  begin
    // Fallback sin métricas si no hay lock
    Result.AddPair('pool_name', FConfig.Name);
    Result.AddPair('error', 'Metrics lock not available');
    Exit;
  end;

  // Tomar snapshot atómico de todas las métricas
  FMetricsLock.Acquire;
  try
    LocalCurrentSize := FCurrentSize;
    LocalActiveConnectionCount := FActiveConnectionCount;
    LocalTotalCreated := FTotalCreated;
    LocalTotalAcquired := FTotalAcquired;
    LocalTotalReleased := FTotalReleased;
    LocalTotalValidatedOK := FTotalValidatedOK;
    LocalTotalFailedCreations := FTotalFailedCreations;
    LocalTotalFailedValidations := FTotalFailedValidations;
    LocalWaitCount := FWaitCount;
    LocalWaitTimeAccumulatedMs := FWaitTimeAccumulatedMs;
  finally
    FMetricsLock.Release;
  end;

  // Obtener idle count de forma thread-safe
  LocalIdleCount := GetIdleConnectionsCount;

  // Calcular promedio fuera del lock
  AvgWait := 0;
  if LocalTotalAcquired > 0 then
    AvgWait := LocalWaitTimeAccumulatedMs / LocalTotalAcquired;

  // Construir JSON con datos locales
  try
    Result.AddPair('pool_name', FConfig.Name);
    Result.AddPair('db_type', TRttiEnumerationType.GetName<TDBType>(FConfig.DBType));
    Result.AddPair('min_size', FConfig.MinPoolSize);
    Result.AddPair('max_size', FConfig.MaxPoolSize);
    Result.AddPair('current_size', LocalCurrentSize);
    Result.AddPair('active_connections', LocalActiveConnectionCount);
    Result.AddPair('idle_connections', LocalIdleCount);
    Result.AddPair('total_created_physical', LocalTotalCreated);
    Result.AddPair('total_acquired_from_pool', LocalTotalAcquired);
    Result.AddPair('total_released_to_pool', LocalTotalReleased);
    Result.AddPair('total_validated_ok', LocalTotalValidatedOK);
    Result.AddPair('total_failed_creations', LocalTotalFailedCreations);
    Result.AddPair('total_failed_validations', LocalTotalFailedValidations);
    Result.AddPair('waiting_requests_now', LocalWaitCount);
    Result.AddPair('avg_acquire_wait_time_ms', AvgWait);
    Result.AddPair('pooling_enabled', FConfig.PoolingEnabled);
    Result.AddPair('is_shutting_down', FShuttingDown);
  except
    on E: Exception do
    begin
      LogMessage(Format('Pool "%s": Error building stats JSON: %s', [FConfig.Name, E.Message]), logError);
      FreeAndNil(Result);
      Result := TJSONObject.Create;
      Result.AddPair('pool_name', FConfig.Name);
      Result.AddPair('error', 'Failed to generate stats: ' + E.Message);
    end;
  end;
end;

{ TDBConnectionPoolManager }

class constructor TDBConnectionPoolManager.CreateClassLock;
begin
  if not Assigned(FSingletonLock) then
    FSingletonLock := TCriticalSection.Create;
  FInstanceDestroyed := False;
end;

class destructor TDBConnectionPoolManager.DestroyClassLock;
begin
  if Assigned(FSingletonLock) then
  begin
    FSingletonLock.Acquire;
    try
      FInstanceDestroyed := True; // ← AGREGAR ESTA LÍNEA
      FreeAndNil(FInstance);
    finally
      FSingletonLock.Release;
    end;
  end;
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
  ShutdownAllPools;
  FreeAndNil(FPools);
  FreeAndNil(FManagerLock);
  LogMessage('TDBConnectionPoolManager destroyed.', logInfo);
  inherited;
end;

class procedure TDBConnectionPoolManager.DestroyInstance;
begin
  if Assigned(FSingletonLock) then
  begin
    FSingletonLock.Acquire;
    try
      FInstanceDestroyed := True;
      FreeAndNil(FInstance);
    finally
      FSingletonLock.Release;
    end;
  end;
end;

class function TDBConnectionPoolManager.GetInstance: TDBConnectionPoolManager;
begin
  // Verificar si la instancia fue destruida
  if FInstanceDestroyed then
  begin
    raise EDBPoolError.Create('TDBConnectionPoolManager instance has been destroyed and cannot be recreated.');
  end;

  // Double-checked locking pattern correcto
  if not Assigned(FInstance) then
  begin
    if not Assigned(FSingletonLock) then
      raise EDBPoolError.Create('TDBConnectionPoolManager SingletonLock not initialized.');

    FSingletonLock.Acquire;
    try
      if FInstanceDestroyed then
         raise EDBPoolError.Create('TDBConnectionPoolManager instance has been destroyed and cannot be recreated.');

      // Verificar nuevamente dentro del lock
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

    ShutdownAllPools;

    for i := 0 to AConfigArray.Count - 1 do
    begin
      if AConfigArray.Items[i] is TJSONObject then
      begin
        PoolConfigJSON := AConfigArray.Items[i] as TJSONObject;
        try
          DBConfig.LoadFromJSON(PoolConfigJSON);

          if FPools.ContainsKey(DBConfig.Name) then
            LogMessage(Format('Pool "%s" seems to exist after ShutdownAllPools. This is unexpected. Overwriting.', [DBConfig.Name]), logWarning);

          LogMessage(Format('Creating connection pool "%s" for DBType: %s, Server: %s, DB: %s',
            [DBConfig.Name, TRttiEnumerationType.GetName<TDBType>(DBConfig.DBType),
             DBConfig.Server, DBConfig.Database]), logInfo);

          var Pool := TSingleDBConnectionPool.Create(DBConfig, Self.FMonitor);
          FPools.Add(DBConfig.Name, Pool);
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
      FPools.Remove(AConfig.Name);
    end;

    Pool := TSingleDBConnectionPool.Create(AConfig, Self.FMonitor);
    FPools.Add(AConfig.Name, Pool);
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
  FManagerLock.Acquire;
  try
    if not FPools.TryGetValue(APoolName, Pool) then
      raise EDBPoolError.CreateFmt('Connection pool "%s" not found.', [APoolName]);
  finally
    FManagerLock.Release;
  end;
  Result := Pool.AcquireConnection(AAcquireTimeoutMs);
end;

procedure TDBConnectionPoolManager.ReleaseConnection(ADBConnection: IDBConnection; const APoolNameHint: string = '');
var
  Pool: TSingleDBConnectionPool;
  ActualPoolName: string;
  LBaseConn: TBaseConnection;
begin
  if not Assigned(ADBConnection) then Exit;

  ActualPoolName := APoolNameHint;

  if ActualPoolName.IsEmpty then
  begin
    if (ADBConnection is TBaseConnection) then
    begin
      LBaseConn := ADBConnection as TBaseConnection;
      ActualPoolName := LBaseConn.ConnectionConfigName;
    end;
  end;

  if ActualPoolName <> '' then
  begin
    FManagerLock.Acquire;
    try
      if FPools.TryGetValue(ActualPoolName, Pool) then
       begin
        // Pool found, release manager lock before calling pool's method
       end
      else
       Pool := nil;
    finally
      FManagerLock.Release;
    end;
    if not Assigned(Pool) and (ADBConnection is TBaseConnection) then
    begin
      LogMessage(Format('ReleaseConnection: Connection for config "%s" (pooling disabled or not found in pools) is being released directly.',
        [(ADBConnection as TBaseConnection).ConnectionConfigName]), logDebug);
      try
        if ADBConnection.IsConnected then
          ADBConnection.Disconnect;
      except
        on E: Exception do begin
          LogMessage(Format('Error disconnecting direct connection: %s', [E.Message]), logError);
        end;
      end;
      // La interfaz se liberará por reference counting, lo que debería destruir el objeto TBaseConnection.
      // No se necesita FreeAndNil explícito del objeto si la interfaz es la única referencia.
      Exit; // Salir después de manejar la conexión directa
    end
    else if not Assigned(Pool) then
    begin
       LogMessage(Format('ReleaseConnection: Could not find pool for connection and it is not a recognized TBaseConnection for direct release. Hint: %s', [APoolNameHint]), logError);
    end;

    if Assigned(Pool) then
    begin
      Pool.ReleaseConnection(ADBConnection);
      Exit;
    end
    else if APoolNameHint <> '' then
      LogMessage(Format('ReleaseConnection: Hinted pool "%s" not found. Will try iterating all pools.', [APoolNameHint]), logWarning);
  end;

  // If no hint, or hinted pool not found: iterate all pools (less efficient)
  LogMessage('ReleaseConnection: No specific pool identified. Iterating all pools to find owner (this is inefficient).', logWarning);
  FManagerLock.Acquire;
  try
    for var PairValue in FPools.Values do
    begin
      PairValue.ReleaseConnection(ADBConnection);
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
  Result := Pool.GetPoolStats;
end;

function TDBConnectionPoolManager.GetAllPoolsStats: TJSONArray;
var
  PoolListCopy: TList<TSingleDBConnectionPool>;
  Pool: TSingleDBConnectionPool;
begin
  Result := TJSONArray.Create;
  PoolListCopy := TList<TSingleDBConnectionPool>.Create;
  try
    FManagerLock.Acquire;
    try
      for Pool in FPools.Values do
        PoolListCopy.Add(Pool);
    finally
      FManagerLock.Release;
    end;

    for Pool in PoolListCopy do
      Result.Add(Pool.GetPoolStats);
  finally
    PoolListCopy.Free;
  end;
end;

procedure TDBConnectionPoolManager.ShutdownAllPools;
begin
  LogMessage('DBConnectionPoolManager: Shutting down all connection pools...', logInfo);
  FManagerLock.Acquire;
  try
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
finalization
  TDBConnectionPoolManager.DestroyInstance;
end.
