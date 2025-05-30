unit uLib.Database.Types;

interface

uses
  System.SysUtils, System.Classes, Data.DB, System.JSON, System.Rtti,
  FireDAC.Stan.Param;

type
  // Tipos de base de datos soportados
  TDBType = (dbtUnknown, dbtMSSQL, dbtPostgreSQL, dbtMySQL); // dbtUnknown primero es más seguro como default

  // Estado de la conexión
  TConnectionState = (
    csNew,          // Recién creada, no conectada
    csConnecting,
    csConnected,    // Conectada y lista (en el pool podría ser csIdle o csInUse)
    csIdle,         // En el pool, disponible
    csInUse,        // En el pool, prestada
    csValidating,   // Siendo validada antes de entregarla
    csError,        // Error en la conexión
    csClosed,       // Cerrada explícitamente
    csInvalid       // Marcada como inválida, para ser removida del pool
  );

  // Excepciones personalizadas
  EDBException = class(Exception);
  EDBConnectionError = class(EDBException);
  EDBCommandError = class(EDBException);
  EDBPoolError = class(EDBException);
  EDBConfigError = class(EDBException);

  // Configuración de conexión
  TDBConnectionConfig = record
  public
    Name: string; // Nombre/Alias para identificar esta configuración de pool/conexión
    DBType: TDBType;
    Server: string;
    Port: Integer;
    Database: string;
    Schema: string; // Específico para algunas BDs como PostgreSQL
    Username: string;
    Password: string;
    Params: string; // Parámetros adicionales específicos del driver (ej. 'vendorLibWin=libmysql.dll')

    // Timeouts (en segundos)
    ConnectionTimeout: Integer; // Tiempo para establecer la conexión inicial
    CommandTimeout: Integer;    // Tiempo para ejecutar un comando

    // Pooling
    PoolingEnabled: Boolean;
    MinPoolSize: Integer;
    MaxPoolSize: Integer;
    IdleTimeout: Integer;       // Tiempo (segundos) que una conexión puede estar inactiva en el pool antes de cerrarse
    AcquireTimeout: Integer;    // Tiempo (milisegundos) que se espera para obtener una conexión del pool

    // SSL/TLS
    SSL: Boolean;
    SSLCert: string;
    SSLKey: string;
    SSLRootCert: string;

    // Otros
    ApplicationName: string;
    Compress: Boolean;        // Específico para algunos drivers como MySQL
    RetryAttempts: Integer;   // Intentos de reconexión o ejecución de comando
    RetryDelayMs: Integer;    // Retraso entre intentos (milisegundos)

    class function CreateDefault: TDBConnectionConfig; static;
    procedure LoadFromJSON(const AJSONObject: TJSONObject);
    function SaveToJSON: TJSONObject;
    procedure Validate;
  end;

  // Interfaz base para conexiones
  IDBConnection = interface
    ['{F8A92D53-8E47-4E2A-B1C4-6A7D234F9B12}']

    // THREAD-SAFETY: All methods in this interface are thread-safe for concurrent access
    // from multiple threads, except where explicitly noted.

    function GetState: TConnectionState;
    function GetLastError: string;
    function GetNativeConnection: TObject; // THREAD-SAFETY: Not thread-safe - caller must synchronize access

    function Connect: Boolean;
    procedure Disconnect;
    function IsConnected: Boolean;

    // TRANSACTION MANAGEMENT: These methods maintain internal transaction nesting count
    // and are thread-safe for the same connection instance.
    function InTransaction: Boolean;
    procedure StartTransaction;
    procedure Commit;
    procedure Rollback;

    // SQL EXECUTION METHODS
    // THREAD-SAFETY: Safe for concurrent execution on same connection
    // PERFORMANCE: Concurrent executions will be serialized internally
    function Execute(const SQL: string; Params: TFDParams=Nil): Integer;
    function ExecuteScalar(const SQL: string; Params: TFDParams=Nil): Variant;

    // OWNERSHIP: Caller takes ownership of returned TDataSet and MUST free it
    // THREAD-SAFETY: Returned TDataSet is NOT thread-safe - use from single thread only
    // USAGE: Always wrap in try-finally block to ensure proper cleanup
    function ExecuteReader(const SQL: string; Params: TFDParams=Nil): TDataSet;

    // OWNERSHIP: Returns string copy, caller owns the string
    function ExecuteJSON(const SQL: string; Params: TFDParams=Nil): string;

    // OWNERSHIP: Caller takes ownership of returned TStrings and MUST free it
    function GetTables: TStrings;
    function GetFields(const TableName: string): TStrings;

    function GetVersion: string;

    // THREAD-SAFETY: Safe to call concurrently, affects all subsequent operations
    procedure SetQueryTimeout(const AValue: Integer); // Seconds
    function GetQueryTimeout: Integer; // Seconds

    property State: TConnectionState read GetState;
    property LastError: string read GetLastError;
    property QueryTimeout: Integer read GetQueryTimeout write SetQueryTimeout;  end;

  // Interfaz para monitoreo
  IDBMonitor = interface
    ['{2A4B6C8D-0E2F-4A6B-8C0D-2E4F6A8B0C2D}']
    procedure TrackCommand(const SQL: string; ExecutionTimeMs: Int64; const PoolName: string = '');
    procedure TrackError(const Error: Exception; const SQL: string = ''; const PoolName: string = '');
    procedure TrackConnectionStateChange(const ConnectionID: string; NewState: TConnectionState; const PoolName: string = '');
    procedure TrackPoolActivity(const PoolName: string; ActiveConnections, IdleConnections, WaitingRequests: Integer);
    function GetStats(const PoolName: string = ''): string; // Stats para un pool específico o todos
  end;

function CreateDefaultDBConnectionConfig: TDBConnectionConfig;
function GetDatabaseConstants(out ValidationIntervalSec, SlowOperationThresholdMs, MaxCleanupTimeMs, MaxConnectionTimeoutSec, DefaultValidationTimeoutSec: Integer): Boolean;

implementation

uses
   System.Math,

   uLib.Utils,
   uLib.Logger,
   uLib.Config.Manager;

{ TDBConnectionConfig }

class function TDBConnectionConfig.CreateDefault: TDBConnectionConfig;
begin
  Result.Name := 'DefaultPool';
  Result.DBType := dbtUnknown;
  Result.Server := 'localhost';
  Result.Port := 0; // Se establecerá por defecto en Validate o según DBType
  Result.Database := '';
  Result.Schema := '';
  Result.Username := '';
  Result.Password := '';
  Result.Params := '';
  Result.ConnectionTimeout := 30; // segundos
  Result.CommandTimeout := 30;    // segundos
  Result.PoolingEnabled := True;
  Result.MinPoolSize := 1; // Iniciar con al menos 1 si el pooling está habilitado
  Result.MaxPoolSize := 10;
  Result.IdleTimeout := 300;      // 5 minutos
  Result.AcquireTimeout := 15000; // 15 segundos
  Result.SSL := False;
  Result.SSLCert := '';
  Result.SSLKey := '';
  Result.SSLRootCert := '';
  Result.ApplicationName := 'DelphiAppServer';
  Result.Compress := False;
  Result.RetryAttempts := 0;
  Result.RetryDelayMs := 1000;
end;

procedure TDBConnectionConfig.LoadFromJSON(const AJSONObject: TJSONObject);
var
  DBTypeStr: string;
begin
  // Empezar con valores por defecto
  Self := CreateDefault;

  if not Assigned(AJSONObject) then
    raise EDBConfigError.Create('Cannot load DB configuration from a nil JSON object.');

  Self.Name            := TJSONHelper.GetString(AJSONObject, 'name', Self.Name);
  DBTypeStr            := TJSONHelper.GetString(AJSONObject, 'dbType', TRttiEnumerationType.GetName<TDBType>(Self.DBType)).ToUpper; // Convert to uppercase for case-insensitive comparison
  Self.Server          := TJSONHelper.GetString(AJSONObject, 'server', Self.Server);
  Self.Port            := TJSONHelper.GetInteger(AJSONObject, 'port', Self.Port);
  Self.Database        := TJSONHelper.GetString(AJSONObject, 'database', Self.Database);
  Self.Schema          := TJSONHelper.GetString(AJSONObject, 'schema', Self.Schema);
  Self.Username        := TJSONHelper.GetString(AJSONObject, 'username', Self.Username);
  Self.Password        := TJSONHelper.GetString(AJSONObject, 'password', Self.Password); // Considerar cargar desde variables de entorno o vault
  Self.Params          := TJSONHelper.GetString(AJSONObject, 'params', Self.Params);

  Self.ConnectionTimeout := TJSONHelper.GetInteger(AJSONObject, 'connectionTimeout', Self.ConnectionTimeout);
  Self.CommandTimeout    := TJSONHelper.GetInteger(AJSONObject, 'commandTimeout', Self.CommandTimeout);

  Self.PoolingEnabled    := TJSONHelper.GetBoolean(AJSONObject, 'poolingEnabled', Self.PoolingEnabled);
  Self.MinPoolSize       := TJSONHelper.GetInteger(AJSONObject, 'minPoolSize', Self.MinPoolSize);
  Self.MaxPoolSize       := TJSONHelper.GetInteger(AJSONObject, 'maxPoolSize', Self.MaxPoolSize);
  Self.IdleTimeout       := TJSONHelper.GetInteger(AJSONObject, 'idleTimeout', Self.IdleTimeout);
  Self.AcquireTimeout    := TJSONHelper.GetInteger(AJSONObject, 'acquireTimeout', Self.AcquireTimeout);

  Self.SSL               := TJSONHelper.GetBoolean(AJSONObject, 'ssl', Self.SSL);
  Self.SSLCert           := TJSONHelper.GetString(AJSONObject, 'sslCert', Self.SSLCert);
  Self.SSLKey            := TJSONHelper.GetString(AJSONObject, 'sslKey', Self.SSLKey);
  Self.SSLRootCert       := TJSONHelper.GetString(AJSONObject, 'sslRootCert', Self.SSLRootCert);

  Self.ApplicationName   := TJSONHelper.GetString(AJSONObject, 'applicationName', Self.ApplicationName);
  Self.Compress          := TJSONHelper.GetBoolean(AJSONObject, 'compress', Self.Compress);
  Self.RetryAttempts     := TJSONHelper.GetInteger(AJSONObject, 'retryAttempts', Self.RetryAttempts);
  Self.RetryDelayMs      := TJSONHelper.GetInteger(AJSONObject, 'retryDelayMs', Self.RetryDelayMs);

  // Convertir string de DBType a enum
  if DBTypeStr = 'MSSQL' then
    Self.DBType := dbtMSSQL
  else if DBTypeStr = 'POSTGRESQL' then
    Self.DBType := dbtPostgreSQL
  else if DBTypeStr = 'MYSQL' then
    Self.DBType := dbtMySQL
  else
    Self.DBType := dbtUnknown; // Default si no coincide

  Validate; // Validar después de cargar
end;

function TDBConnectionConfig.SaveToJSON: TJSONObject;
begin
  Result := TJSONObject.Create;
  Result.AddPair('name', TJSONString.Create(Name));
  Result.AddPair('dbType', TJSONString.Create(TRttiEnumerationType.GetName<TDBType>(DBType)));
  Result.AddPair('server', TJSONString.Create(Server));
  Result.AddPair('port', TJSONNumber.Create(Port));
  Result.AddPair('database', TJSONString.Create(Database));
  Result.AddPair('schema', TJSONString.Create(Schema));
  Result.AddPair('username', TJSONString.Create(Username));
  Result.AddPair('params', TJSONString.Create(Params));
  // No guardar la contraseña en el JSON por seguridad.
  // Result.AddPair('password', TJSONString.Create(Password));

  Result.AddPair('connectionTimeout', TJSONNumber.Create(ConnectionTimeout));
  Result.AddPair('commandTimeout', TJSONNumber.Create(CommandTimeout));

  Result.AddPair('poolingEnabled', TJSONBool.Create(PoolingEnabled));
  Result.AddPair('minPoolSize', TJSONNumber.Create(MinPoolSize));
  Result.AddPair('maxPoolSize', TJSONNumber.Create(MaxPoolSize));
  Result.AddPair('idleTimeout', TJSONNumber.Create(IdleTimeout));
  Result.AddPair('acquireTimeout', TJSONNumber.Create(AcquireTimeout));

  Result.AddPair('ssl', TJSONBool.Create(SSL));
  if SSL then
  begin
    Result.AddPair('sslCert', TJSONString.Create(SSLCert));
    Result.AddPair('sslKey', TJSONString.Create(SSLKey));
    Result.AddPair('sslRootCert', TJSONString.Create(SSLRootCert));
  end;

  Result.AddPair('applicationName', TJSONString.Create(ApplicationName));
  Result.AddPair('compress', TJSONBool.Create(Compress));
  Result.AddPair('retryAttempts', TJSONNumber.Create(RetryAttempts));
  Result.AddPair('retryDelayMs', TJSONNumber.Create(RetryDelayMs));
end;

procedure TDBConnectionConfig.Validate;
var
  ConfigMgr: TConfigManager;
  IsProduction: Boolean;
  MinAllowedPoolSize, MaxAllowedPoolSize: Integer;
begin
  if Name.Trim.IsEmpty then
    Name := 'Pool_' + FormatDateTime('yyyymmddhhnnsszzz', Now);

  if Server.Trim.IsEmpty then
    raise EDBConfigError.CreateFmt('Database configuration "%s": Server is required.', [Name]);

  if Database.Trim.IsEmpty then
    raise EDBConfigError.CreateFmt('Database configuration "%s": Database is required.', [Name]);

  // Detectar ambiente de producción
  IsProduction := SameText(GetEnvironmentVariable('ENVIRONMENT'), 'PRODUCTION') or
                 SameText(GetEnvironmentVariable('APP_ENV'), 'PROD');

  // Obtener límites configurables de pool
  try
    ConfigMgr := TConfigManager.GetInstance;
    if Assigned(ConfigMgr) and Assigned(ConfigMgr.ConfigData) then
    begin
      MinAllowedPoolSize := TJSONHelper.GetInteger(ConfigMgr.ConfigData, 'database.validation.minAllowedPoolSize', 1);
      MaxAllowedPoolSize := TJSONHelper.GetInteger(ConfigMgr.ConfigData, 'database.validation.maxAllowedPoolSize', 100);

      // En producción, aplicar límites más estrictos
      if IsProduction then
      begin
        MinAllowedPoolSize := Max(MinAllowedPoolSize, TJSONHelper.GetInteger(ConfigMgr.ConfigData, 'database.validation.production.minPoolSize', 2));
        MaxAllowedPoolSize := Min(MaxAllowedPoolSize, TJSONHelper.GetInteger(ConfigMgr.ConfigData, 'database.validation.production.maxPoolSize', 50));
      end;
    end
    else
    begin
      MinAllowedPoolSize := IfThen(IsProduction, 2, 1);
      MaxAllowedPoolSize := IfThen(IsProduction, 50, 100);
    end;
  except
    on E: Exception do
    begin
      LogMessage(Format('Error reading pool validation config: %s. Using defaults.', [E.Message]), logWarning);
      MinAllowedPoolSize := IfThen(IsProduction, 2, 1);
      MaxAllowedPoolSize := IfThen(IsProduction, 50, 100);
    end;
  end;

  if Port = 0 then
  begin
    case DBType of
      dbtMSSQL: Port := 1433;
      dbtPostgreSQL: Port := 5432;
      dbtMySQL: Port := 3306;
    else
      if DBType <> dbtUnknown then
        raise EDBConfigError.CreateFmt('Database configuration "%s": Port is 0 and DBType ("%s") is known. Please specify a port or use dbtUnknown if port is not applicable for this type.',
          [Name, TRttiEnumerationType.GetName<TDBType>(DBType)]);
    end;
  end;

  // Validación mejorada de pooling
  if PoolingEnabled then
  begin
    // MinPoolSize debe ser al menos 1 cuando pooling está habilitado
    if MinPoolSize < MinAllowedPoolSize then
    begin
      var OldValue := MinPoolSize;
      MinPoolSize := MinAllowedPoolSize;
      LogMessage(Format('Pool "%s": MinPoolSize adjusted from %d to %d (minimum required when pooling enabled)',
        [Name, OldValue, MinPoolSize]), logWarning);
    end;

    if MaxPoolSize < MinAllowedPoolSize then
    begin
      var OldValue := MaxPoolSize;
      MaxPoolSize := MinAllowedPoolSize;
      LogMessage(Format('Pool "%s": MaxPoolSize adjusted from %d to %d (minimum required)',
        [Name, OldValue, MaxPoolSize]), logWarning);
    end;

    if MaxPoolSize > MaxAllowedPoolSize then
    begin
      var OldValue := MaxPoolSize;
      MaxPoolSize := MaxAllowedPoolSize;
      LogMessage(Format('Pool "%s": MaxPoolSize adjusted from %d to %d (maximum allowed)',
        [Name, OldValue, MaxPoolSize]), logWarning);
    end;

    if MinPoolSize > MaxPoolSize then
    begin
      var OldMinValue := MinPoolSize;
      MinPoolSize := MaxPoolSize;
      LogMessage(Format('Pool "%s": MinPoolSize adjusted from %d to %d (cannot exceed MaxPoolSize)',
        [Name, OldMinValue, MinPoolSize]), logWarning);
    end;

    // Validación específica para producción
    if IsProduction then
    begin
      if MinPoolSize < 2 then
      begin
        MinPoolSize := 2;
        LogMessage(Format('Pool "%s": MinPoolSize set to 2 (production requirement)', [Name]), logWarning);
      end;

      if MaxPoolSize < MinPoolSize * 2 then
      begin
        var OldValue := MaxPoolSize;
        MaxPoolSize := MinPoolSize * 2;
        LogMessage(Format('Pool "%s": MaxPoolSize adjusted from %d to %d (production best practice: at least 2x MinPoolSize)',
          [Name, OldValue, MaxPoolSize]), logWarning);
      end;
    end;

    if IdleTimeout <= 0 then IdleTimeout := 300; // Default 5 minutos si es inválido
    if AcquireTimeout <= 0 then AcquireTimeout := 15000; // Default 15 segundos si es inválido

    // Validar timeouts razonables
    if AcquireTimeout < 1000 then
    begin
      LogMessage(Format('Pool "%s": AcquireTimeout too low (%d ms), setting to 1000ms', [Name, AcquireTimeout]), logWarning);
      AcquireTimeout := 1000;
    end;

    if AcquireTimeout > 300000 then // 5 minutos máximo
    begin
      LogMessage(Format('Pool "%s": AcquireTimeout too high (%d ms), setting to 300000ms', [Name, AcquireTimeout]), logWarning);
      AcquireTimeout := 300000;
    end;

    if IdleTimeout < 60 then // Mínimo 1 minuto
    begin
      LogMessage(Format('Pool "%s": IdleTimeout too low (%d sec), setting to 60sec', [Name, IdleTimeout]), logWarning);
      IdleTimeout := 60;
    end;

  end else
  begin
    // Si el pooling no está habilitado, asegurar valores coherentes
    MinPoolSize := 0;
    MaxPoolSize := 1;
    LogMessage(Format('Pool "%s": Pooling disabled, pool sizes set to Min=0, Max=1', [Name]), logDebug);
  end;

  // Validación de timeouts de conexión
  if ConnectionTimeout <= 0 then ConnectionTimeout := 30; // Default 30s
  if CommandTimeout <= 0 then CommandTimeout := 30;    // Default 30s

  // Validar rangos razonables para timeouts
  if ConnectionTimeout > 300 then // 5 minutos máximo para conexión
  begin
    LogMessage(Format('Pool "%s": ConnectionTimeout too high (%d sec), setting to 300sec', [Name, ConnectionTimeout]), logWarning);
    ConnectionTimeout := 300;
  end;

  if CommandTimeout > 3600 then // 1 hora máximo para comandos
  begin
    LogMessage(Format('Pool "%s": CommandTimeout too high (%d sec), setting to 3600sec', [Name, CommandTimeout]), logWarning);
    CommandTimeout := 3600;
  end;

  // Validación de reintentos
  if RetryAttempts < 0 then RetryAttempts := 0;
  if RetryAttempts > 10 then
  begin
    LogMessage(Format('Pool "%s": RetryAttempts too high (%d), setting to 10', [Name, RetryAttempts]), logWarning);
    RetryAttempts := 10;
  end;

  if RetryDelayMs < 0 then RetryDelayMs := 1000; // Default 1s
  if RetryDelayMs > 60000 then // 1 minuto máximo entre reintentos
  begin
    LogMessage(Format('Pool "%s": RetryDelayMs too high (%d ms), setting to 60000ms', [Name, RetryDelayMs]), logWarning);
    RetryDelayMs := 60000;
  end;

  // Validación de seguridad básica
  if IsProduction then
  begin
    if Username.Trim.IsEmpty then
      raise EDBConfigError.CreateFmt('Pool "%s": Username is required in production environment', [Name]);

    if Password.Trim.IsEmpty then
      LogMessage(Format('Pool "%s": WARNING - Empty password in production environment', [Name]), logCritical);

    if SameText(Username, 'sa') or SameText(Username, 'root') or SameText(Username, 'admin') then
      LogMessage(Format('Pool "%s": WARNING - Using administrative username "%s" in production', [Name, Username]), logWarning);
  end;

  LogMessage(Format('Pool "%s" configuration validated successfully', [Name]), logDebug);
end;

function GetDatabaseConstants(out ValidationIntervalSec, SlowOperationThresholdMs, MaxCleanupTimeMs, MaxConnectionTimeoutSec, DefaultValidationTimeoutSec: Integer): Boolean;
var
  ConfigMgr: TConfigManager;
begin
  // Defaults hardcoded
  ValidationIntervalSec := 300;        // 5 minutos
  SlowOperationThresholdMs := 2000;    // 2 segundos
  MaxCleanupTimeMs := 30000;           // 30 segundos
  MaxConnectionTimeoutSec := 30;       // 30 segundos
  DefaultValidationTimeoutSec := 5;    // 5 segundos
  Result := False;

  try
    ConfigMgr := TConfigManager.GetInstance;
    if Assigned(ConfigMgr) and Assigned(ConfigMgr.ConfigData) then
    begin
      ValidationIntervalSec := TJSONHelper.GetInteger(ConfigMgr.ConfigData, 'database.defaults.validationIntervalSeconds', ValidationIntervalSec);
      SlowOperationThresholdMs := TJSONHelper.GetInteger(ConfigMgr.ConfigData, 'database.defaults.slowOperationThresholdMs', SlowOperationThresholdMs);
      MaxCleanupTimeMs := TJSONHelper.GetInteger(ConfigMgr.ConfigData, 'database.defaults.maxCleanupTimeMs', MaxCleanupTimeMs);
      MaxConnectionTimeoutSec := TJSONHelper.GetInteger(ConfigMgr.ConfigData, 'database.defaults.maxConnectionTimeoutSec', MaxConnectionTimeoutSec);
      DefaultValidationTimeoutSec := TJSONHelper.GetInteger(ConfigMgr.ConfigData, 'database.defaults.defaultValidationTimeoutSec', DefaultValidationTimeoutSec);
      Result := True;
    end;
  except
    // Usar defaults si falla
  end;
end;

function CreateDefaultDBConnectionConfig: TDBConnectionConfig;
var
  ConfigMgr: TConfigManager;
begin
  // Valores por defecto hardcoded como fallback
  Result.Name := '';
  Result.Server := 'localhost';
  Result.Port := 0; // 0 = puerto por defecto del driver
  Result.Database := '';
  Result.Schema := '';
  Result.Username := '';
  Result.Password := '';
  Result.ConnectionTimeout := 15;
  Result.CommandTimeout := 30;
  Result.PoolingEnabled := True;
  Result.MinPoolSize := 2;
  Result.MaxPoolSize := 20;
  Result.IdleTimeout := 300;
  Result.AcquireTimeout := 10000;
  Result.SSL := False;
  Result.ApplicationName := 'DelphiApp';
  Result.Compress := False;
  Result.RetryAttempts := 1;
  Result.RetryDelayMs := 500;
  Result.Params := '';

  // Intentar obtener valores de configuración usando TJSONHelper
  try
    ConfigMgr := TConfigManager.GetInstance;
    if Assigned(ConfigMgr) and Assigned(ConfigMgr.ConfigData) then
    begin
      // Configuración por defecto de base de datos
      Result.ConnectionTimeout := TJSONHelper.GetInteger(ConfigMgr.ConfigData, 'database.defaults.connectionTimeoutSeconds', Result.ConnectionTimeout);
      Result.CommandTimeout := TJSONHelper.GetInteger(ConfigMgr.ConfigData, 'database.defaults.commandTimeoutSeconds', Result.CommandTimeout);
      Result.MinPoolSize := TJSONHelper.GetInteger(ConfigMgr.ConfigData, 'database.defaults.minPoolSize', Result.MinPoolSize);
      Result.MaxPoolSize := TJSONHelper.GetInteger(ConfigMgr.ConfigData, 'database.defaults.maxPoolSize', Result.MaxPoolSize);
      Result.IdleTimeout := TJSONHelper.GetInteger(ConfigMgr.ConfigData, 'database.defaults.idleTimeoutSeconds', Result.IdleTimeout);
      Result.AcquireTimeout := TJSONHelper.GetInteger(ConfigMgr.ConfigData, 'database.defaults.acquireTimeoutMs', Result.AcquireTimeout);

      // Configuración de defaults de environment si están disponibles
      Result.Server := TJSONHelper.GetString(ConfigMgr.ConfigData, 'environment.defaults.dbHost', Result.Server);
      Result.Database := TJSONHelper.GetString(ConfigMgr.ConfigData, 'environment.defaults.dbName', Result.Database);
      Result.Username := TJSONHelper.GetString(ConfigMgr.ConfigData, 'environment.defaults.dbUser', Result.Username);
      Result.Password := TJSONHelper.GetString(ConfigMgr.ConfigData, 'environment.defaults.dbPassword', Result.Password);
    end;
  except
    on E: Exception do
    begin
      // Si falla, usar defaults hardcoded
      try
        LogMessage('Warning: Could not load database defaults from config file, using hardcoded defaults: ' + E.Message, logWarning);
      except
        // Si el logger tampoco está disponible, silenciar
      end;
    end;
  end;
end;

end.

