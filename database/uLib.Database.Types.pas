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
    function GetState: TConnectionState;
    function GetLastError: string;
    function GetNativeConnection: TObject; // Para acceder al TFDConnection subyacente

    function Connect: Boolean;
    procedure Disconnect;
    function IsConnected: Boolean;

    function InTransaction: Boolean;
    procedure StartTransaction;
    procedure Commit;
    procedure Rollback;

    function Execute(const SQL: string; Params: TFDParams=Nil): Integer;
    function ExecuteScalar(const SQL: string; Params: TFDParams=Nil): Variant;
    function ExecuteReader(const SQL: string; Params: TFDParams=Nil): TDataSet; // Caller frees DataSet  <--- COMENTARIO IMPORTANTE A MANTENER/AÑADIR

    function ExecuteJSON(const SQL: string; Params: TFDParams=Nil): string;

    function GetTables: TStrings; // Caller frees TStrings
    function GetFields(const TableName: string): TStrings; // Caller frees TStrings
    function GetVersion: string;

    procedure SetQueryTimeout(const AValue: Integer); // Segundos
    function GetQueryTimeout: Integer; // Segundos

    // Podría ser útil añadir una propiedad para el nombre del pool/configuración
    // function GetPoolName: string;
    // property PoolName: string read GetPoolName;

    property State: TConnectionState read GetState;
    property LastError: string read GetLastError;
    property QueryTimeout: Integer read GetQueryTimeout write SetQueryTimeout;
  end;

  // Interfaz para monitoreo
  IDBMonitor = interface
    ['{2A4B6C8D-0E2F-4A6B-8C0D-2E4F6A8B0C2D}']
    procedure TrackCommand(const SQL: string; ExecutionTimeMs: Int64; const PoolName: string = '');
    procedure TrackError(const Error: Exception; const SQL: string = ''; const PoolName: string = '');
    procedure TrackConnectionStateChange(const ConnectionID: string; NewState: TConnectionState; const PoolName: string = '');
    procedure TrackPoolActivity(const PoolName: string; ActiveConnections, IdleConnections, WaitingRequests: Integer);
    function GetStats(const PoolName: string = ''): string; // Stats para un pool específico o todos
  end;

implementation

uses
   uLib.Utils;

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
begin
  if Name.Trim.IsEmpty then
    Name := 'Pool_' + FormatDateTime('yyyymmddhhnnsszzz', Now);

  if Server.Trim.IsEmpty then
    raise EDBConfigError.CreateFmt('Database configuration "%s": Server is required.', [Name]);

  if Database.Trim.IsEmpty then
    raise EDBConfigError.CreateFmt('Database configuration "%s": Database is required.', [Name]);

  if Port = 0 then
  begin
    case DBType of
      dbtMSSQL: Port := 1433;
      dbtPostgreSQL: Port := 5432;
      dbtMySQL: Port := 3306;
    else
      // Solo lanzar error si el tipo es conocido y el puerto es 0.
      // Si es dbtUnknown y Port es 0, se asume que la cadena de conexión lo manejará o no es aplicable.
      if DBType <> dbtUnknown then
        raise EDBConfigError.CreateFmt('Database configuration "%s": Port is 0 and DBType ("%s") is known. Please specify a port or use dbtUnknown if port is not applicable for this type.',
          [Name, TRttiEnumerationType.GetName<TDBType>(DBType)]);
    end;
  end;

  if PoolingEnabled then
  begin
    if MinPoolSize < 0 then MinPoolSize := 0; // No puede ser negativo
    if MaxPoolSize < 1 then MaxPoolSize := 1; // Debe ser al menos 1
    if MinPoolSize > MaxPoolSize then MinPoolSize := MaxPoolSize; // Min no puede exceder Max

    if IdleTimeout <= 0 then IdleTimeout := 300; // Default 5 minutos si es inválido
    if AcquireTimeout <= 0 then AcquireTimeout := 15000; // Default 15 segundos si es inválido
  end else
  begin
    // Si el pooling no está habilitado, los tamaños del pool no son estrictamente necesarios,
    // pero se pueden establecer a valores predeterminados o ignorar.
    MinPoolSize := 0;
    MaxPoolSize := 1; // O 0, según cómo se manejen las conexiones directas.
  end;

  if ConnectionTimeout <= 0 then ConnectionTimeout := 30; // Default 30s
  if CommandTimeout <= 0 then CommandTimeout := 30;    // Default 30s
  if RetryAttempts < 0 then RetryAttempts := 0;
  if RetryDelayMs < 0 then RetryDelayMs := 1000; // Default 1s
end;

end.

