unit uLib.Database.MSSQL;

interface

uses
  System.Types, System.SysUtils, System.Classes, System.JSON, System.Variants, // Added System.Variants
  Data.DB, FireDAC.Comp.Client, FireDAC.Stan.Def, FireDAC.Stan.Intf,
  FireDAC.Stan.Async, FireDAC.Phys.Intf, FireDAC.DApt, FireDAC.Stan.Option,
  FireDAC.Phys.MSSQL, FireDAC.Phys.MSSQLDef, FireDAC.Stan.Param,

  uLib.Database.Types,
  uLib.Database.Connection,
  uLib.Logger;   // Para QuoteName, IfThen, GetStr, etc.

type
  TMSSQLConnection = class(TBaseConnection)
  private
  const
    MSSQL_QUOTE_CHAR = '[';
  var
    FDBType: Char;
    FDriverLink: TFDPhysMSSQLDriverLink;

    procedure ConfigureDriverLink;
    function IsSafeBackupPath(const ABackupPath: string; out ASafeFullPath: string): Boolean;
  protected
    function GetDriverSpecificConnectionString: string; override;
    procedure ApplyDriverSpecificSettings; override;
    function GetVersion: string; override; // Sobrescribir para obtener info específica de MSSQL
  public
    constructor Create(const AConfig: TDBConnectionConfig; AMonitor: IDBMonitor = nil);
    destructor Destroy; override;

    // Operaciones específicas de SQL Server (ejemplos)
    function GetDatabaseFilesAsJSON: TJSONObject; // Movido a public como ejemplo
    procedure ShrinkDatabase(const ADatabaseName: string = ''; ATargetPercent: Integer = 0); // Añadido ATargetPercent
    procedure RebuildIndexes(const ATableName: string = ''; AWithOnlineOption: Boolean = False); // Añadido AWithOnlineOption
    procedure UpdateStatistics(const ATableName: string = ''; AWithFullScan: Boolean = True); // Añadido AWithFullScan
    procedure CheckDB(const ADatabaseName: string = ''; ARepairOption: string = 'NO_INFOMSGS'); // ARepairOption: NO_INFOMSGS, REPAIR_ALLOW_DATA_LOSS, etc.

    // Diagnóstico
    function GetWaitStatsAsJSON(ATopN: Integer = 15): TJSONObject;
    function GetBlockingProcessesAsJSONArray: TJSONArray;
    function GetQueryStatsAsJSON(ATopN: Integer = 15): TJSONObject; // Requiere permisos VIEW SERVER STATE
    function GetMemoryUsageAsJSON: TJSONObject;

    // Mantenimiento
    procedure KillProcess(ASPID: Integer);
    procedure ClearProcedureCache;
    function GetBackupHistoryAsJSONArray(ADatabaseName: string = ''): TJSONArray;
    procedure BackupDatabase(const ABackupPath: string; ABackupType: Byte = 0; const ADatabaseName: string = ''; const ABackupName: string = ''); // 0=Full, 1=Diff, 2=Log
  end;

implementation

uses
  System.Math,
  System.StrUtils, // Para IfThen, SameText
  System.IOUtils,
  System.DateUtils,
  System.IniFiles,

  uLib.Config.Manager,
  uLib.Utils;

{ TMSSQLConnection }

constructor TMSSQLConnection.Create(const AConfig: TDBConnectionConfig; AMonitor: IDBMonitor = nil);
begin
  inherited Create(AConfig, AMonitor); // Llama al constructor de TBaseConnection
  FDBType:=MSSQL_QUOTE_CHAR;
  ConfigureDriverLink;
  LogMessage(Format('TMSSQLConnection created for config: %s', [AConfig.Name]), logInfo);
end;

destructor TMSSQLConnection.Destroy;
begin
  LogMessage(Format('TMSSQLConnection for config %s destroying...', [Config.Name]), logDebug);
  FreeAndNil(FDriverLink);
  LogMessage(Format('TMSSQLConnection for config %s destroyed.', [Config.Name]), logDebug);
  inherited;
end;

procedure TMSSQLConnection.ConfigureDriverLink;
var
  FIni: TMemIniFile;
begin
  if not FileExists('drivers.ini') then
  begin
    LogMessage(Format('TFDPhysMSSQLDriverLink file drivers does not exist: %s', ['drivers.ini']), logFatal);
    raise EDBCommandError.Create('MSSQL driver configuration file "drivers.ini" not found. Cannot initialize MSSQL connection.');
  end;

  FIni := TMemIniFile.Create('drivers.ini');
  try
    FDriverLink := TFDPhysMSSQLDriverLink.Create(nil);
    try
      FDriverLink.ODBCDriver := FIni.ReadString('MSSQL','ODBCDriver','SQL Server Native Client 11.0');
      LogMessage('TFDPhysMSSQLDriverLink instance created for TMSSQLConnection.', logDebug);
    except
      on E: Exception do
      begin
        FreeAndNil(FDriverLink);
        raise;
      end;
    end;
  finally
    FreeAndNil(FIni);
  end;
end;

function TMSSQLConnection.GetDriverSpecificConnectionString: string;
var
  Params: TStringList;
  TrustCertValue: string;
begin
  Params := TStringList.Create;
  try
    Params.Add('DriverID=MSSQL'); // Identificador de FireDAC para SQL Server
    Params.Add(Format('Server=%s', [Config.Server]));
    if Config.Port > 0 then // Solo añadir si no es el puerto por defecto (1433) o se especifica explícitamente
      Params.Add(Format('Port=%d', [Config.Port]));
    Params.Add(Format('Database=%s', [Config.Database]));
    Params.Add(Format('User_Name=%s', [Config.Username])); // User_Name es el parámetro correcto para FireDAC
    Params.Add(Format('Password=%s', [Config.Password]));

    Params.Add(Format('ApplicationName=%s', [IfThen(Config.ApplicationName.Trim <> '', Config.ApplicationName, 'DelphiAppServer')]));
    Params.Add('MultipleActiveResultSets=True'); // Habilitar MARS por defecto
    Params.Add(Format('ConnectTimeout=%d', [Config.ConnectionTimeout])); // Timeout de conexión en segundos

    if Config.SSL then
    begin
      Params.Add('Encrypt=Yes'); // O 'Mandatory' o 'Strict' según la política
      // TrustServerCertificate: 'Yes' para desarrollo/certificados autofirmados.
      // Para producción, debería ser 'No', y el certificado del servidor debe ser de una CA confiable
      // o estar en el almacén de confianza del cliente.
      // Se puede configurar a través de Config.Params.
      TrustCertValue := GetStrPair(Config.Params, 'TrustServerCertificate', 'No'); // Default a No advertir en producción.
      Params.Add('TrustServerCertificate=' + TrustCertValue);

      if SameText(TrustCertValue, 'Yes') then
        LogMessage('MSSQL Connection: TrustServerCertificate=Yes. This setting is insecure and should NOT be used in production environments unless the risk is fully understood and mitigated.', logCritical) // Elevado a logCritical
      else if SameText(TrustCertValue, 'No') then
        LogMessage('MSSQL Connection: TrustServerCertificate=No. Server certificate will be validated.', logInfo)
      else // Si el valor es algo diferente a 'Yes' o 'No'
        LogMessage(Format('MSSQL Connection: TrustServerCertificate set to an unconventional value: "%s". Behavior depends on driver interpretation.', [TrustCertValue]), logWarning);
    end;

    // Añadir otros parámetros específicos de MSSQL desde Config.Params si es necesario
    // Ejemplo: Params.Add(TJSONHelper.GetString(Config.Params, 'NetworkLibrary', 'DBMSSOCN'));
    if Config.Params.Trim <> '' then
       Params.Add(Config.Params); // Añadir parámetros adicionales si existen

    Result := Params.CommaText;
    LogMessage(Format('MSSQL Connection String (password omitted for log): Server=%s;Database=%s;User_Name=%s;...',
      [Config.Server, Config.Database, Config.Username]), logDebug);
  finally
    FreeAndNil(Params);
  end;
end;

procedure TMSSQLConnection.ApplyDriverSpecificSettings;
begin
  inherited; // Llama a TBaseConnection.ApplyDriverSpecificSettings (que no hace nada por defecto)
  LogMessage(Format('Applying MSSQL specific settings for connection "%s"...', [Config.Name]), logDebug);
  try
    // Configuraciones de sesión estándar recomendadas para SQL Server
    Execute('SET ANSI_NULLS ON;');
    Execute('SET ANSI_PADDING ON;');
    Execute('SET ANSI_WARNINGS ON;');
    Execute('SET ARITHABORT ON;');
    Execute('SET CONCAT_NULL_YIELDS_NULL ON;');
    Execute('SET QUOTED_IDENTIFIER ON;');
    Execute('SET NUMERIC_ROUNDABORT OFF;');
    // Formato de fecha seguro para evitar problemas de interpretación regional.
    // Aunque FireDAC suele manejar bien los TDateTime, esto puede ser una capa extra de seguridad.
    Execute('SET DATEFORMAT ymd;');

    // El esquema por defecto se maneja a nivel de usuario en la BD o calificando nombres de tabla.
    // No se suele cambiar con SET search_path como en PostgreSQL.
    if Config.Schema.Trim <> '' then
      LogMessage(Format('MSSQL Connection "%s": Default schema "%s" specified in config. Ensure queries qualify object names if not using the user''s default schema (e.g., "%s.MyTable").',
        [Config.Name, Config.Schema, Config.Schema]), logInfo);

  except
    on E: EDBConnectionError do
    begin
      LogMessage(Format('Critical connection error applying MSSQL settings for "%s": %s',
        [Config.Name, E.Message]), logError);
      raise; // Re-lanzar errores de conexión críticos
    end;
    on E: EDBCommandError do
    begin
      LogMessage(Format('Command error applying MSSQL settings for "%s": %s. Connection may work with default settings.',
        [Config.Name, E.Message]), logWarning);
      // No re-lanzar para comandos específicos que pueden fallar sin afectar la conexión
    end;
    on E: Exception do
    begin
      LogMessage(Format('Unexpected error applying MSSQL settings for "%s": %s - %s. Re-raising for debugging.',
        [Config.Name, E.ClassName, E.Message]), logError);
      raise; // Re-lanzar errores inesperados
    end;
  end;
end;

function TMSSQLConnection.GetVersion: string;
begin
  try
    // @@VERSION devuelve información detallada del producto, versión, y plataforma.
    Result := VarToStr(ExecuteScalar('SELECT @@VERSION;'));
  except
    on E: Exception do
    begin
      LogMessage(Format('Error getting SQL Server @@VERSION for config "%s": %s. Falling back to base GetVersion.', [Config.Name, E.Message]), logError);
      Result := inherited GetVersion; // Llama a TBaseConnection.GetVersion
    end;
  end;
end;

function TMSSQLConnection.GetDatabaseFilesAsJSON: TJSONObject;
var
  LQuery: TDataSet;
  LJSONArray: TJSONArray;
  LFileObject: TJSONObject;
  SQL: string;
begin
  Result := TJSONObject.Create;
  LJSONArray := TJSONArray.Create;
  Result.AddPair('database_files', LJSONArray); // El TJSONObject toma posesión del TJSONArray
  LQuery := nil;

  // Usar DB_NAME() para la base de datos actual es más seguro que inyectar Config.Database
  // si la conexión ya está establecida a esa base de datos.
  // Si se necesita consultar otra base de datos en el mismo servidor, se necesitaría calificar sys.database_files.
  // Por ahora, asumimos que la conexión actual es a Config.Database.
  SQL := 'SELECT name, physical_name, type_desc, state_desc, ' +
         'CAST(size/128.0 AS DECIMAL(18,2)) AS size_mb, ' + // size es en páginas de 8KB
         'CAST(FILEPROPERTY(name, ''SpaceUsed'')/128.0 AS DECIMAL(18,2)) AS used_space_mb, ' +
         'growth, is_percent_growth ' +
         'FROM sys.database_files;'; // Para la base de datos actual
  try
    LQuery := ExecuteReader(SQL); // ExecuteReader devuelve un TDataSet que el llamador debe liberar
    while not LQuery.Eof do
    begin
      LFileObject := TJSONObject.Create;
      LFileObject.AddPair('name', LQuery.FieldByName('name').AsString);
      LFileObject.AddPair('physical_name', LQuery.FieldByName('physical_name').AsString);
      LFileObject.AddPair('type_desc', LQuery.FieldByName('type_desc').AsString);
      LFileObject.AddPair('state_desc', LQuery.FieldByName('state_desc').AsString);
      LFileObject.AddPair('size_mb', LQuery.FieldByName('size_mb').AsFloat); // O AsCurrency si se prefiere
      LFileObject.AddPair('used_space_mb', LQuery.FieldByName('used_space_mb').AsFloat);
      LFileObject.AddPair('growth_units', LQuery.FieldByName('growth').AsInteger); // En páginas de 8KB o porcentaje
      LFileObject.AddPair('is_percent_growth', LQuery.FieldByName('is_percent_growth').AsBoolean);
      LJSONArray.Add(LFileObject); // LJSONArray toma posesión de LFileObject
      LQuery.Next;
    end;
  finally
    FreeAndNil(LQuery);
  end;
end;

procedure TMSSQLConnection.ShrinkDatabase(const ADatabaseName: string = ''; ATargetPercent: Integer = 0);
var
  DBName: string;
  Params: TFDParams;
  SQL: string;
begin
  DBName := ADatabaseName.Trim;
  if DBName.IsEmpty then
    DBName := Config.Database;
  if DBName.IsEmpty then
    raise EDBCommandError.Create('ShrinkDatabase: Database name not specified and not available from config.');

  Params := TFDParams.Create;
  try
    if (ATargetPercent < 0) or (ATargetPercent > 99) then
      SQL := 'DBCC SHRINKDATABASE (?);'
    else
    begin
      SQL := 'DBCC SHRINKDATABASE (?, ?);';
      Params.Add('target_percent', ATargetPercent);
    end;

    Params.Add('database_name', DBName);
    Execute(SQL, Params);
    LogMessage(Format('DBCC SHRINKDATABASE executed for database "%s" with target percent %d.', [DBName, ATargetPercent]), logWarning);
  finally
    FreeAndNil(Params);
  end;
end;

procedure TMSSQLConnection.RebuildIndexes(const ATableName: string = ''; AWithOnlineOption: Boolean = False);
var
  SQL: string;
  OnlineOptionStr: string;
begin
  // Reconstruir índices puede ser una operación larga y que consume recursos.
  // La opción ONLINE requiere SQL Server Enterprise Edition (o equivalentes en Azure SQL).
  OnlineOptionStr := '';
  if AWithOnlineOption then
  begin
    // Verificar la edición del servidor podría ser necesario antes de usar ONLINE=ON.
    // Por ahora, se asume que el llamador sabe si es aplicable.
    OnlineOptionStr := ' WITH (ONLINE = ON)'; // O OFF si no se quiere o no se puede online
    LogMessage('RebuildIndexes: Attempting ONLINE index rebuild. Ensure server edition supports this.', logWarning);
  end;

  if ATableName.Trim.IsEmpty then // Para todas las tablas de la base de datos actual
    SQL := 'EXEC sp_MSforeachtable @command1="PRINT ''Rebuilding indexes for ?'' ALTER INDEX ALL ON ? REBUILD' + OnlineOptionStr + '"'
  else
    SQL := Format('ALTER INDEX ALL ON %s REBUILD%s;', [QuoteIdentifier(ATableName, FDBType), OnlineOptionStr]); // Usar QuoteIdentifier

  Execute(SQL);
  LogMessage(Format('Indexes rebuilt for %s%s.', [IfThen(ATableName.Trim = '', 'all tables', QuoteIdentifier(ATableName, FDBType)), OnlineOptionStr]), logInfo);
end;

procedure TMSSQLConnection.UpdateStatistics(const ATableName: string = ''; AWithFullScan: Boolean = True);
var
  SQL: string;
  ScanOptionStr: string;
begin
  ScanOptionStr := IfThen(AWithFullScan, ' WITH FULLSCAN', ' WITH SAMPLE'); // O RESAMPLE

  if ATableName.Trim.IsEmpty then
    SQL := 'EXEC sp_updatestats;' // Actualiza estadísticas para todos los objetos de la BD actual
  else
    SQL := Format('UPDATE STATISTICS %s%s;', [QuoteIdentifier(ATableName, FDBType), ScanOptionStr]);

  Execute(SQL);
  LogMessage(Format('Statistics updated for %s%s.', [IfThen(ATableName.Trim = '', 'all tables', QuoteIdentifier(ATableName, FDBType)), ScanOptionStr]), logInfo);
end;

procedure TMSSQLConnection.CheckDB(const ADatabaseName: string = ''; ARepairOption: string = 'NO_INFOMSGS');
var
  DBName, ValidRepairOption: string;
  SQL: string;
  AllowedOptions: TArray<string>;
  IsValidOption: Boolean;
  i: Integer;
begin
  DBName := ADatabaseName.Trim;
  if DBName.IsEmpty then
    DBName := Config.Database;
  if DBName.IsEmpty then
    raise EDBCommandError.Create('CheckDB: Database name not specified and not available from config.');

  // Validar nombre de base de datos (solo caracteres seguros)
  if not IsValidIdentifier(DBName) then
    raise EDBCommandError.CreateFmt('Invalid database name: %s', [DBName]);

  // Validar ARepairOption contra lista permitida
  AllowedOptions := ['NO_INFOMSGS', 'PHYSICAL_ONLY', 'REPAIR_ALLOW_DATA_LOSS', 'REPAIR_FAST', 'REPAIR_REBUILD', 'ALL_ERRORMSGS', 'TABLOCK', 'ESTIMATEONLY'];
  IsValidOption := False;
  for i := Low(AllowedOptions) to High(AllowedOptions) do
  begin
    if SameText(ARepairOption, AllowedOptions[i]) then
    begin
      IsValidOption := True;
      Break;
    end;
  end;

  ValidRepairOption := IfThen(IsValidOption, ARepairOption, 'NO_INFOMSGS');

  if SameText(ValidRepairOption, 'REPAIR_ALLOW_DATA_LOSS') then
    LogMessage(Format('CheckDB: WARNING - Using REPAIR_ALLOW_DATA_LOSS for database "%s". This may result in data loss.', [DBName]), logCritical);

  // Usar QuoteIdentifier pero con validación previa
  SQL := Format('DBCC CHECKDB (%s) WITH %s;', [QuoteIdentifier(DBName, FDBType), ValidRepairOption]);
  Execute(SQL);
  LogMessage(Format('DBCC CHECKDB WITH %s completed for database "%s".', [ValidRepairOption, DBName]), logWarning);
end;

function TMSSQLConnection.GetWaitStatsAsJSON(ATopN: Integer = 15): TJSONObject;
const
  SQL_WAIT_STATS_TEMPLATE = // Query de Paul Randal, adaptada
    'WITH [Waits] AS (' +
    '  SELECT ' +
    '    [wait_type], ' +
    '    [wait_time_ms] / 1000.0 AS [WaitS], ' +
    '    ([wait_time_ms] - [signal_wait_time_ms]) / 1000.0 AS [ResourceS], ' +
    '    [signal_wait_time_ms] / 1000.0 AS [SignalS], ' +
    '    [waiting_tasks_count] AS [WaitCount], ' +
    '    100.0 * [wait_time_ms] / SUM ([wait_time_ms]) OVER() AS [Percentage] ' +
    '  FROM sys.dm_os_wait_stats ' +
    '  WHERE [wait_type] NOT IN ( ' + // Excluir esperas benignas/comunes
    '    N''BROKER_EVENTHANDLER'', N''BROKER_RECEIVE_WAITFOR'', N''BROKER_TASK_STOP'', N''BROKER_TO_FLUSH'', N''BROKER_TRANSMITTER'', ' +
    '    N''CHECKPOINT_QUEUE'', N''CHKPT'', N''CLR_AUTO_EVENT'', N''CLR_MANUAL_EVENT'', N''CLR_SEMAPHORE'', N''CXCONSUMER'', ' +
    '    N''DBMIRROR_DBM_EVENT'', N''DBMIRROR_EVENTS_QUEUE'', N''DBMIRROR_WORKER_QUEUE'', N''DBMIRRORING_CMD'', N''DIRTY_PAGE_POLL'', ' +
    '    N''DISPATCHER_QUEUE_SEMAPHORE'', N''EXECSYNC'', N''FSAGENT'', N''FT_IFTS_SCHEDULER_IDLE_WAIT'', N''FT_IFTSHC_MUTEX'', ' +
    '    N''HADR_CLUSAPI_CALL'', N''HADR_FILESTREAM_IOMGR_IOCOMPLETION'', N''HADR_LOGCAPTURE_WAIT'', N''HADR_NOTIFICATION_DEQUEUE'', ' +
    '    N''HADR_TIMER_TASK'', N''HADR_WORK_QUEUE'', N''KSOURCE_WAKEUP'', N''LAZYWRITER_SLEEP'', N''LOGMGR_QUEUE'', ' +
    '    N''MEMORY_ALLOCATION_EXT'', N''ONDEMAND_TASK_QUEUE'', N''PARALLEL_REDO_DRAIN_WORKER'', N''PARALLEL_REDO_LOG_CACHE'', ' +
    '    N''PARALLEL_REDO_TRAN_LIST'', N''PARALLEL_REDO_WORKER_SYNC'', N''PARALLEL_REDO_WORKER_WAIT_WORK'', N''PREEMPTIVE_OS_FLUSHFILEBUFFERS'', ' +
    '    N''PREEMPTIVE_OS_AUTHENTICATIONOPS'', N''PREEMPTIVE_OS_GENERICOPS'', N''PREEMPTIVE_OS_LIBRARYOPS'', N''PREEMPTIVE_OS_QUERYREGISTRY'', ' +
    '    N''PREEMPTIVE_OS_WRITEFILEGATHER'', N''PREEMPTIVE_XE_CALLBACKEXECUTE'', N''PREEMPTIVE_XE_DISPATCHER'', N''PREEMPTIVE_XE_GETTARGETSTATE'', ' +
    '    N''PREEMPTIVE_XE_SESSIONCOMMIT'', N''PREEMPTIVE_XE_TARGETINIT'', N''PREEMPTIVE_XE_TARGETFINALIZE'', N''PWAIT_ALL_COMPONENTS_INITIALIZED'', ' +
    '    N''PWAIT_DIRECTLOGCONSUMER_GETNEXT'', N''QDS_PERSIST_TASK_MAIN_LOOP_SLEEP'', N''QDS_ASYNC_QUEUE_ACTIVATION'', ' +
    '    N''QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP'', N''QDS_SHUTDOWN_QUEUE'', N''REDO_THREAD_PENDING_WORK'', N''REQUEST_FOR_DEADLOCK_SEARCH'', ' +
    '    N''RESOURCE_QUEUE'', N''SERVER_IDLE_CHECK'', N''SLEEP_BPOOL_FLUSH'', N''SLEEP_DBSTARTUP'', N''SLEEP_DCOMSTARTUP'', N''SLEEP_MASTERDBREADY'', ' +
    '    N''SLEEP_MASTERMDREADY'', N''SLEEP_MASTERUPGRADED'', N''SLEEP_MSDBSTARTUP'', N''SLEEP_SYSTEMTASK'', N''SLEEP_TASK'', N''SLEEP_TEMPDBSTARTUP'', ' +
    '    N''SNI_HTTP_ACCEPT'', N''SOS_WORK_DISPATCHER'', N''SP_SERVER_DIAGNOSTICS_SLEEP'', N''SQLTRACE_BUFFER_FLUSH'', N''SQLTRACE_INCREMENTAL_FLUSH_SLEEP'', ' +
    '    N''SQLTRACE_WAIT_ENTRIES'', N''WAIT_FOR_RESULTS'', N''WAITFOR'', N''WAITFOR_TASKSHUTDOWN'', N''WAIT_XTP_RECOVERY'', ' +
    '    N''WAIT_XTP_HOST_WAIT'', N''WAIT_XTP_OFFLINE_CKPT_NEW_LOG'', N''WAIT_XTP_CKPT_CLOSE'', N''XE_DISPATCHER_JOIN'', ' +
    '    N''XE_DISPATCHER_WAIT'', N''XE_TIMER_EVENT'') ' +
    '  AND [waiting_tasks_count] > 0 ' + // Solo mostrar esperas que realmente ocurrieron
    ') ' +
    'SELECT TOP (%d) ' + // Usar parámetro para TOP N
    '  [W1].[wait_type] AS [WaitType], ' +
    '  CAST ([W1].[WaitS] AS DECIMAL (16,2)) AS [Wait_S], ' +
    '  CAST ([W1].[ResourceS] AS DECIMAL (16,2)) AS [Resource_S], ' +
    '  CAST ([W1].[SignalS] AS DECIMAL (16,2)) AS [Signal_S], ' +
    '  [W1].[WaitCount] AS [WaitCount], ' +
    '  CAST ([W1].[Percentage] AS DECIMAL (5,2)) AS [Percentage] ' +
    'FROM [Waits] AS [W1] ' +
    'ORDER BY [Wait_S] DESC;';
var
  LQuery: TDataSet;
  LJSONArray: TJSONArray;
  LStatObject: TJSONObject;
  SQL: string;
begin
  Result := TJSONObject.Create;
  LJSONArray := TJSONArray.Create;
  Result.AddPair('top_wait_stats', LJSONArray);
  LQuery := nil;
  SQL := Format(SQL_WAIT_STATS_TEMPLATE, [IfThen(ATopN > 0, ATopN, 15)]);

  try
    LQuery := ExecuteReader(SQL);
    while not LQuery.Eof do
    begin
      LStatObject := TJSONObject.Create;
      LStatObject.AddPair('wait_type', LQuery.FieldByName('WaitType').AsString);
      LStatObject.AddPair('wait_seconds', LQuery.FieldByName('Wait_S').AsFloat);
      LStatObject.AddPair('resource_seconds', LQuery.FieldByName('Resource_S').AsFloat);
      LStatObject.AddPair('signal_seconds', LQuery.FieldByName('Signal_S').AsFloat);
      LStatObject.AddPair('wait_count', LQuery.FieldByName('WaitCount').AsLargeInt);
      LStatObject.AddPair('percentage_of_total_waits', LQuery.FieldByName('Percentage').AsFloat);
      LJSONArray.Add(LStatObject);
      LQuery.Next;
    end;
  finally
    FreeAndNil(LQuery);
  end;
end;

function TMSSQLConnection.GetBlockingProcessesAsJSONArray: TJSONArray;
const
  SQL_BLOCKING = // Query mejorada para incluir más detalles
    'SELECT ' +
    '  req.session_id AS BlockedSPID, ' +
    '  DB_NAME(req.database_id) AS BlockedDatabase, ' +
    '  obj.name AS BlockedObject, ' +
    '  req.blocking_session_id AS BlockingSPID, ' +
    '  req.wait_type AS BlockedWaitType, ' +
    '  req.wait_time AS BlockedWaitTimeMs, ' +
    '  req.wait_resource AS BlockedWaitResource, ' +
    '  (SELECT [text] FROM sys.dm_exec_sql_text(blocked_sql.sql_handle)) AS BlockedSQLText, ' +
    '  (SELECT [text] FROM sys.dm_exec_sql_text(blocking_sql.sql_handle)) AS BlockingSQLText, ' +
    '  blocking_req.status AS BlockingStatus, ' +
    '  blocking_req.cpu_time AS BlockingCPUTimeMs, ' +
    '  blocking_req.total_elapsed_time AS BlockingTotalTimeMs ' +
    'FROM sys.dm_exec_requests req ' +
    'INNER JOIN sys.dm_exec_connections blocked_conn ON req.session_id = blocked_conn.session_id ' +
    'INNER JOIN sys.dm_exec_connections blocking_conn ON req.blocking_session_id = blocking_conn.session_id ' +
    'LEFT JOIN sys.dm_exec_requests blocking_req ON req.blocking_session_id = blocking_req.session_id ' +
    'OUTER APPLY sys.dm_exec_sql_text(blocked_conn.most_recent_sql_handle) AS blocked_sql ' +
    'OUTER APPLY sys.dm_exec_sql_text(blocking_conn.most_recent_sql_handle) AS blocking_sql ' +
    // Intento básico de parsear el objeto bloqueado desde wait_resource
    'LEFT JOIN sys.partitions p ON p.partition_id = CASE WHEN req.resource_type = ''OBJECT'' THEN req.resource_description ELSE NULL END ' +
    'LEFT JOIN sys.objects obj ON obj.object_id = p.object_id ' +
    'WHERE req.blocking_session_id <> 0;';
var
  LQuery: TDataSet;
  LBlockObject: TJSONObject;
begin
  Result := TJSONArray.Create;
  LQuery := nil;
  try
    LQuery := ExecuteReader(SQL_BLOCKING);
    while not LQuery.Eof do
    begin
      LBlockObject := TJSONObject.Create;
      LBlockObject.AddPair('blocked_spid', LQuery.FieldByName('BlockedSPID').AsInteger);
      LBlockObject.AddPair('blocked_database', LQuery.FieldByName('BlockedDatabase').AsString);
      LBlockObject.AddPair('blocked_object_name', LQuery.FieldByName('BlockedObject').AsString); // Puede ser NULL
      LBlockObject.AddPair('blocking_spid', LQuery.FieldByName('BlockingSPID').AsInteger);
      LBlockObject.AddPair('blocked_wait_type', LQuery.FieldByName('BlockedWaitType').AsString);
      LBlockObject.AddPair('blocked_wait_time_ms', LQuery.FieldByName('BlockedWaitTimeMs').AsInteger);
      LBlockObject.AddPair('blocked_wait_resource', LQuery.FieldByName('BlockedWaitResource').AsString);
      LBlockObject.AddPair('blocked_sql_text', LQuery.FieldByName('BlockedSQLText').AsString);
      LBlockObject.AddPair('blocking_sql_text', LQuery.FieldByName('BlockingSQLText').AsString);
      LBlockObject.AddPair('blocking_status', LQuery.FieldByName('BlockingStatus').AsString);
      LBlockObject.AddPair('blocking_cpu_time_ms', LQuery.FieldByName('BlockingCPUTimeMs').AsInteger);
      LBlockObject.AddPair('blocking_total_time_ms', LQuery.FieldByName('BlockingTotalTimeMs').AsInteger);
      Result.Add(LBlockObject);
      LQuery.Next;
    end;
  finally
    FreeAndNil(LQuery);
  end;
end;

function TMSSQLConnection.GetQueryStatsAsJSON(ATopN: Integer = 15): TJSONObject;
const
  SQL_QUERY_STATS_TEMPLATE = // Query mejorada para incluir avg y convertir tiempos a ms
    'SELECT TOP (%d) ' +
    '  SUBSTRING(qt.text, (qs.statement_start_offset/2) + 1, ' +
    '    ((CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(qt.text) ELSE qs.statement_end_offset END - qs.statement_start_offset)/2) + 1) AS QueryText, ' +
    '  qs.execution_count, ' +
    '  qs.total_logical_reads, qs.last_logical_reads, ' +
    '  qs.total_physical_reads, qs.last_physical_reads, ' +
    '  qs.total_worker_time / 1000 AS total_cpu_time_ms, qs.last_worker_time / 1000 AS last_cpu_time_ms, ' +
    '  (qs.total_worker_time / 1000.0) / qs.execution_count AS avg_cpu_time_ms, ' +
    '  qs.total_elapsed_time / 1000 AS total_elapsed_time_ms, qs.last_elapsed_time / 1000 AS last_elapsed_time_ms, ' +
    '  (qs.total_elapsed_time / 1000.0) / qs.execution_count AS avg_elapsed_time_ms, ' +
    '  qs.creation_time, qs.last_execution_time, ' +
    '  qp.query_plan ' + // Incluir el plan, pero advertir que puede ser grande
    'FROM sys.dm_exec_query_stats qs ' +
    'CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt ' +
    'CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp ' + // Query plan XML
    'WHERE qs.execution_count > 0 ' + // Evitar división por cero en promedios
    'ORDER BY qs.total_elapsed_time DESC;'; // O por total_worker_time, o avg_elapsed_time_ms
var
  LQuery: TDataSet;
  LJSONArray: TJSONArray;
  LStatObject: TJSONObject;
  SQL: string;
begin
  Result := TJSONObject.Create;
  LJSONArray := TJSONArray.Create;
  Result.AddPair('top_queries_by_total_elapsed_time', LJSONArray);
  LQuery := nil;
  SQL := Format(SQL_QUERY_STATS_TEMPLATE, [IfThen(ATopN > 0, ATopN, 15)]);

  try
    LQuery := ExecuteReader(SQL);
    while not LQuery.Eof do
    begin
      LStatObject := TJSONObject.Create;
      LStatObject.AddPair('query_text', LQuery.FieldByName('QueryText').AsString);
      LStatObject.AddPair('execution_count', LQuery.FieldByName('execution_count').AsLargeInt);
      LStatObject.AddPair('total_logical_reads', LQuery.FieldByName('total_logical_reads').AsLargeInt);
      LStatObject.AddPair('total_physical_reads', LQuery.FieldByName('total_physical_reads').AsLargeInt);
      LStatObject.AddPair('total_cpu_time_ms', LQuery.FieldByName('total_cpu_time_ms').AsLargeInt);
      LStatObject.AddPair('avg_cpu_time_ms', LQuery.FieldByName('avg_cpu_time_ms').AsFloat);
      LStatObject.AddPair('total_elapsed_time_ms', LQuery.FieldByName('total_elapsed_time_ms').AsLargeInt);
      LStatObject.AddPair('avg_elapsed_time_ms', LQuery.FieldByName('avg_elapsed_time_ms').AsFloat);
      LStatObject.AddPair('last_execution_time', LQuery.FieldByName('last_execution_time').AsDateTime); // Es DateTime
      // LStatObject.AddPair('query_plan_xml', LQuery.FieldByName('query_plan').AsString); // El plan puede ser muy grande
      LJSONArray.Add(LStatObject);
      LQuery.Next;
    end;
  finally
    FreeAndNil(LQuery);
  end;
end;

function TMSSQLConnection.GetMemoryUsageAsJSON: TJSONObject;
const // Query más completa para uso de memoria
  SQL_MEMORY_USAGE =
    'SELECT ' +
    '  (SELECT cntr_value FROM sys.dm_os_performance_counters WHERE object_name LIKE ''%Memory Manager%'' AND counter_name = ''Total Server Memory (KB)'') AS TotalServerMemoryKB, ' +
    '  (SELECT cntr_value FROM sys.dm_os_performance_counters WHERE object_name LIKE ''%Memory Manager%'' AND counter_name = ''Target Server Memory (KB)'') AS TargetServerMemoryKB, ' +
    '  (SELECT cntr_value FROM sys.dm_os_performance_counters WHERE object_name LIKE ''%Buffer Manager%'' AND counter_name = ''Database Cache Memory (KB)'') AS DatabaseCacheMemoryKB, ' +
    '  (SELECT cntr_value FROM sys.dm_os_performance_counters WHERE object_name LIKE ''%Buffer Manager%'' AND counter_name = ''Page life expectancy'') AS PageLifeExpectancy_Sec, ' +
    '  (SELECT cntr_value FROM sys.dm_os_performance_counters WHERE object_name LIKE ''%Memory Manager%'' AND counter_name = ''Memory Grants Pending'') AS MemoryGrantsPending, ' +
    '  (SELECT cntr_value FROM sys.dm_os_performance_counters WHERE object_name LIKE ''%Memory Manager%'' AND counter_name = ''Memory Grants Outstanding'') AS MemoryGrantsOutstanding;';
var
  LQuery: TDataSet;
begin
  Result := TJSONObject.Create;
  LQuery := nil;
  try
    LQuery := ExecuteReader(SQL_MEMORY_USAGE);
    if not LQuery.Eof then
    begin
      Result.AddPair('total_server_memory_kb', LQuery.FieldByName('TotalServerMemoryKB').AsLargeInt);
      Result.AddPair('target_server_memory_kb', LQuery.FieldByName('TargetServerMemoryKB').AsLargeInt);
      Result.AddPair('database_cache_memory_kb', LQuery.FieldByName('DatabaseCacheMemoryKB').AsLargeInt);
      Result.AddPair('page_life_expectancy_sec', LQuery.FieldByName('PageLifeExpectancy_Sec').AsLargeInt);
      Result.AddPair('memory_grants_pending', LQuery.FieldByName('MemoryGrantsPending').AsInteger);
      Result.AddPair('memory_grants_outstanding', LQuery.FieldByName('MemoryGrantsOutstanding').AsInteger);
    end
    else
      Result.AddPair('error', 'Could not retrieve memory usage data.');
  finally
    FreeAndNil(LQuery);
  end;
end;

procedure TMSSQLConnection.KillProcess(ASPID: Integer);
var
  SessionInfo: TDataSet;
  Params: TFDParams;
  SQL: string;
begin
  if ASPID <= 0 then
    raise EDBCommandError.Create('Invalid SPID specified for KILL command.');
  SessionInfo := nil;

  Params := TFDParams.Create;  // ← AGREGAR ESTA LÍNEA
  try
    // Verificar si es un proceso del sistema consultando sys.dm_exec_sessions
    SQL := 'SELECT is_user_process, login_name, program_name, host_name ' +
           'FROM sys.dm_exec_sessions WHERE session_id = ?';

    Params.Add('session_id', ASPID);  // ← AGREGAR ESTA LÍNEA
    SessionInfo := ExecuteReader(SQL, Params);

    if SessionInfo.Eof then
      raise EDBCommandError.CreateFmt('SPID %d not found or already terminated.', [ASPID]);

    // Verificar si es proceso de usuario
    if not SessionInfo.FieldByName('is_user_process').AsBoolean then
      raise EDBCommandError.CreateFmt('Cannot kill system process SPID %d.', [ASPID]);

    // Log información del proceso antes de terminarlo
    LogMessage(Format('Terminating SPID %d: User=%s, Program=%s, Host=%s',
      [ASPID,
       SessionInfo.FieldByName('login_name').AsString,
       SessionInfo.FieldByName('program_name').AsString,
       SessionInfo.FieldByName('host_name').AsString]), logWarning);

  finally
    FreeAndNil(SessionInfo);
    FreeAndNil(Params);  // ← AGREGAR ESTA LÍNEA
  end;

  Execute(Format('KILL %d;', [ASPID]));
  LogMessage(Format('KILL command executed for SPID: %d', [ASPID]), logWarning);
end;

procedure TMSSQLConnection.ClearProcedureCache;
begin
  // DBCC FREEPROCCACHE limpia todo el plan cache. Usar con precaución.
  // Para limpiar el plan de una consulta específica: DBCC FREEPROCCACHE (plan_handle)
  // Para limpiar el plan de una base de datos específica: DBCC FLUSHPROCINDB (db_id)
  Execute('DBCC FREEPROCCACHE;');
  LogMessage('SQL Server procedure cache (entire plan cache) cleared.', logWarning);
end;

function TMSSQLConnection.GetBackupHistoryAsJSONArray(ADatabaseName: string = ''): TJSONArray;
var
  SQL_BACKUP_HISTORY: string;
  LQuery: TDataSet;
  LBackupObject: TJSONObject;
  params: TFDParams;
  DBName: string;
begin
  Result := TJSONArray.Create;
  LQuery := nil;
  DBName := ADatabaseName.Trim;
  if DBName.IsEmpty then
    DBName := Config.Database;

  if DBName.IsEmpty then
  begin
    LogMessage('GetBackupHistory: Database name not specified and not in config. Returning empty array.', logWarning);
    Exit;
  end;

  SQL_BACKUP_HISTORY :=
    'SELECT TOP 100 ' +
    '  bs.database_name, ' +
    '  bs.backup_start_date, bs.backup_finish_date, ' +
    '  CAST((bs.backup_size / 1024.0 / 1024.0) AS DECIMAL(18, 2)) AS backup_size_mb, ' +
    '  CAST((bs.compressed_backup_size / 1024.0 / 1024.0) AS DECIMAL(18, 2)) AS compressed_size_mb, ' +
    '  bmf.physical_device_name, ' +
    '  CASE bs.type ' +
    '    WHEN ''D'' THEN ''Database'' ' +
    '    WHEN ''I'' THEN ''Differential database'' ' +
    '    WHEN ''L'' THEN ''Log'' ' +
    '    WHEN ''F'' THEN ''File or filegroup'' ' +
    '    WHEN ''G'' THEN ''Differential file'' ' +
    '    WHEN ''P'' THEN ''Partial'' ' +
    '    WHEN ''Q'' THEN ''Differential partial'' ' +
    '    ELSE ''Other'' ' +
    '  END AS backup_type_desc, ' +
    '  bs.first_lsn, bs.last_lsn, bs.database_backup_lsn ' + // LSNs para la cadena de restauración
    'FROM msdb.dbo.backupset bs ' +
    'JOIN msdb.dbo.backupmediafamily bmf ON bs.media_set_id = bmf.media_set_id ' +
    'WHERE bs.database_name = :DBName ' + // Usar parámetro
    'ORDER BY bs.backup_start_date DESC;';
  params:= TFDParams.Create;
  try
    params.add('DBName',DBName);
    LQuery := ExecuteReader(SQL_BACKUP_HISTORY, params); // Pasar DBName como parámetro
    while not LQuery.Eof do
    begin
      LBackupObject := TJSONObject.Create;
      LBackupObject.AddPair('database_name', LQuery.FieldByName('database_name').AsString);
      LBackupObject.AddPair('backup_type_desc', LQuery.FieldByName('backup_type_desc').AsString);
      LBackupObject.AddPair('backup_start_date_utc', DateToISO8601(LQuery.FieldByName('backup_start_date').AsDateTime)); // Asumir que son UTC o convertir
      LBackupObject.AddPair('backup_finish_date_utc', DateToISO8601(LQuery.FieldByName('backup_finish_date').AsDateTime));
      LBackupObject.AddPair('backup_size_mb', LQuery.FieldByName('backup_size_mb').AsFloat);
      LBackupObject.AddPair('compressed_size_mb', LQuery.FieldByName('compressed_size_mb').AsFloat);
      LBackupObject.AddPair('physical_device_name', LQuery.FieldByName('physical_device_name').AsString);
      Result.Add(LBackupObject);
      LQuery.Next;
    end;
  finally
    FreeAndNil(Params);
    FreeAndNil(LQuery);
  end;
end;

function TMSSQLConnection.IsSafeBackupPath(const ABackupPath: string; out ASafeFullPath: string): Boolean;
var
 SafeBackupDir, FileName, FullPath, SafeDirNormalized, FullPathNormalized: string;
 ConfigMgr: TConfigManager;
 i: Integer;
 C: Char;
begin
 Result := False;
 ASafeFullPath := '';

 // Obtener directorio base seguro desde configuración
 try
   ConfigMgr := TConfigManager.GetInstance;
   if Assigned(ConfigMgr) and Assigned(ConfigMgr.ConfigData) then
   begin
     // Buscar configuración específica de backup
     SafeBackupDir := TJSONHelper.GetString(ConfigMgr.ConfigData, 'database.backup.defaultPath', '');

     // Si no está en configuración general, buscar por tipo de BD
     if SafeBackupDir.Trim = '' then
     begin
       SafeBackupDir := TJSONHelper.GetString(ConfigMgr.ConfigData, 'database.backup.mssql.defaultPath', '');
     end;

     // Si tampoco está la específica, buscar en configuración del pool actual
     if SafeBackupDir.Trim = '' then
     begin
       SafeBackupDir := TJSONHelper.GetString(ConfigMgr.ConfigData,
         Format('database.pools.%s.backupPath', [Config.Name]), '');
     end;
   end;
 except
   on E: Exception do
   begin
     LogMessage(Format('Error reading backup path from configuration: %s', [E.Message]), logWarning);
     SafeBackupDir := '';
   end;
 end;

 // Fallback a valor por defecto si no se pudo obtener de configuración
 if SafeBackupDir.Trim = '' then
 begin
   {$IFDEF MSWINDOWS}
   SafeBackupDir := 'C:\SQLBackups';
   {$ELSE}
   SafeBackupDir := '/var/backups/sql';
   {$ENDIF}
   LogMessage(Format('No backup path configured, using fallback directory: %s', [SafeBackupDir]), logWarning);
 end
 else
 begin
   LogMessage(Format('Using configured backup directory: %s', [SafeBackupDir]), logDebug);
 end;

 // Normalizar y validar el directorio base
 try
   SafeBackupDir := TPath.GetFullPath(SafeBackupDir);
 except
   on E: Exception do
   begin
     LogMessage(Format('Invalid backup directory path "%s": %s', [SafeBackupDir, E.Message]), logError);
     Exit;
   end;
 end;

 // Asegurar que el directorio existe
 try
   if not TDirectory.Exists(SafeBackupDir) then
   begin
     LogMessage(Format('Creating backup directory: %s', [SafeBackupDir]), logInfo);
     TDirectory.CreateDirectory(SafeBackupDir);

     // Verificar que se creó correctamente
     if not TDirectory.Exists(SafeBackupDir) then
     begin
       LogMessage(Format('Failed to create backup directory: %s', [SafeBackupDir]), logError);
       Exit;
     end;
   end;
 except
   on E: Exception do
   begin
     LogMessage(Format('Error accessing/creating backup directory %s: %s', [SafeBackupDir, E.Message]), logError);
     Exit;
   end;
 end;

 // Extraer solo el nombre del archivo, eliminando cualquier path
 FileName := TPath.GetFileName(ABackupPath.Trim);
 if FileName.IsEmpty then
 begin
   LogMessage('Backup filename is empty after path extraction', logError);
   Exit;
 end;

 // Validar longitud del filename
 if Length(FileName) > 100 then
 begin
   LogMessage(Format('Backup filename too long (%d characters): %s', [Length(FileName), FileName]), logError);
   Exit;
 end;

 // Validar caracteres del filename - solo permitir caracteres seguros
 for i := 1 to Length(FileName) do
 begin
   C := FileName[i];
   if not CharInSet(C, ['a'..'z', 'A'..'Z', '0'..'9', '_', '-', '.']) then
   begin
     LogMessage(Format('Invalid character "%s" in backup filename: %s', [C, FileName]), logError);
     Exit;
   end;
 end;

 // Validar que no empiece con punto (archivos ocultos)
 if FileName.StartsWith('.') then
 begin
   LogMessage(Format('Backup filename cannot start with dot: %s', [FileName]), logError);
   Exit;
 end;

 // Validar extensión - solo permitir extensiones de backup conocidas
 var FileExt := TPath.GetExtension(FileName).ToLower;
 if not (FileExt.Equals('.bak') or FileExt.Equals('.trn') or
         FileExt.Equals('.diff') or FileExt.Equals('.backup')) then
 begin
   LogMessage(Format('Invalid backup file extension "%s" in filename: %s', [FileExt, FileName]), logError);
   Exit;
 end;

 // Prevenir nombres especiales de Windows
 var FileNameWithoutExt := TPath.GetFileNameWithoutExtension(FileName).ToUpper;
 if (FileNameWithoutExt = 'CON') or (FileNameWithoutExt = 'PRN') or
    (FileNameWithoutExt = 'AUX') or (FileNameWithoutExt = 'NUL') or
    (FileNameWithoutExt = 'COM1') or (FileNameWithoutExt = 'COM2') or
    (FileNameWithoutExt = 'LPT1') or (FileNameWithoutExt = 'LPT2') then
 begin
   LogMessage(Format('Backup filename uses reserved Windows name: %s', [FileName]), logError);
   Exit;
 end;

 // Prevenir nombres que pueden causar problemas
 if FileNameWithoutExt.Contains('..') or FileNameWithoutExt.Contains('\\') or
    FileNameWithoutExt.Contains('/') then
 begin
   LogMessage(Format('Backup filename contains invalid sequences: %s', [FileName]), logError);
   Exit;
 end;

 // Construir path completo
 try
   FullPath := TPath.Combine(SafeBackupDir, FileName);
 except
   on E: Exception do
   begin
     LogMessage(Format('Error combining paths "%s" + "%s": %s', [SafeBackupDir, FileName, E.Message]), logError);
     Exit;
   end;
 end;

 // Normalizar paths para comparación segura y prevenir path traversal
 try
   SafeDirNormalized := TPath.GetFullPath(SafeBackupDir).ToLower;
   FullPathNormalized := TPath.GetFullPath(FullPath).ToLower;

   // Verificar que el path final esté dentro del directorio seguro
   if FullPathNormalized.StartsWith(SafeDirNormalized + PathDelim) or
      SameText(FullPathNormalized, SafeDirNormalized) then
   begin
     ASafeFullPath := FullPath;
     Result := True;
     LogMessage(Format('Backup path validation successful: %s', [ASafeFullPath]), logDebug);
   end
   else
   begin
     LogMessage(Format('Security violation: Resolved path "%s" is outside safe directory "%s"',
       [FullPathNormalized, SafeDirNormalized]), logError);
     Result := False;
   end;
 except
   on E: Exception do
   begin
     LogMessage(Format('Error normalizing paths for security check: %s', [E.Message]), logError);
     Result := False;
   end;
 end;

 // Verificación adicional: el archivo no debe existir si es para crear un nuevo backup
 if Result and TFile.Exists(ASafeFullPath) then
 begin
   LogMessage(Format('Warning: Backup file already exists and will be overwritten: %s', [ASafeFullPath]), logWarning);
   // No fallar, pero advertir - SQL Server puede manejar la sobreescritura
 end;

 // Verificar permisos de escritura en el directorio
 if Result then
 begin
   try
     // Intentar crear un archivo temporal para verificar permisos
     var TempFileName := TPath.Combine(SafeBackupDir, Format('test_permissions_%s.tmp', [FormatDateTime('yyyymmddhhnnsszzz', Now)]));
     var TestStream := TFileStream.Create(TempFileName, fmCreate);
     try
       TestStream.WriteBuffer('test', 4);
     finally
       TestStream.Free;
       // Eliminar archivo temporal
       try
         TFile.Delete(TempFileName);
       except
         // Ignorar errores al eliminar archivo temporal
       end;
     end;
     LogMessage(Format('Write permissions verified for backup directory: %s', [SafeBackupDir]), logDebug);
   except
     on E: Exception do
     begin
       LogMessage(Format('No write permissions in backup directory %s: %s', [SafeBackupDir, E.Message]), logError);
       Result := False;
       ASafeFullPath := '';
     end;
   end;
 end;
end;

procedure TMSSQLConnection.BackupDatabase(const ABackupPath: string; ABackupType: Byte = 0; const ADatabaseName: string = ''; const ABackupName: string = '');
var
  SQL, BackupOption, DatabaseNameForSQL, BackupNameSQL, SafeFullPath: string;
  DBName: string;
begin
  DBName := ADatabaseName.Trim;
  if DBName.IsEmpty then
    DBName := Config.Database;
  if DBName.IsEmpty then
    raise EDBCommandError.Create('BackupDatabase: Database name not specified and not available from config.');

  // Validación robusta del path
  if not IsSafeBackupPath(ABackupPath, SafeFullPath) then
  begin
    LogMessage(Format('BackupDatabase: Invalid or unsafe backup path specified: "%s".', [ABackupPath]), logError);
    raise EDBCommandError.Create('Invalid or unsafe backup path specified. Only filenames are allowed, and backups are stored in a secure directory.');
  end;

  DatabaseNameForSQL := QuoteIdentifier(DBName, FDBType);

  case ABackupType of
    0: BackupOption := ''; // Full
    1: BackupOption := ' WITH DIFFERENTIAL';
    2: BackupOption := 'LOG'; // Para BACKUP LOG
  else
    raise EDBCommandError.Create('Invalid backup type specified. Use 0 for Full, 1 for Differential, 2 for Log.');
  end;

  BackupNameSQL := '';
  if ABackupName.Trim <> '' then // Nombre opcional para el backup set
    BackupNameSQL := Format(', NAME = N%s', [QuoteIdentifier(ABackupName, '''')]);

  if ABackupType = 2 then // Log backup
    SQL := Format('BACKUP LOG %s TO DISK = N%s WITH FORMAT, CHECKSUM%s;', [DatabaseNameForSQL, QuoteIdentifier(ABackupPath, ''''), BackupNameSQL])
  else // Full or Differential
    SQL := Format('BACKUP DATABASE %s TO DISK = N%s%s WITH FORMAT, CHECKSUM%s;',
                  [DatabaseNameForSQL, QuoteIdentifier(ABackupPath, ''''), BackupOption, BackupNameSQL]);

  Execute(SQL);
  LogMessage(Format('%s backup of database "%s" (BackupSet Name: "%s") completed to DISK = N''%s''.',
    [IfThen(ABackupType=2,'Log',IfThen(ABackupType=1,'Differential','Full')), DBName, IfThen(ABackupName.Trim<>'', ABackupName, 'Default'), ABackupPath]), logInfo);
end;

end.

