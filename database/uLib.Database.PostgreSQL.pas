unit uLib.Database.PostgreSQL;

interface

uses
  System.SysUtils, System.Classes, System.JSON, System.Variants, // Added System.Variants
  Data.DB, FireDAC.Comp.Client, FireDAC.Stan.Def, FireDAC.Stan.Intf,
  FireDAC.Stan.Async, FireDAC.Phys.Intf, FireDAC.DApt, FireDAC.Stan.Option,
  FireDAC.Phys.PG, FireDAC.Phys.PGDef,

  uLib.Database.Types,
  uLib.Database.Connection,
  uLib.Logger,
  uLib.Utils;

type
  TPostgreSQLConnection = class(TBaseConnection)
  private
    FDriverLink: TFDPhysPGDriverLink;
    var FDBType: Char;
    procedure ConfigureDriverLink;
    //function GetServerVersionInfoDetailed: string; // Renombrado de GetServerVersionInfo

  protected
    function GetDriverSpecificConnectionString: string; override;
    procedure ApplyDriverSpecificSettings; override;
    function GetVersion: string; override; // Usa SHOW server_version

  public
    constructor Create(const AConfig: TDBConnectionConfig; AMonitor: IDBMonitor = nil);
    destructor Destroy; override;

    // Schema Management
    function GetSchemaNames: TStrings; // Renombrado de GetSchemasAsStrings, Caller frees
    procedure CreateSchema(const ASchemaName: string; AIgnoreIfExists: Boolean = True);
    procedure DropSchema(const ASchemaName: string; ACascade: Boolean = False; AIgnoreIfNotExists: Boolean = True);

    // Table Management
    procedure AnalyzeTable(const ATableName: string; AVerbose: Boolean = False); // AVerbose default False
    procedure VacuumTable(const ATableName: string; AFull: Boolean = False; AAnalyze: Boolean = False; AVerbose: Boolean = False);
    procedure ReindexTable(const ATableName: string; AConcurrently: Boolean = False); // Added AConcurrently

    function GetTablePartitionsAsJSONArray(const ATableName: string): TJSONArray; // Renombrado

    // Statistics and Monitoring
    function GetTableStatsAsJSON(const ASchemaName: string = ''; const ATableName: string = ''): TJSONObject;
    function GetDatabaseStatsAsJSON: TJSONObject;
    function GetActivityStatsAsJSON(ACurrentDatabaseOnly: Boolean = True): TJSONObject; // Renombrado de GetPGActivityAsJSON
    function GetQueryExecutionStatsAsJSON(ATopN: Integer = 20): TJSONObject; // Renombrado de GetQueryStatsAsJSON
    function GetLocksAsJSONArray: TJSONArray;

    // Maintenance
    procedure VacuumAnalyzeDatabase(AVerbose: Boolean = False; AFull: Boolean = False); // Added AFull
    procedure ReindexDatabase(AConcurrently: Boolean = False); // Added AConcurrently
    procedure TerminateBackend(APID: Integer; ACancelQueryFirst: Boolean = False); // Renombrado de KillConnection

    // Replication
    function GetReplicationSlotStatsAsJSON: TJSONObject; // Renombrado de GetReplicationStatusAsJSON
  end;

implementation

uses
  FireDAC.Stan.Param,
  System.Generics.Collections,
  System.StrUtils, // Para IfThen, SameText, Format
  System.Math,     // Para Max
  System.DateUtils, // Para ISODateTimeToString (ya debería estar en uLib.Base o SysUtils)
  System.IOUtils,
  System.IniFiles;


{ TPostgreSQLConnection }

constructor TPostgreSQLConnection.Create(const AConfig: TDBConnectionConfig; AMonitor: IDBMonitor = nil);
begin
  inherited Create(AConfig, AMonitor);
  ConfigureDriverLink;
  LogMessage(Format('TPostgreSQLConnection created for config: %s', [AConfig.Name]), logInfo);
end;

destructor TPostgreSQLConnection.Destroy;
begin
  LogMessage(Format('TPostgreSQLConnection for config %s destroying...', [Config.Name]), logDebug);
  FreeAndNil(FDriverLink);
  LogMessage(Format('TPostgreSQLConnection for config %s destroyed.', [Config.Name]), logDebug);
  inherited;
end;

procedure TPostgreSQLConnection.ConfigureDriverLink;
var
  FIni: TMemIniFile;
begin
  try
   FIni:=TMemIniFile.Create('drivers.ini');
  except
   LogMessage( Format('TFDPhysPGDriverLink file drivers does not exist: %s',
                     ['drivers.ini']), logFatal);

  end;
  FDriverLink := TFDPhysPGDriverLink.Create(nil); // Sin owner, se libera en el destructor
  FDriverLink.Vendorhome := FIni.ReadString('Postgres','VendorHome','');
  LogMessage( Format('TFDPhysPGDriverLink instance created for TPostgreSQLConnection. VendorHome: %s',
                     [FDriverLink.VendorHome]), logDebug);

  FDriverLink.VendorLib := FIni.ReadString('Postgres','VendorLib','libpq.dll');
  LogMessage( Format('TFDPhysPGDriverLink instance created for TPostgreSQLConnection. VendorLib: %s',
                    [FDriverLink.VendorLib]), logDebug);
  FIni.Free;
end;

function TPostgreSQLConnection.GetDriverSpecificConnectionString: string;
var
  Params: TStringList;
  sParams,
  SearchPath: string;
begin
  FDBType:='"';
  Params := TStringList.Create;
  try
    Params.Add('DriverID=PG'); // Identificador de FireDAC para PostgreSQL
    Params.Add(Format('Server=%s', [Config.Server]));
    if Config.Port > 0 then // Puerto estándar de PG es 5432
       Params.Add(Format('Port=%d', [Config.Port]));
    Params.Add(Format('Database=%s', [Config.Database]));
    Params.Add(Format('User_Name=%s', [Config.Username]));
    Params.Add(Format('Password=%s', [Config.Password]));

    Params.Add(Format('ApplicationName=%s', [ IfThen( Config.ApplicationName.Trim <> '',
                                                      Config.ApplicationName,
                                                      'DelphiAppServer')]));
    // Construir SearchPath: Config.Schema primero (si existe), luego public
    if Config.Schema.Trim <> '' then
       SearchPath := Config.Schema.Trim //  QuoteIdentifier(Config.Schema.Trim, FDBType) + ',public'
    else
       SearchPath := 'public';
    Params.Add(Format('SearchPath=%s', [SearchPath]));

    if Config.SSL then
    begin
      // SSLMode para PostgreSQL: disable, allow, prefer, require, verify-ca, verify-full
      Params.Add(Format('SSLMode=%s', [GetStrPair(Config.Params, 'PGSSLMode', 'prefer')]));
      if Config.SSLCert.Trim <> '' then Params.Add(Format('SSLCert=%s', [Config.SSLCert]));
      if Config.SSLKey.Trim <> '' then Params.Add(Format('SSLKey=%s', [Config.SSLKey]));
      if Config.SSLRootCert.Trim <> '' then Params.Add(Format('SSLRootCA=%s', [Config.SSLRootCert]));
      // Otros params SSL de PG: sslcompression, sslcrl, etc. pueden ir en Config.Params
    end;

    // Connection timeouts son manejados por TBaseConnection.ResourceOptions
    // Statement timeout (statement_timeout) se establece en ApplyDriverSpecificSettings

    if Config.Compress then // Compresión de protocolo (si el servidor PG y el driver lo soportan)
      Params.Add('Compression=True'); // FireDAC puede que no soporte este parámetro directamente; verificar documentación

//    if Config.Params.Trim <> '' then
//       Params.Add(Config.Params.Trim); // Añadir parámetros adicionales

    Result := ReplaceText(Params.Text,#$D#$A,';');
    LogMessage(Format('PostgreSQL Connection String (password omitted): Server=%s;Database=%s;User_Name=%s;SearchPath=%s;...',
      [Config.Server, Config.Database, Config.Username, SearchPath]), logDebug);
  finally
    Params.Free;
  end;
end;

procedure TPostgreSQLConnection.ApplyDriverSpecificSettings;
var
  SearchPathSQL: string;
  LockTimeoutMs, StmtTimeoutMs: Integer;
begin
  inherited;
  LogMessage(Format('Applying PostgreSQL specific settings for connection "%s"...', [Config.Name]), logDebug);
  try
    Execute(Format('SET application_name = %s;', [ IfThen( Config.ApplicationName.Trim <> '',
                                                   Config.ApplicationName,
                                                   'DelphiAppServer')]));
    Execute('SET timezone = ''UTC'';'); // Es buena práctica usar UTC en el servidor
    Execute('SET client_encoding = ''UTF8'';');
    Execute('SET DateStyle = ''ISO, YMD'';'); // Formato de fecha estándar

    // Configurar timeouts de sesión
    StmtTimeoutMs := Config.CommandTimeout * 1000; // CommandTimeout está en segundos
    if StmtTimeoutMs <= 0 then StmtTimeoutMs := 30000; // Default a 30s si no está configurado
    Execute(Format('SET statement_timeout = %d;', [StmtTimeoutMs]));

    LockTimeoutMs := Max(1000, StmtTimeoutMs div 3); // Ej: 1/3 del statement_timeout, mínimo 1s
    Execute(Format('SET lock_timeout = %d;', [LockTimeoutMs]));

    // SearchPath ya se establece en la connection string, pero se puede reconfirmar o ajustar aquí si es necesario.
    // if Config.Schema.Trim <> '' then
    //   SearchPathSQL := Format('SET search_path = %s, public;', [QuoteIdentifier(Config.Schema.Trim, FDBType)])
    // else
    //   SearchPathSQL := 'SET search_path = public;';
    // Execute(SearchPathSQL);
    LogMessage(Format('PostgreSQL session settings applied: statement_timeout=%dms, lock_timeout=%dms.', [StmtTimeoutMs, LockTimeoutMs]), logDebug);
  except
    on E: Exception do
      LogMessage(Format('Error applying PostgreSQL specific settings for connection "%s": %s - %s.',
        [Config.Name, E.ClassName, E.Message]), logError);
  end;
end;

function TPostgreSQLConnection.GetVersion: string;
begin
  try
    Result := VarToStr(ExecuteScalar('SELECT current_setting(''server_version'')')); // SHOW server_version es estándar en PG
  except
    on E: Exception do
    begin
      LogMessage(Format('Error getting PostgreSQL server_version for config "%s": %s. Falling back to base GetVersion.', [Config.Name, E.Message]), logError);
      Result := inherited GetVersion; // Llama a TBaseConnection.GetVersion
    end;
  end;
end;

(*
function TPostgreSQLConnection.GetServerVersionInfoDetailed: string;
begin
  Result := VarToStr(ExecuteScalar('SELECT version();')); // version() da una cadena más detallada
end;
*)

function TPostgreSQLConnection.GetSchemaNames: TStrings;
const
  SQL_GET_SCHEMAS = 'SELECT schema_name FROM information_schema.schemata ' +
                    'WHERE schema_name NOT LIKE ''pg_%'' AND schema_name <> ''information_schema'' ORDER BY schema_name;';
var
  LQuery: TDataSet;
begin
  Result := TStringList.Create; // El llamador debe liberar esto
  LQuery := nil;
  try
    LQuery := ExecuteReader(SQL_GET_SCHEMAS);
    while not LQuery.Eof do
    begin
      Result.Add(LQuery.Fields[0].AsString);
      LQuery.Next;
    end;
  finally
    FreeAndNil(LQuery);
  end;
end;

procedure TPostgreSQLConnection.CreateSchema(const ASchemaName: string; AIgnoreIfExists: Boolean = True);
var
  SQL: string;
begin
  if ASchemaName.Trim = '' then
    raise EDBCommandError.Create('CreateSchema: Schema name cannot be empty.');

  SQL := 'CREATE SCHEMA ';
  if AIgnoreIfExists then SQL := SQL + 'IF NOT EXISTS ';
  SQL := SQL + QuoteIdentifier(ASchemaName, FDBType) + ';'; // FDBType para el entrecomillado correcto
  Execute(SQL);
  LogMessage(Format('Schema "%s" %s.', [ASchemaName, IfThen(AIgnoreIfExists, 'created or already exists', 'creation attempted')]), logInfo);
end;

procedure TPostgreSQLConnection.DropSchema(const ASchemaName: string; ACascade: Boolean = False; AIgnoreIfNotExists: Boolean = True);
var
  SQL: string;
begin
  if ASchemaName.Trim = '' then
    raise EDBCommandError.Create('DropSchema: Schema name cannot be empty.');
  if SameText(ASchemaName, 'public') then // Prevenir borrado accidental de 'public'
    raise EDBCommandError.Create('DropSchema: Dropping the "public" schema is not allowed through this method.');

  SQL := 'DROP SCHEMA ';
  if AIgnoreIfNotExists then SQL := SQL + 'IF EXISTS ';
  SQL := SQL + QuoteIdentifier(ASchemaName, FDBType);
  if ACascade then SQL := SQL + ' CASCADE';
  SQL := SQL + ';';
  Execute(SQL);
  LogMessage(Format('Schema "%s" dropped%s.', [ASchemaName, IfThen(ACascade, ' with cascade', '')]), logInfo);
end;

procedure TPostgreSQLConnection.AnalyzeTable(const ATableName: string; AVerbose: Boolean = False);
begin
  if ATableName.Trim = '' then
    raise EDBCommandError.Create('AnalyzeTable: Table name cannot be empty.');
  Execute(Format('ANALYZE%s %s;', [IfThen(AVerbose, ' VERBOSE', ''), QuoteIdentifier(ATableName, FDBType)]));
  LogMessage(Format('Table "%s" analyzed%s.', [ATableName, IfThen(AVerbose, ' (verbose)','')]), logInfo);
end;

procedure TPostgreSQLConnection.VacuumTable(const ATableName: string; AFull: Boolean = False; AAnalyze: Boolean = False; AVerbose: Boolean = False);
var
  Options: TStringList;
  SQL: string;
begin
  if ATableName.Trim = '' then
    raise EDBCommandError.Create('VacuumTable: Table name cannot be empty.');

  Options := TStringList.Create;
  try
    if AFull then Options.Add('FULL');
    if AAnalyze then Options.Add('ANALYZE');
    if AVerbose then Options.Add('VERBOSE');

    if Options.Count > 0 then
      SQL := Format('VACUUM (%s) %s;', [Options.CommaText, QuoteIdentifier(ATableName, FDBType)])
    else
      SQL := Format('VACUUM %s;', [QuoteIdentifier(ATableName, FDBType)]);
    Execute(SQL);
    LogMessage(Format('VACUUM (%s) performed on table "%s".', [IfThen(Options.Count > 0, Options.CommaText, 'Default'), ATableName]), logInfo);
  finally
    Options.Free;
  end;
end;

procedure TPostgreSQLConnection.ReindexTable(const ATableName: string; AConcurrently: Boolean = False);
var
  SQL: string;
begin
  if ATableName.Trim = '' then
    raise EDBCommandError.Create('ReindexTable: Table name cannot be empty.');
  // REINDEX CONCURRENTLY (PostgreSQL 12+) no bloquea escrituras, pero es más lento y consume más recursos.
  SQL := Format('REINDEX%s TABLE %s;', [IfThen(AConcurrently, ' (CONCURRENTLY)', ''), QuoteIdentifier(ATableName, FDBType)]);
  Execute(SQL);
  LogMessage(Format('Table "%s" reindexed%s.', [ATableName, IfThen(AConcurrently, ' concurrently','')]), logInfo);
end;

function TPostgreSQLConnection.GetTablePartitionsAsJSONArray(const ATableName: string): TJSONArray;
const
  SQL_GET_PARTITIONS =
    'SELECT inhrelid::regclass AS partition_name, pg_get_expr(c.relpartbound, c.oid, true) AS partition_expression ' +
    'FROM pg_class c JOIN pg_inherits i ON c.oid = i.inhrelid ' +
    'WHERE i.inhparent = %s::regclass AND c.relispartition ORDER BY partition_name;'; // Usar %s para el nombre de tabla
var
  LQuery: TDataSet;
  LPartitionObject: TJSONObject;
  FormattedSQL: string;
begin
  Result := TJSONArray.Create; // El llamador es responsable de liberar este TJSONArray
  LQuery := nil;
  if ATableName.Trim = '' then
  begin
    LogMessage('GetTablePartitionsAsJSONArray: Table name cannot be empty.', logWarning);
    Exit;
  end;

  // El nombre de la tabla debe ser entrecomillado y escapado correctamente si se interpola.
  // Es más seguro si se puede parametrizar, pero ::regclass espera un literal.
  // Aquí, QuoteIdentifier se usa para el nombre de la tabla antes de insertarlo en el string SQL.
  FormattedSQL := Format(SQL_GET_PARTITIONS, [QuoteIdentifier(ATableName, FDBType).QuotedString]);

  try
    LQuery := ExecuteReader(FormattedSQL); // No hay parámetros de FireDAC aquí, el nombre de la tabla está en el SQL
    while not LQuery.Eof do
    begin
      LPartitionObject := TJSONObject.Create;
      LPartitionObject.AddPair('partition_name', LQuery.FieldByName('partition_name').AsString);
      LPartitionObject.AddPair('partition_expression', LQuery.FieldByName('partition_expression').AsString);
      Result.Add(LPartitionObject); // Result (TJSONArray) toma posesión de LPartitionObject
      LQuery.Next;
    end;
  finally
    FreeAndNil(LQuery);
  end;
end;

function TPostgreSQLConnection.GetTableStatsAsJSON(const ASchemaName: string = ''; const ATableName: string = ''): TJSONObject;
var
  SQL: string;
  Params: TFDParams;
  WhereClauses: TStringList;
  LQuery: TDataSet;
  LJSONArray: TJSONArray;
  LStatObject: TJSONObject;
begin
  Result := TJSONObject.Create;
  LJSONArray := TJSONArray.Create;
  Result.AddPair('table_stats', LJSONArray);
  LQuery := nil;
  WhereClauses := TStringList.Create;
  Params:=TFDParams.Create;
  try
    SQL := 'SELECT schemaname, relname, n_live_tup, n_dead_tup, n_mod_since_analyze, seq_scan, idx_scan, ' +
           'last_vacuum, last_autovacuum, last_analyze, last_autoanalyze, ' +
           'vacuum_count, autovacuum_count, analyze_count, autoanalyze_count ' +
           'FROM pg_stat_user_tables ';

    if ATableName.Trim <> '' then
    begin
      WhereClauses.Add(QuoteIdentifier('relname', FDBType) + ' = :relname');
      Params.Add('relname',ATableName.Trim);
    end;
    if ASchemaName.Trim <> '' then
    begin
      WhereClauses.Add(QuoteIdentifier('schemaname', FDBType) + ' = :schemaname');
      Params.Add('schemaname',ASchemaName.Trim);
    end;

    if WhereClauses.Count > 0 then
      SQL := SQL + 'WHERE ' + WhereClauses.Text; // .Text une con AND por defecto

    SQL := SQL + ' ORDER BY schemaname, relname;';

    LQuery := ExecuteReader(SQL, Params); // Convertir TList<Variant> a TArray<Variant>
    while not LQuery.Eof do
    begin
      LStatObject := TJSONObject.Create;
      LStatObject.AddPair('schema_name', LQuery.FieldByName('schemaname').AsString);
      LStatObject.AddPair('table_name', LQuery.FieldByName('relname').AsString);
      LStatObject.AddPair('live_tuples', LQuery.FieldByName('n_live_tup').AsLargeInt);
      LStatObject.AddPair('dead_tuples', LQuery.FieldByName('n_dead_tup').AsLargeInt);
      LStatObject.AddPair('modified_since_analyze', LQuery.FieldByName('n_mod_since_analyze').AsLargeInt);
      LStatObject.AddPair('sequential_scans', LQuery.FieldByName('seq_scan').AsLargeInt);
      LStatObject.AddPair('index_scans', LQuery.FieldByName('idx_scan').AsLargeInt);
      LStatObject.AddPair('last_manual_vacuum_utc', DateToISO8601(LQuery.FieldByName('last_vacuum').AsDateTime));
      LStatObject.AddPair('last_autovacuum_utc', DateToISO8601(LQuery.FieldByName('last_autovacuum').AsDateTime));
      LStatObject.AddPair('last_manual_analyze_utc', DateToISO8601(LQuery.FieldByName('last_analyze').AsDateTime));
      LStatObject.AddPair('last_autoanalyze_utc', DateToISO8601(LQuery.FieldByName('last_autoanalyze').AsDateTime));
      LStatObject.AddPair('manual_vacuum_count', LQuery.FieldByName('vacuum_count').AsLargeInt);
      LStatObject.AddPair('autovacuum_count', LQuery.FieldByName('autovacuum_count').AsLargeInt);
      LStatObject.AddPair('manual_analyze_count', LQuery.FieldByName('analyze_count').AsLargeInt);
      LStatObject.AddPair('autoanalyze_count', LQuery.FieldByName('autoanalyze_count').AsLargeInt);
      LJSONArray.Add(LStatObject);
      LQuery.Next;
    end;
  finally
    FreeAndNil(LQuery);
    FreeAndNil(Params);
    FreeAndNil(WhereClauses);
  end;
end;

function TPostgreSQLConnection.GetDatabaseStatsAsJSON: TJSONObject;
const
  SQL_DB_STATS =
    'SELECT numbackends, xact_commit, xact_rollback, blks_read, blks_hit, ' +
    'tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted, conflicts, ' +
    'temp_files, temp_bytes, deadlocks, checksum_failures, checksum_last_failure, stats_reset ' +
    'FROM pg_stat_database WHERE datname = current_database();';
var
  LQuery: TDataSet;
begin
  Result := TJSONObject.Create;
  LQuery := nil;
  try
    LQuery := ExecuteReader(SQL_DB_STATS);
    if not LQuery.Eof then
    begin
      Result.AddPair('active_backends', LQuery.FieldByName('numbackends').AsInteger);
      Result.AddPair('transactions_committed', LQuery.FieldByName('xact_commit').AsLargeInt);
      Result.AddPair('transactions_rolled_back', LQuery.FieldByName('xact_rollback').AsLargeInt);
      Result.AddPair('blocks_read_from_disk', LQuery.FieldByName('blks_read').AsLargeInt);
      Result.AddPair('blocks_hit_in_cache', LQuery.FieldByName('blks_hit').AsLargeInt);
      Result.AddPair('tuples_returned_by_queries', LQuery.FieldByName('tup_returned').AsLargeInt);
      Result.AddPair('tuples_fetched_by_queries', LQuery.FieldByName('tup_fetched').AsLargeInt);
      Result.AddPair('tuples_inserted', LQuery.FieldByName('tup_inserted').AsLargeInt);
      Result.AddPair('tuples_updated', LQuery.FieldByName('tup_updated').AsLargeInt);
      Result.AddPair('tuples_deleted', LQuery.FieldByName('tup_deleted').AsLargeInt);
      Result.AddPair('query_conflicts', LQuery.FieldByName('conflicts').AsLargeInt);
      Result.AddPair('temp_files_created', LQuery.FieldByName('temp_files').AsLargeInt);
      Result.AddPair('temp_bytes_written', LQuery.FieldByName('temp_bytes').AsLargeInt);
      Result.AddPair('deadlocks_detected', LQuery.FieldByName('deadlocks').AsLargeInt);
      Result.AddPair('checksum_failures', LQuery.FieldByName('checksum_failures').AsLargeInt);
      Result.AddPair('last_checksum_failure_utc', DateToISO8601(LQuery.FieldByName('checksum_last_failure').AsDateTime));
      Result.AddPair('stats_last_reset_utc', DateToISO8601(LQuery.FieldByName('stats_reset').AsDateTime));
    end;
    Result.AddPair('database_size_pretty', VarToStr(ExecuteScalar('SELECT pg_size_pretty(pg_database_size(current_database()));')));
  finally
    FreeAndNil(LQuery);
  end;
end;

function TPostgreSQLConnection.GetActivityStatsAsJSON(ACurrentDatabaseOnly: Boolean = True): TJSONObject;
var
  SQL_PG_ACTIVITY: string;
  LQuery: TDataSet;
  LJSONArray: TJSONArray;
  LActivityObject: TJSONObject;
begin
  Result := TJSONObject.Create;
  LJSONArray := TJSONArray.Create;
  Result.AddPair('pg_stat_activity', LJSONArray);
  LQuery := nil;

  SQL_PG_ACTIVITY :=
    'SELECT datname, pid, usename, application_name, client_addr, client_port, backend_start, xact_start, query_start, state_change, wait_event_type, wait_event, state, backend_xid, backend_xmin, query ' +
    'FROM pg_stat_activity';
  if ACurrentDatabaseOnly then
    SQL_PG_ACTIVITY := SQL_PG_ACTIVITY + ' WHERE datname = current_database()';
  SQL_PG_ACTIVITY := SQL_PG_ACTIVITY + ' ORDER BY pid;';

  try
    LQuery := ExecuteReader(SQL_PG_ACTIVITY);
    while not LQuery.Eof do
    begin
      LActivityObject := TJSONObject.Create;
      LActivityObject.AddPair('database_name', LQuery.FieldByName('datname').AsString);
      LActivityObject.AddPair('pid', LQuery.FieldByName('pid').AsInteger);
      LActivityObject.AddPair('user_name', LQuery.FieldByName('usename').AsString);
      LActivityObject.AddPair('application_name', LQuery.FieldByName('application_name').AsString);
      LActivityObject.AddPair('client_address', LQuery.FieldByName('client_addr').AsString);
      LActivityObject.AddPair('client_port', LQuery.FieldByName('client_port').AsInteger);
      LActivityObject.AddPair('backend_start_utc', DateToISO8601(LQuery.FieldByName('backend_start').AsDateTime));
      LActivityObject.AddPair('transaction_start_utc', DateToISO8601(LQuery.FieldByName('xact_start').AsDateTime));
      LActivityObject.AddPair('query_start_utc', DateToISO8601(LQuery.FieldByName('query_start').AsDateTime));
      LActivityObject.AddPair('state_change_utc', DateToISO8601(LQuery.FieldByName('state_change').AsDateTime));
      LActivityObject.AddPair('wait_event_type', LQuery.FieldByName('wait_event_type').AsString);
      LActivityObject.AddPair('wait_event', LQuery.FieldByName('wait_event').AsString);
      LActivityObject.AddPair('state', LQuery.FieldByName('state').AsString);
      LActivityObject.AddPair('query_snippet', Copy(LQuery.FieldByName('query').AsString, 1, 200)); // Snippet
      LJSONArray.Add(LActivityObject);
      LQuery.Next;
    end;
  finally
    FreeAndNil(LQuery);
  end;
end;

function TPostgreSQLConnection.GetQueryExecutionStatsAsJSON(ATopN: Integer = 20): TJSONObject;
var
  SQL_QUERY_STATS: string;
  LQuery: TDataSet;
  LJSONArray: TJSONArray;
  LStatObject: TJSONObject;
  ActualLimit: Integer;
  Params: TFDParams;
begin
  Result := TJSONObject.Create;
  LJSONArray := TJSONArray.Create;
  Result.AddPair('top_queries_by_total_time', LJSONArray);
  LQuery := nil;
  ActualLimit := Max(1, ATopN);
  // pg_stat_statements debe estar habilitado en postgresql.conf y la extensión creada.
  SQL_QUERY_STATS :=
    'SELECT queryid, query, calls, total_time, rows, shared_blks_hit, shared_blks_read, ' + // total_time en PG13+, total_exec_time en PG12-
    '  local_blks_hit, local_blks_read, temp_blks_read, temp_blks_written ' +
    'FROM pg_stat_statements ORDER BY total_time DESC LIMIT :Limit;'; // Usar total_time (PG13+) o total_exec_time (PG12-)
                                                                 // Para compatibilidad, se podría verificar la versión de PG y ajustar el nombre del campo.
                                                                 // Por ahora, se asume PG13+ o que el campo se llame total_time.
  Params:=TFDParams.Create;
  Params.Add('Limit',ActualLimit);
  try
    // Intentar crear la extensión si no existe (requiere privilegios de superusuario la primera vez)
    try
      Execute('CREATE EXTENSION IF NOT EXISTS pg_stat_statements;');
    except
      on E:Exception do
         LogMessage('Could not ensure pg_stat_statements extension: '+E.Message +
                    '. Query stats may not be available.', logWarning);
    end;

    LQuery := ExecuteReader(SQL_QUERY_STATS, params);
    while not LQuery.Eof do
    begin
      LStatObject := TJSONObject.Create;
      LStatObject.AddPair('query_id', LQuery.FieldByName('queryid').AsLargeInt); // Podría ser string en algunas versiones
      LStatObject.AddPair('query_text_snippet', Copy(LQuery.FieldByName('query').AsString, 1, 250));
      LStatObject.AddPair('calls', LQuery.FieldByName('calls').AsLargeInt);
      LStatObject.AddPair('total_execution_time_ms', LQuery.FieldByName('total_time').AsFloat); // Asumiendo total_time
      LStatObject.AddPair('rows_returned', LQuery.FieldByName('rows').AsLargeInt);
      LStatObject.AddPair('shared_blocks_hit', LQuery.FieldByName('shared_blks_hit').AsLargeInt);
      LStatObject.AddPair('shared_blocks_read', LQuery.FieldByName('shared_blks_read').AsLargeInt);
      LStatObject.AddPair('local_blocks_hit', LQuery.FieldByName('local_blks_hit').AsLargeInt);
      LStatObject.AddPair('local_blocks_read', LQuery.FieldByName('local_blks_read').AsLargeInt);
      LStatObject.AddPair('temp_blocks_read', LQuery.FieldByName('temp_blks_read').AsLargeInt);
      LStatObject.AddPair('temp_blocks_written', LQuery.FieldByName('temp_blks_written').AsLargeInt);
      LJSONArray.Add(LStatObject);
      LQuery.Next;
    end;
  finally
    FreeAndNil(LQuery);
    FreeAndNil(Params);
  end;
end;

function TPostgreSQLConnection.GetLocksAsJSONArray: TJSONArray;
const
  SQL_LOCKS = // Query más detallada para locks
    'SELECT ' +
    '  locktype, ' +
    '  database::regodatabase AS database_name, ' + // Nombre de la BD
    '  relation::regclass AS relation_name, ' +     // Nombre de la tabla/objeto
    '  page, tuple, virtualxid, transactionid, classid, objid, objsubid, ' +
    '  virtualtransaction, pid, mode, granted, fastpath, ' +
    '  (SELECT query FROM pg_stat_activity WHERE pid = pg_locks.pid) AS holding_query ' + // Query que mantiene el lock
    'FROM pg_locks WHERE database = (SELECT oid FROM pg_database WHERE datname = current_database());';
var
  LQuery: TDataSet;
  LLockObject: TJSONObject;
begin
  Result := TJSONArray.Create;
  LQuery := nil;
  try
    LQuery := ExecuteReader(SQL_LOCKS);
    while not LQuery.Eof do
    begin
      LLockObject := TJSONObject.Create;
      LLockObject.AddPair('lock_type', LQuery.FieldByName('locktype').AsString);
      LLockObject.AddPair('database_name', LQuery.FieldByName('database_name').AsString);
      LLockObject.AddPair('relation_name', LQuery.FieldByName('relation_name').AsString); // Puede ser NULL
      LLockObject.AddPair('page', LQuery.FieldByName('page').AsInteger); // Puede ser NULL
      LLockObject.AddPair('tuple', LQuery.FieldByName('tuple').AsInteger); // Puede ser NULL
      LLockObject.AddPair('pid', LQuery.FieldByName('pid').AsInteger);
      LLockObject.AddPair('mode', LQuery.FieldByName('mode').AsString);
      LLockObject.AddPair('granted', LQuery.FieldByName('granted').AsBoolean);
      LLockObject.AddPair('fastpath', LQuery.FieldByName('fastpath').AsBoolean);
      LLockObject.AddPair('holding_query_snippet', Copy(LQuery.FieldByName('holding_query').AsString, 1, 100));
      Result.Add(LLockObject);
      LQuery.Next;
    end;
  finally
    FreeAndNil(LQuery);
  end;
end;

procedure TPostgreSQLConnection.VacuumAnalyzeDatabase(AVerbose: Boolean = False; AFull: Boolean = False);
var
  Options: TStringList;
  SQL: string;
begin
  Options := TStringList.Create;
  try
    Options.Add('ANALYZE'); // Siempre analizar
    if AVerbose then Options.Add('VERBOSE');
    if AFull then Options.Add('FULL');

    SQL := Format('VACUUM (%s);', [Options.CommaText]);
    Execute(SQL);
    LogMessage(Format('VACUUM (%s) completed for database "%s".',
         [Options.CommaText, Config.Database]), logInfo);
  finally
    Options.Free;
  end;
end;

procedure TPostgreSQLConnection.ReindexDatabase(AConcurrently: Boolean = False);
begin
  // REINDEX DATABASE puede solo ser ejecutado por superusuario o dueño de la BD.
  // REINDEX DATABASE CONCURRENTLY (PG 12+)
  Execute(Format('REINDEX%s DATABASE %s;', [IfThen(AConcurrently, ' (CONCURRENTLY)', ''), QuoteIdentifier(Config.Database, FDBType)]));
  LogMessage(Format('Database "%s" reindexed%s.', [Config.Database, IfThen(AConcurrently, ' concurrently','')]), logWarning);
end;

procedure TPostgreSQLConnection.TerminateBackend(APID: Integer; ACancelQueryFirst: Boolean = False);
var
  Params: TFDParams;
  SQL: string;
begin
  if APID <= 0 then
    raise EDBCommandError.Create('Invalid PID for backend termination.');
  Params:=TFDParams.Create;
  Params.Add('APID',APID);
  if ACancelQueryFirst then // Intentar cancelar la consulta primero
  begin
    SQL := 'SELECT pg_cancel_backend(:APID);';
    try
      if ExecuteScalar(SQL, params) then // pg_cancel_backend devuelve boolean
        LogMessage(Format('Attempted to cancel query for backend PID: %d. Result: Success.', [APID]), logInfo)
      else
        LogMessage(Format('Attempted to cancel query for backend PID: %d. Result: Failed (PID not found or query already finished).', [APID]), logWarning);
      Sleep(500); // Dar un pequeño tiempo para que la cancelación surta efecto
    except
      on E: Exception do
        LogMessage(Format('Error trying to cancel query for PID %d: %s. Proceeding to terminate.', [APID, E.Message]), logWarning);
    end;
  end;

  // Terminar el backend
  SQL := 'SELECT pg_terminate_backend(:APID);';
  if ExecuteScalar(SQL, params) then // pg_terminate_backend devuelve boolean
    LogMessage(Format('Backend PID %d termination requested. Result: Success.', [APID]), logWarning)
  else
    LogMessage(Format('Backend PID %d termination request failed (PID not found or already terminated).', [APID]), logWarning);
  Params.Free;
end;

function TPostgreSQLConnection.GetReplicationSlotStatsAsJSON: TJSONObject;
const
  SQL_REPLICATION_SLOTS = // Para slots físicos y lógicos
    'SELECT slot_name, plugin, slot_type, datoid::regdatabase as database_name, temporary, active, active_pid, ' +
    '  xmin, catalog_xmin, restart_lsn, confirmed_flush_lsn, ' +
    '  pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS restart_lag_bytes, ' + // Lag desde el LSN de reinicio
    '  pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS flush_lag_bytes ' + // Lag desde el LSN confirmado
    'FROM pg_replication_slots;';
var
  LQuery: TDataSet;
  LJSONArray: TJSONArray;
  LSlotObject: TJSONObject;
begin
  Result := TJSONObject.Create;
  LJSONArray := TJSONArray.Create;
  Result.AddPair('replication_slots', LJSONArray);
  LQuery := nil;
  try
    LQuery := ExecuteReader(SQL_REPLICATION_SLOTS);
    while not LQuery.Eof do
    begin
      LSlotObject := TJSONObject.Create;
      LSlotObject.AddPair('slot_name', LQuery.FieldByName('slot_name').AsString);
      LSlotObject.AddPair('plugin', LQuery.FieldByName('plugin').AsString); // NULL para slots físicos
      LSlotObject.AddPair('slot_type', LQuery.FieldByName('slot_type').AsString);
      LSlotObject.AddPair('database_name', LQuery.FieldByName('database_name').AsString); // NULL para slots físicos
      LSlotObject.AddPair('is_temporary', LQuery.FieldByName('temporary').AsBoolean);
      LSlotObject.AddPair('is_active', LQuery.FieldByName('active').AsBoolean);
      LSlotObject.AddPair('active_pid', LQuery.FieldByName('active_pid').AsInteger); // Puede ser NULL
      LSlotObject.AddPair('restart_lsn', LQuery.FieldByName('restart_lsn').AsString); // pg_lsn
      LSlotObject.AddPair('confirmed_flush_lsn', LQuery.FieldByName('confirmed_flush_lsn').AsString); // pg_lsn
      LSlotObject.AddPair('restart_lag_bytes', LQuery.FieldByName('restart_lag_bytes').AsLargeInt); // Puede ser NULL si LSN es NULL
      LSlotObject.AddPair('flush_lag_bytes', LQuery.FieldByName('flush_lag_bytes').AsLargeInt); // Puede ser NULL
      LJSONArray.Add(LSlotObject);
      LQuery.Next;
    end;
  finally
    FreeAndNil(LQuery);
  end;
end;

end.

