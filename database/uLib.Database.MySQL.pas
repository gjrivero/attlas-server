unit uLib.Database.MySQL;

interface

uses
  System.SysUtils, System.Classes, System.JSON, System.RegularExpressions, System.Variants, // Added Variants
  Data.DB, FireDAC.Comp.Client, FireDAC.Stan.Def, FireDAC.Stan.Intf,
  FireDAC.Stan.Async, FireDAC.Phys.Intf, FireDAC.DApt, FireDAC.Stan.Option,
  FireDAC.Phys.MySQL, FireDAC.Phys.MySQLDef,

  uLib.Database.Types,
  uLib.Database.Connection,
  uLib.Logger;

type
  TMySQLConnection = class(TBaseConnection)
  private
    FDriverLink: TFDPhysMySQLDriverLink;
    var FDBType: Char;
    procedure ConfigureDriverLink;
    function GetInnoDBStatusRaw: string; // Renombrado para indicar que devuelve el string crudo

  protected
    function GetDriverSpecificConnectionString: string; override;
    procedure ApplyDriverSpecificSettings; override;
    function GetVersion: string; override;

  public
    constructor Create(const AConfig: TDBConnectionConfig; AMonitor: IDBMonitor = nil);
    destructor Destroy; override;

    // Operaciones específicas de MySQL
    procedure OptimizeTable(const ATableName: string);
    procedure AnalyzeTable(const ATableName: string);
    procedure RepairTable(const ATableName: string; AExtended: Boolean = False); // MyISAM specific, added AExtended
    function GetTableStatusAsJSONArray(const ADatabaseName: string = ''): TJSONArray; // Renombrado y devuelve TJSONArray
    function GetProcessListAsJSONArray: TJSONArray;
    procedure KillMySQLProcess(AProcessId: Integer); // Renombrado de KillProcess

    // Mantenimiento y Variables
    procedure FlushTables(AWithReadLock: Boolean = False; const ATableNames: TArray<string> = nil); // Added ATableNames
    function GetGlobalVariablesAsJSON: TJSONObject; // Renombrado y devuelve JSON
    function GetSessionVariablesAsJSON: TJSONObject; // Renombrado y devuelve JSON
    procedure SetSessionVariable(const AVariableName: string; const AValue: string);
    function GetEngineStatusAsJSON(const AEngineName: string = 'InnoDB'): TJSONObject;
  end;

implementation

uses
  System.StrUtils, // For IfThen, SameText, Format
  System.DateUtils, // Para ISODateTimeToString (si se necesitara para fechas en JSON)
  System.IniFiles,
  FireDac.Stan.Param,

  uLib.Utils;

{ TMySQLConnection }

constructor TMySQLConnection.Create(const AConfig: TDBConnectionConfig; AMonitor: IDBMonitor = nil);
begin
  inherited Create(AConfig, AMonitor);
  FDBType:='`';
  ConfigureDriverLink;
  LogMessage(Format('TMySQLConnection created for config: %s', [AConfig.Name]), logInfo);
end;

destructor TMySQLConnection.Destroy;
begin
  LogMessage(Format('TMySQLConnection for config %s destroying...', [Config.Name]), logDebug);
  FreeAndNil(FDriverLink);
  LogMessage(Format('TMySQLConnection for config %s destroyed.', [Config.Name]), logDebug);
  inherited;
end;

procedure TMySQLConnection.ConfigureDriverLink;
var
  FIni: TMemIniFile;
begin
  try
   FIni:=TMemIniFile.Create('drivers.ini');
  except
   LogMessage( Format('TFDPhysMySQLDriverLink file drivers does not exist: %s',
                     ['drivers.ini']), logFatal);
  end;
  FDriverLink := TFDPhysMySQLDriverLink.Create(nil); // Sin owner, se libera en el destructor
  FDriverLink.Vendorhome := FIni.ReadString('MySQL','VendorHome','');
  LogMessage( Format('TFDPhysMySQLDriverLink instance created for TMySQLConnection. VendorHome: %s',
                     [FDriverLink.VendorHome]), logDebug);

  FDriverLink.VendorLib := FIni.ReadString('MySQL','VendorLib','libpq.dll');
  LogMessage(Format('TFDPhysMySQLDriverLink instance created for TMySQLConnection. VendorLib: %s',
              [FDriverLink.VendorLib]), logDebug);
  FIni.Free;
  // Ejemplo: "VendorLibWin=libmysql.dll;VendorLibLinux=libmysqlclient.so"
end;

function TMySQLConnection.GetDriverSpecificConnectionString: string;
var
  Params: TStringList;
begin
  Params := TStringList.Create;
  try
    Params.Add('DriverID=MySQL');
    Params.Add(Format('Server=%s', [Config.Server]));
    if Config.Port > 0 then // Puerto estándar de MySQL es 3306
      Params.Add(Format('Port=%d', [Config.Port]));
    Params.Add(Format('Database=%s', [Config.Database]));
    Params.Add(Format('User_Name=%s', [Config.Username]));
    Params.Add(Format('Password=%s', [Config.Password]));

    Params.Add(Format('ApplicationName=%s', [IfThen(Config.ApplicationName.Trim <> '', Config.ApplicationName, 'DelphiAppServer')]));
    Params.Add('CharacterSet=utf8mb4'); // Recomendado para MySQL para soportar todos los caracteres Unicode

    if Config.SSL then
    begin
      // Para MySQL, los parámetros SSL suelen ser:
      // UseSSL=True (o False, o un modo específico como SSLMode=Required)
      // SSLCA, SSLCert, SSLKey, SSLCipher
      // Estos pueden ser pasados a través de Config.Params o tener campos dedicados en TDBConnectionConfig
      Params.Add('UseSSL=True'); // Opcionalmente configurable: GetStr(Config.Params, 'MySQLUseSSL', 'True')
      if Config.SSLRootCert.Trim <> '' then Params.Add(Format('SSLCA=%s', [Config.SSLRootCert]));
      if Config.SSLCert.Trim <> '' then Params.Add(Format('SSLCert=%s', [Config.SSLCert]));
      if Config.SSLKey.Trim <> '' then Params.Add(Format('SSLKey=%s', [Config.SSLKey]));
      // Ejemplo de SSLCipher: Params.Add(GetStr(Config.Params, 'MySQLSSLCipher', 'DHE-RSA-AES256-SHA'));
    end;

    if Config.Compress then
      Params.Add('Compress=Yes'); // Habilitar compresión de protocolo

    // Connection timeouts son manejados por TBaseConnection.ResourceOptions
    // Statement timeout (net_read_timeout, net_write_timeout) se puede establecer en ApplyDriverSpecificSettings si es necesario.

    if Config.Params.Trim <> '' then
       Params.Add(Config.Params); // Añadir parámetros adicionales

    Result := Params.CommaText;
    LogMessage(Format('MySQL Connection String (password omitted): Server=%s;Database=%s;User_Name=%s;CharacterSet=utf8mb4;...',
      [Config.Server, Config.Database, Config.Username]), logDebug);
  finally
    Params.Free;
  end;
end;

procedure TMySQLConnection.ApplyDriverSpecificSettings;
var
  SQLMode: string;
begin
  inherited;
  LogMessage(Format('Applying MySQL specific settings for connection "%s"...', [Config.Name]), logDebug);
  try
    Execute('SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;');
    // Execute('SET CHARACTER SET utf8mb4;'); // SET NAMES ya establece character_set_client, character_set_connection, character_set_results
    Execute('SET SESSION time_zone = ''+00:00'';'); // Usar UTC es buena práctica

    // SQL_MODE: Obtener de Config.Params o usar un default robusto.
    // El default aquí es bastante estricto, lo cual es bueno.
    SQLMode := GetStrPair(Config.Params, 'MySQLSessionSQLMode',
      'STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION');
    if SQLMode.Trim <> '' then
      Execute(Format('SET SESSION sql_mode = ''%s'';', [SQLMode])) // El valor de SQLMode no debe ser entrecomillado por QuoteIdentifier
    else
      Execute('SET SESSION sql_mode = ''STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'';');


    // Autocommit es True por defecto desde TBaseConnection.TxOptions.AutoCommit.
    // El nivel de aislamiento de transacción por defecto para InnoDB es REPEATABLE READ.
    // Si se necesita otro: Execute('SET SESSION transaction_isolation = ''READ-COMMITTED'';');
    LogMessage(Format('MySQL session settings applied. SQL_MODE: %s', [SQLMode]), logDebug);
  except
    on E: Exception do
      LogMessage(Format('Error applying MySQL specific settings for connection "%s": %s - %s.',
        [Config.Name, E.ClassName, E.Message]), logError);
  end;
end;

function TMySQLConnection.GetVersion: string;
begin
  try
    Result := VarToStr(ExecuteScalar('SELECT VERSION();'));
  except
    on E: Exception do
    begin
      LogMessage(Format('Error getting MySQL server_version for config "%s": %s. Falling back to base GetVersion.', [Config.Name, E.Message]), logError);
      Result := inherited GetVersion; // Llama a TBaseConnection.GetVersion
    end;
  end;
end;

procedure TMySQLConnection.OptimizeTable(const ATableName: string);
begin
  if ATableName.Trim = '' then
    raise EDBCommandError.Create('OptimizeTable: Table name cannot be empty.');
  // OPTIMIZE TABLE es para InnoDB, MyISAM, NDB. Reorganiza el almacenamiento físico.
  Execute(Format('OPTIMIZE TABLE %s;', [QuoteIdentifier(ATableName, FDBType)]));
  LogMessage(Format('Table "%s" optimization attempted.', [ATableName]), logInfo);
end;

procedure TMySQLConnection.AnalyzeTable(const ATableName: string);
begin
  if ATableName.Trim = '' then
    raise EDBCommandError.Create('AnalyzeTable: Table name cannot be empty.');
  // ANALYZE TABLE actualiza las estadísticas de distribución de claves para el optimizador.
  Execute(Format('ANALYZE TABLE %s;', [QuoteIdentifier(ATableName, FDBType)]));
  LogMessage(Format('Table "%s" analysis attempted.', [ATableName]), logInfo);
end;

procedure TMySQLConnection.RepairTable(const ATableName: string; AExtended: Boolean = False);
var
  SQL: string;
begin
  if ATableName.Trim = '' then
    raise EDBCommandError.Create('RepairTable: Table name cannot be empty.');
  // REPAIR TABLE funciona principalmente para tablas MyISAM. Para InnoDB, a menudo no es necesario o se maneja diferente.
  LogMessage(Format('Attempting to REPAIR TABLE %s (Note: Primarily for MyISAM tables). Extended: %s', [ATableName, BoolToStr(AExtended, True)]), logInfo);
  SQL := Format('REPAIR TABLE %s%s;', [QuoteIdentifier(ATableName, FDBType), IfThen(AExtended, ' EXTENDED', '')]);
  Execute(SQL);
  LogMessage(Format('REPAIR TABLE command executed for "%s".', [ATableName]), logInfo);
end;

function TMySQLConnection.GetTableStatusAsJSONArray(const ADatabaseName: string = ''): TJSONArray;
var
  SQL: string;
  DBName: string;
  LQuery: TDataSet;
begin
  DBName := ADatabaseName.Trim;
  if DBName.IsEmpty then
    DBName := Config.Database;

  if DBName.Trim <> '' then
    SQL := Format('SHOW TABLE STATUS FROM %s;', [QuoteIdentifier(DBName, FDBType)])
  else
    SQL := 'SHOW TABLE STATUS;'; // Muestra para la base de datos actual si DBName está vacío
  LQuery := nil;
  try
    LQuery := ExecuteReader(SQL); // LQuery ahora posee el TDataSet
    Result := LQuery.AsJSONArray(); // Asumiendo que AsJSONArray existe y no libera LQuery
  finally
    FreeAndNil(LQuery); // Asegurar la liberación del TDataSet
  end;
end;

function TMySQLConnection.GetProcessListAsJSONArray: TJSONArray;
const
  SQL_PROCESS_LIST = 'SHOW FULL PROCESSLIST;';
var
  LQuery: TDataSet; // Variable para manejar el DataSet
begin
  LQuery := nil;
  try
    LQuery := ExecuteReader(SQL_PROCESS_LIST);
    Result := LQuery.AsJSONArray(); // Asumiendo que AsJSONArray existe y no libera LQuery
  finally
    FreeAndNil(LQuery); // Asegurar la liberación del TDataSet
  end;
end;

procedure TMySQLConnection.KillMySQLProcess(AProcessId: Integer);
begin
  if AProcessId <= 0 then
    raise EDBCommandError.Create('Invalid ProcessId for KILL command.');
  // En MySQL, KILL [CONNECTION | QUERY] processlist_id
  // Por defecto, KILL sin CONNECTION o QUERY es lo mismo que KILL CONNECTION.

  Execute(Format('KILL %d;', [AProcessId])); // El punto y coma es opcional pero buena práctica
  LogMessage(Format('Attempted to KILL MySQL process ID: %d', [AProcessId]), logWarning);
end;

procedure TMySQLConnection.FlushTables(AWithReadLock: Boolean = False; const ATableNames: TArray<string> = nil);
var
  SQL: string;
  TableListStr: string;
  I: Integer;
begin
  SQL := 'FLUSH TABLES';
  if Assigned(ATableNames) and (Length(ATableNames) > 0) then
  begin
    var SB := TStringBuilder.Create;
    try
      for I := 0 to High(ATableNames) do
      begin
        if I > 0 then SB.Append(', ');
        SB.Append(QuoteIdentifier(ATableNames[I], FDBType));
      end;
      TableListStr := SB.ToString;
    finally
      SB.Free;
    end;
    SQL := SQL + ' ' + TableListStr;
  end;

  if AWithReadLock then
    SQL := SQL + ' WITH READ LOCK';

  Execute(SQL + ';');
  LogMessage(Format('FLUSH TABLES%s%s executed.',
    [IfThen(Assigned(ATableNames) and (Length(ATableNames) > 0), ' ' + TableListStr, ''),
     IfThen(AWithReadLock, ' WITH READ LOCK', '')]), logInfo);
end;

function TMySQLConnection.GetGlobalVariablesAsJSON: TJSONObject;
const
  SQL_GLOBAL_VARIABLES = 'SHOW GLOBAL VARIABLES;';
var
  LQuery: TDataSet;
begin
  Result := TJSONObject.Create; // El llamador libera
  LQuery := nil;
  try
    LQuery := ExecuteReader(SQL_GLOBAL_VARIABLES);
    while not LQuery.Eof do
    begin
      Result.AddPair(LQuery.FieldByName('Variable_name').AsString, LQuery.FieldByName('Value').AsString);
      LQuery.Next;
    end;
  finally
    FreeAndNil(LQuery);
  end;
end;

function TMySQLConnection.GetSessionVariablesAsJSON: TJSONObject;
const
  SQL_SESSION_VARIABLES = 'SHOW SESSION VARIABLES;';
var
  LQuery: TDataSet;
begin
  Result := TJSONObject.Create; // El llamador libera
  LQuery := nil;
  try
    LQuery := ExecuteReader(SQL_SESSION_VARIABLES);
    while not LQuery.Eof do
    begin
      Result.AddPair(LQuery.FieldByName('Variable_name').AsString, LQuery.FieldByName('Value').AsString);
      LQuery.Next;
    end;
  finally
    FreeAndNil(LQuery);
  end;
end;

procedure TMySQLConnection.SetSessionVariable(const AVariableName: string; const AValue: string);
var
  Params: TFDParams;
  SanitizedVarName: string;
begin
  // Validar AVariableName para prevenir inyección SQL.
  // Los nombres de variables de sesión en MySQL no suelen necesitar entrecomillado con acentos graves
  // en sentencias SET, pero deben ser nombres válidos.
  if not TRegEx.IsMatch(AVariableName, '^[a-zA-Z0-9_]+$') then
    raise EDBCommandError.Create('Invalid variable name for SET SESSION: ' + AVariableName);

  Params:=TFDParams.Create;
  Params.Add('',AValue);
  SanitizedVarName := AVariableName; // Ya validado por regex
  // El valor se pasa como parámetro para que FireDAC lo escape correctamente.
  Execute(Format('SET SESSION %s = :AValue;', [SanitizedVarName]), Params);
  Params.free;
  LogMessage(Format('Session variable "%s" set to value (parameterized).', [SanitizedVarName]), logInfo);
end;

function TMySQLConnection.GetInnoDBStatusRaw: string;
const
  SQL_INNODB_STATUS = 'SHOW ENGINE INNODB STATUS;';
var
  LQuery: TDataSet;
  StatusText: string;
begin
  Result := '';
  LQuery := nil;
  try
    LQuery := ExecuteReader(SQL_INNODB_STATUS);
    if not LQuery.Eof then
    begin
      // SHOW ENGINE INNODB STATUS devuelve múltiples columnas, la principal es 'Status'.
      // Otras son 'Type', 'Name'.
      StatusText := LQuery.FieldByName('Status').AsString;
      Result := StatusText;
      // Podrías querer parsear esta larga cadena de StatusText en un TJSONObject si necesitas valores específicos.
      // Por ahora, se devuelve el texto crudo.
    end
    else
      LogMessage('Could not retrieve InnoDB status (empty result).', logWarning);
  finally
    FreeAndNil(LQuery);
  end;
end;

function TMySQLConnection.GetEngineStatusAsJSON(const AEngineName: string = 'InnoDB'): TJSONObject;
var
  LQuery: TDataSet;
  SQL: string;
  FieldName: string;
  I: Integer;
begin
  Result := TJSONObject.Create;
  LQuery := nil;

  if AEngineName.Trim = '' then
  begin
    Result.AddPair('error', 'Engine name cannot be empty for GetEngineStatusAsJSON.');
    LogMessage('GetEngineStatusAsJSON: Engine name cannot be empty.', logWarning);
    Exit;
  end;

  // Para InnoDB, SHOW ENGINE INNODB STATUS es más detallado y tiene un formato específico.
  if SameText(AEngineName, 'InnoDB') then
  begin
    Result.AddPair('engine_name', 'InnoDB');
    Result.AddPair('raw_status_text', GetInnoDBStatusRaw); // Devolver el texto crudo
    // Parsear el raw_status_text aquí sería complejo y propenso a errores si el formato cambia.
    // El cliente puede parsear el raw_status_text si es necesario.
    Exit;
  end;

  // Para otros motores, o un resumen (si SHOW ENGINE <Name> STATUS es soportado de forma genérica)
  SQL := Format('SHOW ENGINE %s STATUS;', [QuoteIdentifier(AEngineName, FDBType)]);
  try
    LQuery := ExecuteReader(SQL);
    if not LQuery.Eof then
    begin
      Result.AddPair('engine_name', AEngineName); // El nombre del motor que se consultó
      // Iterar sobre los campos devueltos por SHOW ENGINE <Name> STATUS
      // El formato de salida puede variar entre motores.
      for I := 0 to LQuery.FieldCount - 1 do
      begin
        FieldName := LQuery.Fields[I].FieldName;
        if LQuery.Fields[I].IsNull then
          Result.AddPair(FieldName, TJSONNull.Create)
        else
          Result.AddPair(FieldName, TJSONString.Create(LQuery.Fields[I].AsString)); // Tratar todos como string por seguridad
      end;
    end
    else
      Result.AddPair('error', Format('Could not retrieve status for engine "%s" or engine does not exist/support this command.', [AEngineName]));
  finally
    FreeAndNil(LQuery);
  end;
end;

end.

