unit uLib.Config.Manager;

interface

uses
  System.SysUtils, System.Classes, System.JSON, System.SyncObjs,
  System.Generics.Collections, // Para TDictionary si se usara internamente
  uLib.Server.Types, // Para EConfigurationError
  uLib.Logger;       // Para LogMessage

type
  TConfigManager = class
  protected
  private
    class var FInstance: TConfigManager;
    class var FSingletonLock: TCriticalSection;
    var FConfigFilePath: string;    // Path completo al archivo config.json
    FConfigLock: TCriticalSection; // Protege el acceso a FConfigData y FConfigFilePath
    FConfigData: TJSONObject;
    procedure LoadConfigurationFromFile; // Carga o recarga desde FConfigFilePath

    class constructor CreateClass;
    class destructor DestroyClass;

    constructor CreateInternal(const AConfigBaseDir: string); // Constructor privado
  public

    destructor Destroy; override;
    class function GetInstance(const AConfigBaseDir: string = ''): TConfigManager; // AConfigBaseDir opcional en GetInstance

    procedure Initialize(const AConfigBaseDir: string); // Para inicializar o re-inicializar con un path

    function ReloadConfiguration: Boolean;

    /// <summary>
    /// Devuelve un clon de toda la configuración actual.
    /// El llamador es responsable de liberar el objeto devuelto.
    /// </summary>
    function GetGlobalConfigClone: TJSONObject; // Devuelve un clon de toda la configuraci�n

    // M�todo gen�rico para obtener un valor de una ruta de secci�n, con un valor por defecto.
    // APath puede ser anidado, ej. "server.port" o "security.jwt.secret"

    property ConfigData: TJSONObject read FConfigData;
    property ConfigFilePath: string read FConfigFilePath;
  end;

var
   GConfigManager: TConfigManager;

implementation

uses
  System.StrUtils,
  System.IOUtils,
  System.Diagnostics,

  uLib.Utils;

{ TConfigManager }

class constructor TConfigManager.CreateClass;
begin
  if not Assigned(FSingletonLock) then
    FSingletonLock := TCriticalSection.Create;
end;

class destructor TConfigManager.DestroyClass;
begin
  FreeAndNil(FSingletonLock);
end;

constructor TConfigManager.CreateInternal(const AConfigBaseDir: string);
begin
  inherited Create;
  FConfigLock := TCriticalSection.Create;
  FConfigData := TJSONObject.Create; // Iniciar con un objeto JSON vac�o
  if AConfigBaseDir.Trim <> '' then
    Initialize(AConfigBaseDir) // Cargar configuraci�n si se proporciona un path base
  else
    FConfigFilePath := ''; // No hay path, se deber� llamar a Initialize o Load expl�citamente
end;

destructor TConfigManager.Destroy;
begin
  FreeAndNil(FConfigData);
  FreeAndNil(FConfigLock);
  inherited;
end;

class function TConfigManager.GetInstance(const AConfigBaseDir: string = ''): TConfigManager;
var
  LProvidedBaseDirTrimmed: string;
  LInstanceConfigPathDir: string;
begin
  if not Assigned(FInstance) then
  begin
    if not Assigned(FSingletonLock) then // Guarda de seguridad
    begin
      LogMessage('CRITICAL: TConfigManager.FSingletonLock is nil in GetInstance!', logFatal);
      FSingletonLock := TCriticalSection.Create; // Intento de recuperaci�n
      if not Assigned(FSingletonLock) then
        raise Exception.Create('TConfigManager SingletonLock could not be initialized.');
    end;
    FSingletonLock.Acquire;
    try
      if not Assigned(FInstance) then
      begin
        // Si AConfigBaseDir est� vac�o aqu�, FConfigFilePath no se establecer�
        // y LoadConfigurationFromFile fallar� o no har� nada hasta que Initialize sea llamado.
        FInstance := TConfigManager.CreateInternal(AConfigBaseDir);
      end
      else if (AConfigBaseDir.Trim <> '') and (FInstance.FConfigFilePath.Trim = '') then
      begin
        // La instancia ya existe pero no fue inicializada con un path, y ahora se provee uno.
        LogMessage('TConfigManager.GetInstance: Instance exists but not initialized with path. Initializing now.', logInfo);
        FInstance.Initialize(AConfigBaseDir);
      end;
    finally
      FSingletonLock.Release;
    end;
  end;
  LProvidedBaseDirTrimmed := Trim(AConfigBaseDir);

  // Caso 1: La instancia existe, pero su FConfigFilePath no se ha establecido (lazy init)
  // Y se proporciona un AConfigBaseDir ahora.
  if Assigned(FInstance) and (FInstance.FConfigFilePath.Trim = '') and (LProvidedBaseDirTrimmed <> '') then
  begin
    LogMessage(Format('TConfigManager.GetInstance: Instance exists but FConfigFilePath was not set. Initializing now with base directory: "%s".', [LProvidedBaseDirTrimmed]), logInfo);
    FInstance.Initialize(LProvidedBaseDirTrimmed);
  end
  // Caso 2: La instancia existe, FConfigFilePath est� establecido,
  // Y se proporciona un AConfigBaseDir diferente.
  else if Assigned(FInstance) and (FInstance.FConfigFilePath.Trim <> '') and (LProvidedBaseDirTrimmed <> '') then
  begin
    // Normalizar ambos paths antes de comparar para evitar falsos positivos por delimitadores.
    LInstanceConfigPathDir := EnsurePathHasTrailingDelimiter(ExtractFilePath(FInstance.FConfigFilePath));
    var LProvidedNormalizedBaseDir := EnsurePathHasTrailingDelimiter(LProvidedBaseDirTrimmed);
    {$IFDEF MSWINDOWS}
    if not SameText(LInstanceConfigPathDir, LProvidedNormalizedBaseDir) then
    {$ELSE}
    if not SameStr(LInstanceConfigPathDir, LProvidedNormalizedBaseDir) then
    {$ENDIF}
    begin
      LogMessage(Format('TConfigManager.GetInstance called with a different base directory ("%s") ' +
        'but an instance was already initialized with a path derived from base directory ("%s"). ' +
        'The existing configuration path will be kept. Use TConfigManager.Initialize() for explicit re-initialization.',
        [LProvidedBaseDirTrimmed, LInstanceConfigPathDir]), logWarning);
      // NO se reinicializa autom�ticamente. Se mantiene la configuraci�n existente.
    end;
  end;
  Result := FInstance;
end;

procedure TConfigManager.Initialize(const AConfigBaseDir: string);
begin
  FConfigLock.Acquire;
  try
    if AConfigBaseDir.Trim = '' then
    begin
      LogMessage('TConfigManager.Initialize: AConfigBaseDir is empty. Configuration path not set.', logError);
      FConfigFilePath := '';
      // Liberar FConfigData anterior y crear uno vacío si se reinicializa sin path
      FreeAndNil(FConfigData);
      FConfigData := TJSONObject.Create;
      Exit;
    end;

    var LConfigDir := EnsurePathHasTrailingDelimiter(AConfigBaseDir);
    FConfigFilePath := TPath.Combine(LConfigDir, 'config.json');
    LogMessage(Format('TConfigManager initialized. Config file path set to: %s', [FConfigFilePath]), logInfo);
    // Cargar la configuraci�n inmediatamente despu�s de establecer el path
    LoadConfigurationFromFile;
  finally
    FConfigLock.Release;
  end;
end;

procedure TConfigManager.LoadConfigurationFromFile;
var
  LJsonString: string;
  LNewConfig: TJSONObject;
  LParsedValue: TJSONValue;
  FileSize: Int64;
  LoadStartTime: TStopwatch;
begin
  // Este método debe ser llamado bajo FConfigLock
  if FConfigFilePath.Trim = '' then
  begin
    LogMessage('TConfigManager.LoadConfigurationFromFile: Config file path not set. Cannot load.', logWarning);
    FreeAndNil(FConfigData);
    FConfigData := TJSONObject.Create;
    Exit;
  end;

  LoadStartTime := TStopwatch.StartNew;
  LogMessage(Format('TConfigManager: Loading configuration from: %s', [FConfigFilePath]), logInfo);

  if not TFile.Exists(FConfigFilePath) then
  begin
    LogMessage(Format('Configuration file not found: %s. Using empty configuration.', [FConfigFilePath]), logError);
    FreeAndNil(FConfigData);
    FConfigData := TJSONObject.Create;
    raise EConfigurationError.Create(Format('Configuration file "%s" not found.', [FConfigFilePath]));
  end;

  // Información sobre el archivo
  try
    FileSize := TFile.GetSize(FConfigFilePath);
    LogMessage(Format('Configuration file size: %d bytes', [FileSize]), logDebug);

    if FileSize = 0 then
    begin
      LogMessage('Configuration file is empty', logWarning);
      FreeAndNil(FConfigData);
      FConfigData := TJSONObject.Create;
      Exit;
    end;

    if FileSize > 10 * 1024 * 1024 then // 10MB
    begin
      LogMessage(Format('Configuration file is unusually large: %d bytes', [FileSize]), logWarning);
    end;
  except
    on E: Exception do
    begin
      LogMessage(Format('Error getting file size for %s: %s', [FConfigFilePath, E.Message]), logWarning);
    end;
  end;

  LNewConfig := nil;
  LParsedValue := nil; // CORRECCIÓN: Inicializar explícitamente

  try
    try
      LJsonString := TFile.ReadAllText(FConfigFilePath, TEncoding.UTF8);
      LogMessage(Format('Configuration file read successfully. Length: %d characters', [Length(LJsonString)]), logDebug);

      LParsedValue := TJSONObject.ParseJSONValue(LJsonString);
      if not Assigned(LParsedValue) then
        raise EConfigurationError.Create(Format('Failed to parse "%s" as valid JSON.', [FConfigFilePath]));

      // CORRECCIÓN: Verificar tipo antes del cast
      if not (LParsedValue is TJSONObject) then
      begin
        raise EConfigurationError.Create(Format('Configuration file "%s" does not contain a JSON object. Found: %s',
          [FConfigFilePath, LParsedValue.ClassName]));
      end;

      LNewConfig := LParsedValue as TJSONObject;
      LParsedValue := nil; // Transfer ownership - CORRECCIÓN: Prevenir doble liberación

      // Logging de configuración cargada
      LogMessage(Format('Configuration parsed successfully. Root keys: %d', [LNewConfig.Count]), logInfo);

      {$IFDEF DEBUG}
      // En modo debug, listar las secciones principales
      for var i := 0 to LNewConfig.Count - 1 do
      begin
        var Pair := LNewConfig.Pairs[i];
        if Assigned(Pair) then
        begin
          if Pair.JsonValue is TJSONObject then
            LogMessage(Format('Config section: %s (object with %d properties)',
              [Pair.JsonString.Value, (Pair.JsonValue as TJSONObject).Count]), logDebug)
          else if Pair.JsonValue is TJSONArray then
            LogMessage(Format('Config section: %s (array with %d items)',
              [Pair.JsonString.Value, (Pair.JsonValue as TJSONArray).Count]), logDebug)
          else
            LogMessage(Format('Config property: %s = %s',
              [Pair.JsonString.Value, Copy(Pair.JsonValue.Value, 1, 50)]), logDebug);
        end;
      end;
      {$ENDIF}

      // Reemplazar configuración actual
      FreeAndNil(FConfigData);
      FConfigData := LNewConfig;
      LNewConfig := nil; // Prevent cleanup

      LoadStartTime.Stop;
      LogMessage(Format('Configuration loaded successfully in %d ms from: %s',
        [LoadStartTime.ElapsedMilliseconds, FConfigFilePath]), logInfo);

    except
      on E: EConfigurationError do
      begin
        LogMessage(Format('Configuration error loading %s: %s', [FConfigFilePath, E.Message]), logError);
        // CORRECCIÓN: Liberar recursos apropiadamente
        if Assigned(LNewConfig) then FreeAndNil(LNewConfig);
        if Assigned(LParsedValue) then FreeAndNil(LParsedValue);
        if not Assigned(FConfigData) then FConfigData := TJSONObject.Create;
        raise; // Re-lanzar errores de configuración
      end;
      on E: Exception do
      begin
        LogMessage(Format('Unexpected error loading configuration file %s: %s - %s. Current configuration will be kept or reset to empty.',
          [FConfigFilePath, E.ClassName, E.Message]), logError);
        // CORRECCIÓN: Liberar recursos apropiadamente
        if Assigned(LNewConfig) then FreeAndNil(LNewConfig);
        if Assigned(LParsedValue) then FreeAndNil(LParsedValue);
        if not Assigned(FConfigData) then FConfigData := TJSONObject.Create;
        // No re-lanzar errores inesperados, mantener funcionamiento con config vacía
      end;
    end;
  finally
    // CORRECCIÓN: Finally adicional para casos extremos
    if Assigned(LNewConfig) then FreeAndNil(LNewConfig);
    if Assigned(LParsedValue) then FreeAndNil(LParsedValue);
  end;
end;

function TConfigManager.ReloadConfiguration: Boolean;
begin
  FConfigLock.Acquire;
  try
    LogMessage('TConfigManager: Reloading configuration...', logInfo);
    LoadConfigurationFromFile;
    Result := Assigned(FConfigData) and (FConfigData.Count > 0); // �xito si se carg� algo
  finally
    FConfigLock.Release;
  end;
end;

function TConfigManager.GetGlobalConfigClone: TJSONObject;
begin
  FConfigLock.Acquire;
  try
    if Assigned(FConfigData) then
      Result := FConfigData.Clone as TJSONObject
    else
      Result := TJSONObject.Create; // Devolver objeto vac�o si no hay config
  finally
    FConfigLock.Release;
  end;
end;


initialization
  TConfigManager.FInstance := nil;
  // El class constructor se encarga de FSingletonLock
finalization
  if Assigned(TConfigManager.FInstance) then
    FreeAndNil(TConfigManager.FInstance);
  // El class destructor se encarga de FSingletonLock
end.
