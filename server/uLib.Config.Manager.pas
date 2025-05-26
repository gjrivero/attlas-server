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
    FSingletonLock: TCriticalSection;

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

    function GetGlobalConfigClone: TJSONObject; // Devuelve un clon de toda la configuración

    // Método genérico para obtener un valor de una ruta de sección, con un valor por defecto.
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
  FConfigData := TJSONObject.Create; // Iniciar con un objeto JSON vacío
  if AConfigBaseDir.Trim <> '' then
    Initialize(AConfigBaseDir) // Cargar configuración si se proporciona un path base
  else
    FConfigFilePath := ''; // No hay path, se deberá llamar a Initialize o Load explícitamente
  LogMessage('TConfigManager instance (CreateInternal) created.', logInfo);
end;

destructor TConfigManager.Destroy;
begin
  LogMessage('TConfigManager instance destroying...', logDebug);
  FreeAndNil(FConfigData);
  FreeAndNil(FConfigLock);
  LogMessage('TConfigManager instance destroyed.', logInfo);
  inherited;
end;

class function TConfigManager.GetInstance(const AConfigBaseDir: string = ''): TConfigManager;
begin
  if not Assigned(FInstance) then
  begin
    if not Assigned(FSingletonLock) then // Guarda de seguridad
    begin
      LogMessage('CRITICAL: TConfigManager.FSingletonLock is nil in GetInstance!', logFatal);
      FSingletonLock := TCriticalSection.Create; // Intento de recuperación
      if not Assigned(FSingletonLock) then
        raise Exception.Create('TConfigManager SingletonLock could not be initialized.');
    end;
    FSingletonLock.Acquire;
    try
      if not Assigned(FInstance) then
      begin
        // Si AConfigBaseDir está vacío aquí, FConfigFilePath no se establecerá
        // y LoadConfigurationFromFile fallará o no hará nada hasta que Initialize sea llamado.
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
  end
  else if (AConfigBaseDir.Trim <> '') and
       (not SameText( ExtractFilePath(FInstance.FConfigFilePath),
                      EnsurePathHasTrailingDelimiter(AConfigBaseDir))) then
  begin
    // La instancia existe pero se llama GetInstance con un path base diferente.
    // Esto podría indicar un error de lógica o la necesidad de re-inicializar.
    LogMessage(Format('TConfigManager.GetInstance called with a new base directory "%s" but instance was already initialized with "%s". Re-initializing.',
      [AConfigBaseDir, FInstance.FConfigFilePath]), logWarning);
    FInstance.Initialize(AConfigBaseDir); // Re-inicializar con el nuevo path
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
    // Cargar la configuración inmediatamente después de establecer el path
    LoadConfigurationFromFile;
  finally
    FConfigLock.Release;
  end;
end;

procedure TConfigManager.LoadConfigurationFromFile;
var
  LJsonString: string;
  LNewConfig: TJSONObject;
begin
  // Este método debe ser llamado bajo FConfigLock
  if FConfigFilePath.Trim = '' then
  begin
    LogMessage('TConfigManager.LoadConfigurationFromFile: Config file path not set. Cannot load.', logWarning);
    // Asegurar que FConfigData esté vacío si no se puede cargar
    FreeAndNil(FConfigData);
    FConfigData := TJSONObject.Create;
    Exit;
  end;

  LogMessage(Format('TConfigManager: Loading configuration from: %s', [FConfigFilePath]), logInfo);
  if not TFile.Exists(FConfigFilePath) then
  begin
    LogMessage(Format('Configuration file not found: %s. Using empty configuration.', [FConfigFilePath]), logError);
    FreeAndNil(FConfigData); // Liberar config anterior si existía
    FConfigData := TJSONObject.Create; // Usar un JSON vacío
    // Considerar lanzar EConfigurationError si el archivo es obligatorio:
    // raise EConfigurationError.Create(Format('Configuration file "%s" not found.', [FConfigFilePath]));
    Exit;
  end;

  LNewConfig := nil;
  try
    LJsonString := TFile.ReadAllText(FConfigFilePath, TEncoding.UTF8);
    LNewConfig := TJSONObject.ParseJSONValue(LJsonString) as TJSONObject;
    if not Assigned(LNewConfig) then // ParseJSONValue puede devolver nil si el string es válido pero no un objeto (ej. "null" o un array)
      raise EConfigurationError.Create(Format('Failed to parse "%s" into a valid JSON object. Content might be an array or null.', [FConfigFilePath]));

    // Reemplazar la configuración existente
    FreeAndNil(FConfigData);
    FConfigData := LNewConfig; // LNewConfig ahora es propiedad de FConfigData
    LogMessage(Format('Configuration successfully loaded from %s.', [FConfigFilePath]), logInfo);
  except
    on E: Exception do
    begin
      LogMessage(Format('Error loading or parsing configuration file %s: %s - %s. Current configuration (if any) will be kept, or reset to empty.',
        [FConfigFilePath, E.ClassName, E.Message]), logError);
      FreeAndNil(LNewConfig); // Liberar si se creó parcialmente antes del error
      // Mantener FConfigData anterior o resetear a vacío en lugar de dejarlo en estado inconsistente.
      // Si FConfigData ya fue liberado y LNewConfig falló, FConfigData será nil.
      // Si FConfigData no fue liberado (ej. error de parseo antes de FreeAndNil(FConfigData)),
      // es mejor resetearlo para evitar usar una config corrupta o antigua.
      if not Assigned(FConfigData) then // Si ya era nil o se liberó
         FConfigData := TJSONObject.Create; // Asegurar que siempre haya un objeto JSON válido (aunque sea vacío)
      // No re-lanzar para permitir que la aplicación continúe con defaults o config anterior si es posible,
      // pero el log de error es crucial. O, si la config es crítica, re-lanzar:
      // raise EConfigurationError.Create(Format('Failed to load configuration from %s: %s', [FConfigFilePath, E.Message]));
    end;
  end;
end;

function TConfigManager.ReloadConfiguration: Boolean;
begin
  FConfigLock.Acquire;
  try
    LogMessage('TConfigManager: Reloading configuration...', logInfo);
    LoadConfigurationFromFile;
    Result := Assigned(FConfigData) and (FConfigData.Count > 0); // Éxito si se cargó algo
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
      Result := TJSONObject.Create; // Devolver objeto vacío si no hay config
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
