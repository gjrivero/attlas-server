unit uLib.Sync.Manager;

interface

uses
  System.SysUtils, System.Classes, System.JSON, System.SyncObjs,
  System.Generics.Collections, System.DateUtils, System.Net.HttpClient,
  uLib.Sync.Types, uLib.Database.Types, uLib.Logging,
  uLib.Config.Manager;

type
  TSyncCompletedEvent = procedure(Sender: TObject; 
    const Progress: TSyncProgress) of object;
  TSyncErrorEvent = procedure(Sender: TObject; 
    const Error: string) of object;
  TSyncConflictEvent = procedure(Sender: TObject; 
    var Conflict: TSyncConflict) of object;

  TSyncQueue = class
  private
    FQueue: TQueue<TSyncEntity>;
    FLock: TCriticalSection;
    
  public
    constructor Create;
    destructor Destroy; override;
    
    procedure Enqueue(const Entity: TSyncEntity);
    function Dequeue: TSyncEntity;
    function IsEmpty: Boolean;
    procedure Clear;
  end;

  TSyncManager = class
  private
    class var FInstance: TSyncManager;
    
    FLock: TCriticalSection;
    FLogger: TLogger;
    FConfig: TConfigManager;
    FHttpClient: THTTPClient;
    FQueue: TSyncQueue;
    FOptions: TSyncOptions;
    FProgress: TSyncProgress;
    FSyncing: Boolean;
    
    FOnSyncCompleted: TSyncCompletedEvent;
    FOnSyncError: TSyncErrorEvent;
    FOnConflict: TSyncConflictEvent;
    
    constructor Create;
    procedure Initialize;
    function ProcessEntity(var Entity: TSyncEntity): Boolean;
    function HandleConflict(var Conflict: TSyncConflict): Boolean;
    function CompareData(const Local, Remote: TJSONObject): Boolean;
    procedure UpdateProgress(const Entity: TSyncEntity);
    function ValidateHash(const Data: TJSONObject): string;
    
  public
    destructor Destroy; override;
    class function GetInstance: TSyncManager;
    
    // Control de sincronización
    procedure StartSync(const Options: TSyncOptions);
    procedure StopSync;
    procedure PauseSync;
    procedure ResumeSync;
    
    // Gestión de entidades
    procedure AddEntity(const Entity: TSyncEntity);
    function GetEntityStatus(const ID: string): TSyncState;
    function GetPendingEntities: TArray<TSyncEntity>;
    
    // Configuración y estado
    procedure UpdateOptions(const NewOptions: TSyncOptions);
    function GetProgress: TSyncProgress;
    function IsSyncing: Boolean;
    
    // Eventos
    property OnSyncCompleted: TSyncCompletedEvent 
      read FOnSyncCompleted write FOnSyncCompleted;
    property OnSyncError: TSyncErrorEvent 
      read FOnSyncError write FOnSyncError;
    property OnConflict: TSyncConflictEvent 
      read FOnConflict write FOnConflict;
  end;

implementation

{ TSyncQueue }

constructor TSyncQueue.Create;
begin
  inherited Create;
  FQueue := TQueue<TSyncEntity>.Create;
  FLock := TCriticalSection.Create;
end;

destructor TSyncQueue.Destroy;
begin
  FQueue.Free;
  FLock.Free;
  inherited;
end;

procedure TSyncQueue.Enqueue(const Entity: TSyncEntity);
begin
  FLock.Enter;
  try
    FQueue.Enqueue(Entity);
  finally
    FLock.Leave;
  end;
end;

function TSyncQueue.Dequeue: TSyncEntity;
begin
  FLock.Enter;
  try
    Result := FQueue.Dequeue;
  finally
    FLock.Leave;
  end;
end;

function TSyncQueue.IsEmpty: Boolean;
begin
  FLock.Enter;
  try
    Result := FQueue.Count = 0;
  finally
    FLock.Leave;
  end;
end;

procedure TSyncQueue.Clear;
begin
  FLock.Enter;
  try
    FQueue.Clear;
  finally
    FLock.Leave;
  end;
end;

{ TSyncManager }

constructor TSyncManager.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FQueue := TSyncQueue.Create;
  FHttpClient := THTTPClient.Create;
  
  Initialize;
end;

destructor TSyncManager.Destroy;
begin
  StopSync;
  FQueue.Free;
  FHttpClient.Free;
  FLock.Free;
  inherited;
end;

class function TSyncManager.GetInstance: TSyncManager;
begin
  if not Assigned(FInstance) then
    FInstance := TSyncManager.Create;
  Result := FInstance;
end;

procedure TSyncManager.Initialize;
begin
  try
    FLogger := TLogger.GetInstance;
    FConfig := TConfigManager.GetInstance;
    
    // Configurar cliente HTTP
    FHttpClient.ConnectionTimeout := 30000; // 30 segundos
    FHttpClient.ResponseTimeout := 30000;   // 30 segundos
    
    if Assigned(FLogger) then
      FLogger.Log(llInfo, 'Sync manager initialized');
      
  except
    on E: Exception do
    begin
      if Assigned(FLogger) then
        FLogger.Log(llError, 'Error initializing sync manager: ' + E.Message);
      raise;
    end;
  end;
end;

procedure TSyncManager.StartSync(const Options: TSyncOptions);
begin
  if FSyncing then
    Exit;
    
  FLock.Enter;
  try
    FOptions := Options;
    FSyncing := True;
    FProgress.StartTime := Now;
    FProgress.TotalEntities := FQueue.FQueue.Count;
    FProgress.CompletedEntities := 0;
    FProgress.FailedEntities := 0;
    
    if Assigned(FLogger) then
      FLogger.Log(llInfo, 'Starting synchronization...');
      
    // Iniciar sincronización en thread separado
    TThread.CreateAnonymousThread(
      procedure
      var
        Entity: TSyncEntity;
      begin
        while FSyncing and not FQueue.IsEmpty do
        try
          Entity := FQueue.Dequeue;
          if ProcessEntity(Entity) then
            Inc(FProgress.CompletedEntities)
          else
            Inc(FProgress.FailedEntities);
            
          UpdateProgress(Entity);
          
        except
          on E: Exception do
          begin
            if Assigned(FLogger) then
              FLogger.Log(llError, 'Sync error: ' + E.Message);
              
            if Assigned(FOnSyncError) then
              FOnSyncError(Self, E.Message);
          end;
        end;
        
        // Finalizar sincronización
        FSyncing := False;
        FProgress.EndTime := Now;
        
        if Assigned(FOnSyncCompleted) then
          FOnSyncCompleted(Self, FProgress);
      end
    ).Start;
    
  finally
    FLock.Leave;
  end;
end;

procedure TSyncManager.StopSync;
begin
  FSyncing := False;
end;

procedure TSyncManager.PauseSync;
begin
  FSyncing := False;
end;

procedure TSyncManager.ResumeSync;
begin
  if not FQueue.IsEmpty then
    StartSync(FOptions);
end;

function TSyncManager.ProcessEntity(var Entity: TSyncEntity): Boolean;
var
  LocalData, RemoteData: TJSONObject;
begin
  Result := False;
  
  try
    Entity.State := ssSyncing;
    
    // Obtener datos locales
    LocalData := GetLocalData(Entity);
    try
      // Obtener datos remotos
      RemoteData := GetRemoteData(Entity);
      try
        // Verificar conflictos
        if not CompareData(LocalData, RemoteData) then
        begin
          var Conflict: TSyncConflict;
          Conflict.EntityID := Entity.ID;
          Conflict.TableName := Entity.TableName;
          Conflict.LocalData := LocalData.Clone as TJSONObject;
          Conflict.RemoteData := RemoteData.Clone as TJSONObject;
          
          if not HandleConflict(Conflict) then
          begin
            Entity.State := ssFailed;
            Entity.HasConflicts := True;
            Exit;
          end;
        end;
        
        // Sincronizar datos
        case FOptions.Direction of
          sdUpload:
            Result := UploadData(Entity, LocalData);
            
          sdDownload:
            Result := DownloadData(Entity, RemoteData);
            
          sdBidirectional:
            begin
              if Entity.Version < RemoteData.GetValue<Integer>('version') then
                Result := DownloadData(Entity, RemoteData)
              else
                Result := UploadData(Entity, LocalData);
            end;
        end;
        
        if Result then
        begin
          Entity.State := ssCompleted;
          Entity.LastSync := Now;
          Entity.LastHash := ValidateHash(
            IfThen(FOptions.Direction = sdDownload, RemoteData, LocalData)
          );
        end
        else
          Entity.State := ssFailed;
          
      finally
        RemoteData.Free;
      end;
    finally
      LocalData.Free;
    end;
    
  except
    on E: Exception do
    begin
      Entity.State := ssFailed;
      Entity.ErrorMessage := E.Message;
      
      if Assigned(FLogger) then
        FLogger.Log(llError, Format('Error processing entity %s: %s',
          [Entity.ID, E.Message]));
          
      Result := False;
    end;
  end;
end;

function TSyncManager.HandleConflict(var Conflict: TSyncConflict): Boolean;
begin
  Result := True;
  
  // Si hay handler de conflictos personalizado, usarlo
  if Assigned(FOnConflict) then
  begin
    FOnConflict(Self, Conflict);
    Exit;
  end;
  
  // Resolución por defecto según configuración
  case FOptions.ConflictResolution of
    'server_wins':
      Conflict.Resolution := 'server';
      
    'client_wins':
      Conflict.Resolution := 'client';
      
    'newest_wins':
      begin
        var LocalTime := Conflict.LocalData.GetValue<TDateTime>('updated_at');
        var RemoteTime := Conflict.RemoteData.GetValue<TDateTime>('updated_at');
        
        if LocalTime > RemoteTime then
          Conflict.Resolution := 'client'
        else
          Conflict.Resolution := 'server';
      end;
      
    else
      Result := False;
  end;
  
  Conflict.ResolvedBy := 'system';
  Conflict.ResolvedAt := Now;
end;

function TSyncManager.CompareData(const Local, Remote: TJSONObject): Boolean;
begin
  if FOptions.ValidateHashes then
    Result := ValidateHash(Local) = ValidateHash(Remote)
  else
    Result := Local.ToJSON = Remote.ToJSON;
end;

function TSyncManager.ValidateHash(const Data: TJSONObject): string;
begin
  Result := THashSHA2.GetHashString(Data.ToJSON);
end;

procedure TSyncManager.UpdateProgress(const Entity: TSyncEntity);
begin
  FProgress.CurrentEntity := Entity.ID;
  
  if Assigned(FLogger) then
    FLogger.Log(llDebug, Format('Sync progress: %d/%d (Failed: %d)',
      [FProgress.CompletedEntities, FProgress.TotalEntities, 
       FProgress.FailedEntities]));
end;

procedure TSyncManager.AddEntity(const Entity: TSyncEntity);
begin
  FQueue.Enqueue(Entity);
end;

function TSyncManager.GetEntityStatus(const ID: string): TSyncState;
begin
  Result := ssNone;
  FLock.Enter;
  try
    for var Entity in FQueue.FQueue.ToArray do
      if Entity.ID = ID then
      begin
        Result := Entity.State;
        Break;
      end;
  finally
    FLock.Leave;
  end;
end;

function TSyncManager.GetPendingEntities: TArray<TSyncEntity>;
var
  Pending: TList<TSyncEntity>;
begin
  Pending := TList<TSyncEntity>.Create;
  try
    FLock.Enter;
    try
      for var Entity in FQueue.FQueue.ToArray do
        if Entity.State in [ssNone, ssPending] then
          Pending.Add(Entity);
          
      Result := Pending.ToArray;
    finally
      FLock.Leave;
    end;
  finally
    Pending.Free;
  end;
end;

procedure TSyncManager.UpdateOptions(const NewOptions: TSyncOptions);
begin
  FLock.Enter;
  try
    FOptions := NewOptions;
  finally
    FLock.Leave;
  end;
end;

function TSyncManager.GetProgress: TSyncProgress;
begin
  FLock.Enter;
  try
    Result := FProgress;
  finally
    FLock.Leave;
  end;
end;

function TSyncManager.IsSyncing: Boolean;
begin
  Result := FSyncing;
end;

initialization
  TSyncManager.FInstance := nil;
  
finalization
  if Assigned(TSyncManager.FInstance) then
    TSyncManager.FInstance.Free;
    
end.