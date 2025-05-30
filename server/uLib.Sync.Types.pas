unit uLib.Sync.Types;

interface

uses
  System.SysUtils, System.Classes, System.JSON, System.DateUtils,
  System.Generics.Collections;

type
  TSyncState = (ssNone, ssPending, ssSyncing, ssCompleted, ssFailed);
  
  TSyncType = (stFull, stIncremental, stDelta);

  TSyncDirection = (sdUpload, sdDownload, sdBidirectional);

  TSyncEntity = class
  private
    ID: string;
    TableName: string;
    LastSync: TDateTime;
    LastHash: string;
    Version: Integer;
    State: TSyncState;
    HasConflicts: Boolean;
    ErrorMessage: string;
  public  
    function ToJSON: TJSONObject;
    procedure FromJSON(const JSON: TJSONObject);
  end;

  TSyncConflict = class
  private
    EntityID: string;
    TableName: string;
    LocalVersion: Integer;
    RemoteVersion: Integer;
    LocalData: TJSONObject;
    RemoteData: TJSONObject;
    Resolution: string;
    ResolvedBy: string;
    ResolvedAt: TDateTime;
  public
    function ToJSON: TJSONObject;
    procedure FromJSON(const JSON: TJSONObject);
  end;

  TSyncProgress = class
  private
    TotalEntities: Integer;
    CompletedEntities: Integer;
    FailedEntities: Integer;
    CurrentEntity: string;
    StartTime: TDateTime;
    EndTime: TDateTime;
    BytesTransferred: Int64;
  public
    function ToJSON: TJSONObject;
    procedure FromJSON(const JSON: TJSONObject);
    function GetProgress: Double;
  end;

  TSyncOptions = class
  private
    SyncType: TSyncType;
    Direction: TSyncDirection;
    BatchSize: Integer;
    Timeout: Integer;
    RetryCount: Integer;
    ConflictResolution: string;
    Tables: TArray<string>;
    MaxChanges: Integer;
    CompressData: Boolean;
    ValidateHashes: Boolean;
  public  
    function ToJSON: TJSONObject;
    procedure FromJSON(const JSON: TJSONObject);
  end;

implementation

uses
   System.Rtti,

   uLib.Helpers;

{ TSyncEntity }

function TSyncEntity.ToJSON: TJSONObject;
begin
  Result := TJSONObject.Create;
  Result.AddPair('id', ID);
  Result.AddPair('table_name', TableName);
  Result.AddPair('last_sync', ISODateTimeToString(LastSync));
  Result.AddPair('last_hash', LastHash);
  Result.AddPair('version', TJSONNumber.Create(Version));
  Result.AddPair('state', TRttiEnumerationType.GetName(State));
  Result.AddPair('has_conflicts', TJSONBool.Create(HasConflicts));
  if not ErrorMessage.IsEmpty then
    Result.AddPair('error_message', ErrorMessage);
end;

procedure TSyncEntity.FromJSON(const JSON: TJSONObject);
begin
  ID := JSON.GetValue<string>('id');
  TableName := JSON.GetValue<string>('table_name');
  LastSync := ISO8601ToDate(JSON.GetValue<string>('last_sync'));
  LastHash := JSON.GetValue<string>('last_hash');
  Version := JSON.GetValue<Integer>('version');
  State := TRttiEnumerationType.GetValue<TSyncState>(JSON.GetValue<string>('state'));
  HasConflicts := JSON.GetValue<Boolean>('has_conflicts');
  ErrorMessage := JSON.GetValue<string>('error_message', '');
end;

{ TSyncConflict }

function TSyncConflict.ToJSON: TJSONObject;
begin
  Result := TJSONObject.Create;
  Result.AddPair('entity_id', EntityID);
  Result.AddPair('table_name', TableName);
  Result.AddPair('local_version', TJSONNumber.Create(LocalVersion));
  Result.AddPair('remote_version', TJSONNumber.Create(RemoteVersion));
  
  if Assigned(LocalData) then
    Result.AddPair('local_data', LocalData.Clone as TJSONObject);
    
  if Assigned(RemoteData) then
    Result.AddPair('remote_data', RemoteData.Clone as TJSONObject);
    
  Result.AddPair('resolution', Resolution);
  Result.AddPair('resolved_by', ResolvedBy);
  
  if ResolvedAt > 0 then
    Result.AddPair('resolved_at', ISODateTimeToString(ResolvedAt));
end;

procedure TSyncConflict.FromJSON(const JSON: TJSONObject);
begin
  EntityID := JSON.GetValue<string>('entity_id');
  TableName := JSON.GetValue<string>('table_name');
  LocalVersion := JSON.GetValue<Integer>('local_version');
  RemoteVersion := JSON.GetValue<Integer>('remote_version');
  
  var LocalObj := JSON.GetValue<TJSONObject>('local_data');
  if Assigned(LocalObj) then
    LocalData := LocalObj.Clone as TJSONObject;
    
  var RemoteObj := JSON.GetValue<TJSONObject>('remote_data');
  if Assigned(RemoteObj) then
    RemoteData := RemoteObj.Clone as TJSONObject;
    
  Resolution := JSON.GetValue<string>('resolution');
  ResolvedBy := JSON.GetValue<string>('resolved_by');
  
  var ResolvedAtStr := JSON.GetValue<string>('resolved_at');
  if not ResolvedAtStr.IsEmpty then
    ResolvedAt := ISO8601ToDate(ResolvedAtStr);
end;

{ TSyncProgress }

function TSyncProgress.ToJSON: TJSONObject;
begin
  Result := TJSONObject.Create;
  Result.AddPair('total_entities', TJSONNumber.Create(TotalEntities));
  Result.AddPair('completed_entities', TJSONNumber.Create(CompletedEntities));
  Result.AddPair('failed_entities', TJSONNumber.Create(FailedEntities));
  Result.AddPair('current_entity', CurrentEntity);
  Result.AddPair('start_time', ISODateTimeToString(StartTime));
  
  if EndTime > 0 then
    Result.AddPair('end_time', ISODateTimeToString(EndTime));
    
  Result.AddPair('bytes_transferred', TJSONNumber.Create(BytesTransferred));
  Result.AddPair('progress', TJSONNumber.Create(GetProgress));
end;

procedure TSyncProgress.FromJSON(const JSON: TJSONObject);
begin
  TotalEntities := JSON.GetValue<Integer>('total_entities');
  CompletedEntities := JSON.GetValue<Integer>('completed_entities');
  FailedEntities := JSON.GetValue<Integer>('failed_entities');
  CurrentEntity := JSON.GetValue<string>('current_entity');
  StartTime := ISO8601ToDate(JSON.GetValue<string>('start_time'));
  
  var EndTimeStr := JSON.GetValue<string>('end_time');
  if not EndTimeStr.IsEmpty then
    EndTime := ISO8601ToDate(EndTimeStr);
    
  BytesTransferred := JSON.GetValue<Int64>('bytes_transferred');
end;

function TSyncProgress.GetProgress: Double;
begin
  if TotalEntities > 0 then
    Result := (CompletedEntities / TotalEntities) * 100
  else
    Result := 0;
end;

{ TSyncOptions }

function TSyncOptions.ToJSON: TJSONObject;
begin
  Result := TJSONObject.Create;
  Result.AddPair('sync_type', TRttiEnumerationType.GetName(SyncType));
  Result.AddPair('direction', TRttiEnumerationType.GetName(Direction));
  Result.AddPair('batch_size', TJSONNumber.Create(BatchSize));
  Result.AddPair('timeout', TJSONNumber.Create(Timeout));
  Result.AddPair('retry_count', TJSONNumber.Create(RetryCount));
  Result.AddPair('conflict_resolution', ConflictResolution);
  
  var TablesArray := TJSONArray.Create;
  for var Table in Tables do
    TablesArray.Add(Table);
  Result.AddPair('tables', TablesArray);
  
  Result.AddPair('max_changes', TJSONNumber.Create(MaxChanges));
  Result.AddPair('compress_data', TJSONBool.Create(CompressData));
  Result.AddPair('validate_hashes', TJSONBool.Create(ValidateHashes));
end;

procedure TSyncOptions.FromJSON(const JSON: TJSONObject);
begin
  SyncType := TRttiEnumerationType.GetValue<TSyncType>(JSON.GetValue<string>('sync_type'));
  Direction := TRttiEnumerationType.GetValue<TSyncDirection>(JSON.GetValue<string>('direction'));
  BatchSize := JSON.GetValue<Integer>('batch_size', 100);
  Timeout := JSON.GetValue<Integer>('timeout', 30);
  RetryCount := JSON.GetValue<Integer>('retry_count', 3);
  ConflictResolution := JSON.GetValue<string>('conflict_resolution', 'server_wins');
  
  var TablesArray := JSON.GetValue<TJSONArray>('tables');
  if Assigned(TablesArray) then
  begin
    SetLength(Tables, TablesArray.Count);
    for var I := 0 to TablesArray.Count - 1 do
      Tables[I] := TablesArray.Items[I].Value;
  end;
  
  MaxChanges := JSON.GetValue<Integer>('max_changes', 1000);
  CompressData := JSON.GetValue<Boolean>('compress_data', True);
  ValidateHashes := JSON.GetValue<Boolean>('validate_hashes', True);
end;

end.