unit uLib.Database.Connection;

interface

uses
  System.SysUtils, System.Classes, System.SyncObjs, System.Variants,
  Data.DB, FireDAC.Comp.Client, FireDAC.Stan.Def, FireDAC.Stan.Intf,
  FireDAC.Stan.Async, FireDAC.Phys.Intf, FireDAC.Stan.Error,
  FireDAC.DApt, FireDAC.Stan.Param, FireDAC.Stan.Option,
  System.JSON, System.Generics.Collections, System.Diagnostics, System.Rtti,

  uLib.Database.Types,
  uLib.Logger;

type
  TBaseConnection = class(TInterfacedObject, IDBConnection)
  private
    FConnection: TFDConnection;
    FConfig: TDBConnectionConfig;
    FConnectionState: TConnectionState;
    FLastError: string;
    FLock: TCriticalSection;
    FMonitor: IDBMonitor;
    FTransactionCount: Integer;
    FCurrentQueryTimeoutMs: Integer; // Stored in milliseconds

    procedure SetConnectionState(ANewState: TConnectionState);
    procedure HandleFireDACException(
               const AOperation: string;
               E: Exception;
               const ASQL: string = '');
    procedure ConfigureConnectionFromConfig; virtual;

    function ExecuteWithRetry(
              const AOperationName: string;
              const ARetryableFunc: TFunc<Boolean>;
              const ASQLForLog: string = ''): Boolean;

  protected
    function GetDriverSpecificConnectionString: string; virtual; abstract;
    procedure ApplyDriverSpecificSettings; virtual;

    procedure DoBeforeConnect; virtual;
    procedure DoAfterConnect; virtual;
    procedure DoBeforeDisconnect; virtual;
    procedure DoAfterDisconnect; virtual;

    function CreateQueryComponent: TFDQuery;
    procedure PrepareQueryComponent(
                AQuery: TFDQuery;
                const ASQL: string;
                AParams: TFDParams=Nil);
    function InternalExecuteUpdate(AQuery: TFDQuery): Integer;
    function InternalExecuteQuery(AQuery: TFDQuery): TDataSet; // Returns a TFDQuery, caller might own it.

    property NativeConnectionObject: TFDConnection read FConnection;

  public
    constructor Create(const AConfig: TDBConnectionConfig; AMonitor: IDBMonitor = nil);
    destructor Destroy; override;

    // IDBConnection
    function GetState: TConnectionState;
    function GetLastError: string;
    function GetNativeConnection: TObject;

    function Connect: Boolean;
    procedure Disconnect;
    function IsConnected: Boolean;

    property Config: TDBConnectionConfig read FConfig; // Expose config record
    function InTransaction: Boolean;
    procedure StartTransaction;
    procedure Commit;
    procedure Rollback;

    function Execute(const SQL: string; Params: TFDParams=Nil): Integer;
    function ExecuteScalar(const SQL: string; Params: TFDParams=Nil): Variant;
    function ExecuteReader(const SQL: string; Params: TFDParams=Nil): TDataSet; // Caller frees DataSet
    function ExecuteJSON(const SQL: string; Params: TFDParams=Nil): string;

    function GetTables: TStrings; // Caller frees TStrings
    function GetFields(const TableName: string): TStrings; // Caller frees TStrings
    function GetVersion: string; virtual;

    procedure SetQueryTimeout(const AValue: Integer); // Seconds
    function GetQueryTimeout: Integer; // Seconds
    // End IDBConnection

    property Monitor: IDBMonitor read FMonitor write FMonitor;
    property ConnectionConfigName: string read FConfig.Name;
  end;

implementation

uses
  System.DateUtils, FireDAC.Stan.Util, Data.FireDACJSONReflect,
  FireDAC.Comp.DataSet, System.StrUtils;

{ TBaseConnection }

constructor TBaseConnection.Create(const AConfig: TDBConnectionConfig; AMonitor: IDBMonitor = nil);
begin
  inherited Create;
  FConfig := AConfig;
  FConfig.Validate; // Ensure config is valid

  FMonitor := AMonitor;
  FConnectionState := csNew;
  FLock := TCriticalSection.Create;
  FTransactionCount := 0;
  FCurrentQueryTimeoutMs := FConfig.CommandTimeout * 1000; // Convert seconds to milliseconds

  FConnection := TFDConnection.Create(nil); // No owner, managed by this class
  FConnection.LoginPrompt := False;

  ConfigureConnectionFromConfig; // Sets up TFDConnection based on FConfig
  LogMessage(Format('TBaseConnection created for config "%s". Initial state: %s. DBType: %s',
    [FConfig.Name, TRttiEnumerationType.GetName<TConnectionState>(FConnectionState), TRttiEnumerationType.GetName<TDBType>(FConfig.DBType)]), logDebug);
end;

destructor TBaseConnection.Destroy;
begin
  try
    if InTransaction then
    begin
      LogMessage(Format('Connection "%s" (Config: %s) destroyed with active transaction. Attempting rollback.', [Self.ClassName, FConfig.Name]), logWarning);
      Rollback; // Attempt to rollback if destroyed mid-transaction
    end;
    if IsConnected then
      Disconnect; // Ensure disconnection
  finally
    FreeAndNil(FConnection);
    FreeAndNil(FLock);
    LogMessage(Format('TBaseConnection for config "%s" destroyed.', [FConfig.Name]), logDebug);
    inherited;
  end;
end;

procedure TBaseConnection.ConfigureConnectionFromConfig;
begin
  // General FireDAC settings
  FConnection.FetchOptions.AssignedValues := [evMode, evRowsetSize, evAutoClose, evRecordCountMode, evCursorKind];
  FConnection.FetchOptions.Mode := fmAll;
  FConnection.FetchOptions.RowsetSize := 50;
  FConnection.FetchOptions.AutoClose := False;
  FConnection.FetchOptions.RecordCountMode := cmTotal;
  FConnection.FetchOptions.CursorKind := ckDefault;

  FConnection.ResourceOptions.AssignedValues := [rvCmdExecTimeout, rvAutoReconnect, rvDirectExecute];
  FConnection.ResourceOptions.CmdExecTimeout := FConfig.CommandTimeout * 1000; // Milliseconds
  // FConnection.ResourceOptions.ConnectTimeout is set by FireDAC based on driver/OS, or specific params in connection string
  FConnection.ResourceOptions.AutoReconnect := FConfig.RetryAttempts > 0;
  FConnection.ResourceOptions.DirectExecute := True;

  FConnection.TxOptions.AutoCommit := True;
  FConnection.TxOptions.EnableNested := False;
  FConnection.TxOptions.Isolation := xiReadCommitted;
  FConnection.ConnectionString := GetDriverSpecificConnectionString;
end;

procedure TBaseConnection.SetConnectionState(ANewState: TConnectionState);
var
  OldState: TConnectionState;
begin
  FLock.Acquire;
  try
    if FConnectionState <> ANewState then
    begin
      OldState := FConnectionState;
      FConnectionState := ANewState;
      if Assigned(FMonitor) then
        FMonitor.TrackConnectionStateChange(FConfig.Name + '_' + Self.ClassName, FConnectionState, FConfig.Name);
      LogMessage(Format('Connection "%s" state changed: %s -> %s', [FConfig.Name,
        TRttiEnumerationType.GetName<TConnectionState>(OldState),
        TRttiEnumerationType.GetName<TConnectionState>(FConnectionState)]), logDebug);
    end;
  finally
    FLock.Release;
  end;
end;

function TBaseConnection.GetState: TConnectionState;
begin
  FLock.Acquire;
  try
    Result := FConnectionState;
  finally
    FLock.Release;
  end;
end;

function TBaseConnection.GetLastError: string;
begin
  FLock.Acquire;
  try
    Result := FLastError;
  finally
    FLock.Release;
  end;
end;

function TBaseConnection.GetNativeConnection: TObject;
begin
  Result := FConnection;
end;

procedure TBaseConnection.HandleFireDACException(const AOperation: string; E: Exception; const ASQL: string = '');
var
  ErrorMsg: string;
  FDEx: EFDDBEngineException;
begin
  ErrorMsg := Format('FireDAC Exception during [%s] on connection "%s" (Config: %s): %s - %s',
    [AOperation, Self.ClassName, FConfig.Name, E.ClassName, E.Message]);

  if E is EFDDBEngineException then
  begin
    FDEx := E as EFDDBEngineException;
    ErrorMsg := ErrorMsg + Format(' (Kind: %s, Code: %d, Server Msg: "%s")',
      [TRttiEnumerationType.GetName<TFDCommandExceptionKind>(FDEx.Kind), FDEx.ErrorCode, FDEx.Message.TrimRight]);

    case FDEx.Kind of
      ekServerGone, ekCmdAborted, ekUserPwdInvalid, ekUserPwdExpired:
        SetConnectionState(csError);
    else
      if FConnectionState <> csConnected then
      begin
        LogMessage(Format('HandleFireDACException: Operational FDEx.Kind (%s) but connection was not in csConnected state (was %s). Setting to csError.',
          [TRttiEnumerationType.GetName<TFDCommandExceptionKind>(FDEx.Kind), TRttiEnumerationType.GetName<TConnectionState>(FConnectionState)]), logWarning);
        SetConnectionState(csError);
      end
      else
        LogMessage(Format('HandleFireDACException: Operational FDEx.Kind (%s). Connection state %s maintained.',
          [TRttiEnumerationType.GetName<TFDCommandExceptionKind>(FDEx.Kind), TRttiEnumerationType.GetName<TConnectionState>(FConnectionState)]), logDebug);
    end;
  end
  else
    SetConnectionState(csError);

  FLock.Acquire;
  try
    FLastError := ErrorMsg;
  finally
    FLock.Release;
  end;

  LogMessage(FLastError, logError);
  if Assigned(FMonitor) then
    FMonitor.TrackError(E, IfThen(ASQL <> '', ASQL, AOperation), FConfig.Name);
end;

function TBaseConnection.ExecuteWithRetry(const AOperationName: string; const ARetryableFunc: TFunc<Boolean>; const ASQLForLog: string = ''): Boolean;
var
  iAttempts: Integer;
  LSuccess: Boolean;
  LDelayStopwatch: TStopwatch;
  LastException: Exception;
begin
  Result := False;
  LastException := nil;

  for iAttempts := 0 to FConfig.RetryAttempts do
  begin
    LSuccess := False;
    try
      LSuccess := ARetryableFunc();
      if LSuccess then
      begin
        Result := True;
        LastException := nil; // Clear last exception on success
        Break; // Success, exit loop
      end;
      // If ARetryableFunc returned False without an exception
      LogMessage(Format('Operation "%s" (SQL: %s) for config "%s" returned False on attempt %d of %d.',
        [AOperationName, IfThen(ASQLForLog<>'', ASQLForLog, 'N/A'), FConfig.Name, iAttempts + 1, FConfig.RetryAttempts + 1]), logWarning);

    except
      on E: Exception do
      begin
        LastException := E; // Store the exception
        LogMessage(Format('Exception in operation "%s" (SQL: %s) for config "%s", attempt %d of %d: %s - %s',
          [AOperationName, IfThen(ASQLForLog<>'', ASQLForLog, 'N/A'), FConfig.Name, iAttempts + 1, FConfig.RetryAttempts + 1, E.ClassName, E.Message]), logWarning);
        // If this is the last attempt, HandleFireDACException will be called outside this loop, or by the caller.
        // No: if it's the last attempt and an exception occurred, call HandleFireDACException here.
      end;
    end; // end try-except

    // If not successful and not the last attempt, delay before retrying
    if (not LSuccess) and (iAttempts < FConfig.RetryAttempts) then
    begin
      LogMessage(Format('Retrying operation "%s" for config "%s" in %d ms... (Attempt %d/%d)',
        [AOperationName, FConfig.Name, FConfig.RetryDelayMs, iAttempts + 2, FConfig.RetryAttempts + 1]), logInfo);
      LDelayStopwatch := TStopwatch.StartNew;
      while LDelayStopwatch.ElapsedMilliseconds < FConfig.RetryDelayMs do
        Sleep(10);
      LDelayStopwatch.Stop;
    end
    else if (not LSuccess) and (iAttempts = FConfig.RetryAttempts) then // Last attempt failed
    begin
      if Assigned(LastException) then // If failure was due to an exception
      begin
        HandleFireDACException(AOperationName, LastException, ASQLForLog);
        // Result remains False (already default)
      end
      else // Failure was ARetryableFunc returning False
      begin
        FLastError := Format('Operation "%s" (SQL: %s) for config "%s" failed after %d attempts (returned False).',
          [AOperationName, IfThen(ASQLForLog<>'', ASQLForLog, 'N/A'), FConfig.Name, FConfig.RetryAttempts + 1]);
        LogMessage(FLastError, logError);
        SetConnectionState(csError); // Mark as error if all retries failed by returning False
      end;
      Exit; // Exit function, Result is False
    end;
  end; // end for loop
end;

procedure TBaseConnection.DoBeforeConnect;
begin
  // Hook for descendant classes
end;

procedure TBaseConnection.DoAfterConnect;
begin
  ApplyDriverSpecificSettings;
end;

procedure TBaseConnection.DoBeforeDisconnect;
begin
  // Hook for descendant classes
end;

procedure TBaseConnection.DoAfterDisconnect;
begin
  // Hook for descendant classes
end;

function TBaseConnection.Connect: Boolean;
begin
  FLock.Acquire;
  try
    if IsConnected then
    begin
      Result := True;
      Exit;
    end;

    SetConnectionState(csConnecting);
    LogMessage(Format('Attempting to connect to DB "%s" for config "%s"...',
          [FConfig.Database, FConfig.Name]), logInfo);
    DoBeforeConnect;

    try
      Result := ExecuteWithRetry('Connect',
        function: Boolean
        begin
          FConnection.Connected := True; // Attempt to connect
          Result := FConnection.Connected;
        end
      );

      if Result then
      begin
        SetConnectionState(csConnected);
        DoAfterConnect;
        LogMessage(Format('Successfully connected to DB "%s" for config "%s". Server Version: %s',
           [FConfig.Database, FConfig.Name, GetVersion]), logInfo);
      end
      else
      begin
        // FLastError should have been set by ExecuteWithRetry if it failed after all attempts
        if FLastError = '' then // Safety net if ExecuteWithRetry logic missed setting FLastError
           FLastError := Format('Failed to connect to DB "%s" for config "%s" after retries (no specific exception logged by retry).', [FConfig.Database, FConfig.Name]);
        SetConnectionState(csError); // Ensure state is error
        LogMessage(FLastError, logError); // Log the final error state
      end;
    except
      on E: Exception do // Catches exceptions from DoBeforeConnect or other unexpected issues
      begin
        Result := False;
        HandleFireDACException('Connect (outer try)', E);
      end;
    end;
  finally
    FLock.Release;
  end;
end;

procedure TBaseConnection.Disconnect;
begin
  FLock.Acquire;
  try
    if (FConnectionState = csClosed) and (not FConnection.Connected) then Exit;

    LogMessage(Format('Disconnecting from DB "%s" for config "%s"...',
         [FConfig.Database, FConfig.Name]), logInfo);
    DoBeforeDisconnect;
    try
      if FConnection.Connected then
        FConnection.Connected := False;
      SetConnectionState(csClosed);
      DoAfterDisconnect;
      LogMessage(Format('Disconnected from DB "%s" for config "%s".',
         [FConfig.Database, FConfig.Name]), logInfo);
    except
      on E: Exception do
      begin
        HandleFireDACException('Disconnect', E);
        SetConnectionState(csError);
      end;
    end;
  finally
    FLock.Release;
  end;
end;

function TBaseConnection.IsConnected: Boolean;
begin
  FLock.Acquire;
  try
    Result := (FConnectionState = csConnected) and Assigned(FConnection) and FConnection.Connected;
  finally
    FLock.Release;
  end;
end;

function TBaseConnection.InTransaction: Boolean;
begin
  FLock.Acquire;
  try
    Result := (FTransactionCount > 0) and Assigned(FConnection) and FConnection.InTransaction;
  finally
    FLock.Release;
  end;
end;

procedure TBaseConnection.StartTransaction;
begin
  FLock.Acquire;
  try
    if not IsConnected then
      if not Connect then
        raise EDBConnectionError.CreateFmt('Cannot start transaction for config "%s": Failed to connect to database.', [FConfig.Name]);

    if FTransactionCount = 0 then
    begin
      LogMessage(Format('Starting transaction for connection config "%s"...', [FConfig.Name]), logDebug);
      FConnection.StartTransaction;
    end
    else
      LogMessage(Format('Incrementing transaction counter for config "%s". Current depth: %d.', [FConfig.Name, FTransactionCount]), logDebug);

    Inc(FTransactionCount);
  except
    on E: Exception do
    begin
      FLock.Release; // Release lock before re-raising
      HandleFireDACException('StartTransaction', E);
      raise EDBCommandError.CreateFmt('Error starting transaction for config "%s": %s', [FConfig.Name, E.Message]);
    end;
  end;
  FLock.Release; // Normal release
end;

procedure TBaseConnection.Commit;
begin
  FLock.Acquire;
  try
    if FTransactionCount > 0 then
    begin
      Dec(FTransactionCount);
      if FTransactionCount = 0 then
      begin
        LogMessage(Format('Committing transaction for connection config "%s"...', [FConfig.Name]), logDebug);
        FConnection.Commit;
      end
      else
        LogMessage(Format('Decremented transaction counter for config "%s". Remaining depth: %d.', [FConfig.Name, FTransactionCount]), logDebug);
    end
    else
      LogMessage(Format('Commit called on connection config "%s" without an active transaction (FTransactionCount is 0).', [FConfig.Name]), logWarning);
  except
    on E: Exception do
    begin
      FLock.Release; // Release lock before re-raising
      HandleFireDACException('Commit', E);
      raise EDBCommandError.CreateFmt('Error committing transaction for config "%s": %s', [FConfig.Name, E.Message]);
    end;
  end;
  FLock.Release; // Normal release
end;

procedure TBaseConnection.Rollback;
begin
  FLock.Acquire;
  try
    if FTransactionCount > 0 then
    begin
      LogMessage(Format('Rolling back transaction for connection config "%s"... (FTransactionCount: %d)', [FConfig.Name, FTransactionCount]), logDebug);
      if FConnection.InTransaction then
        FConnection.Rollback;
      FTransactionCount := 0;
    end
    else
      LogMessage(Format('Rollback called on connection config "%s" without an active transaction (FTransactionCount is 0).', [FConfig.Name]), logWarning);
  except
    on E: Exception do
    begin
      FLock.Release; // Release lock before re-raising
      HandleFireDACException('Rollback', E);
      raise EDBCommandError.CreateFmt('Error rolling back transaction for config "%s": %s', [FConfig.Name, E.Message]);
    end;
  end;
  FLock.Release; // Normal release
end;

function TBaseConnection.CreateQueryComponent: TFDQuery;
begin
  Result := TFDQuery.Create(nil);
  Result.Connection := Self.FConnection;
  Result.ResourceOptions.CmdExecTimeout := FCurrentQueryTimeoutMs;
  Result.FetchOptions.Assign(FConnection.FetchOptions);
end;

procedure TBaseConnection.PrepareQueryComponent(AQuery: TFDQuery; const ASQL: string;
  AParams: TFDParams=Nil);
begin
  AQuery.SQL.Text := ASQL;
  If AParams<>Nil then
     begin
       AQuery.Params:=AParams;
       AQuery.Prepare;
     end;
end;

function TBaseConnection.InternalExecuteUpdate(AQuery: TFDQuery): Integer;
var
  StartTime: TStopwatch;
  SQLText: string;
begin
  Result := -1;
  if not IsConnected then
    if not Connect then
      raise EDBConnectionError.CreateFmt('Cannot execute update for config "%s": Failed to connect to database.', [FConfig.Name]);

  SQLText := AQuery.SQL.Text;
  StartTime := TStopwatch.StartNew;
  try
    Result := AQuery.ExecSQL(True);
    if Assigned(FMonitor) then
      FMonitor.TrackCommand(SQLText, StartTime.ElapsedMilliseconds, FConfig.Name);
  except
    on E: Exception do
    begin
      HandleFireDACException('InternalExecuteUpdate', E, SQLText);
      raise EDBCommandError.CreateFmt('Error executing update for config "%s": %s. SQL: %s', [FConfig.Name, E.Message, SQLText]);
    end;
  end;
end;

function TBaseConnection.InternalExecuteQuery(AQuery: TFDQuery): TDataSet;
var
  StartTime: TStopwatch;
  SQLText: string;
begin
  Result := nil; // Initialize result
  if not IsConnected then
    if not Connect then
      // AQuery is owned by the caller (e.g., ExecuteReader), so it shouldn't be freed here if Connect fails.
      // The caller's finally block should handle it.
      raise EDBConnectionError.CreateFmt('Cannot execute query for config "%s": Failed to connect to database.', [FConfig.Name]);

  SQLText := AQuery.SQL.Text;
  StartTime := TStopwatch.StartNew;
  try
    AQuery.Open();
    if Assigned(FMonitor) then
      FMonitor.TrackCommand(SQLText, StartTime.ElapsedMilliseconds, FConfig.Name);
    Result := AQuery; // Return the opened TFDQuery; caller is responsible for freeing it
  except
    on E: Exception do
    begin
      HandleFireDACException('InternalExecuteQuery', E, SQLText);
      // Do not free AQuery here. The caller (ExecuteReader) is responsible for freeing it in its finally block.
      raise EDBCommandError.CreateFmt('Error executing query for config "%s": %s. SQL: %s', [FConfig.Name, E.Message, SQLText]);
    end;
  end;
end;

function TBaseConnection.Execute(const SQL: string; Params: TFDParams=Nil): Integer;
var
  Query: TFDQuery;
begin
  Result := -1;
  Query := CreateQueryComponent;
  try
    PrepareQueryComponent(Query, SQL, Params);
    Result := InternalExecuteUpdate(Query);
  finally
    Query.Free;
  end;
end;

function TBaseConnection.ExecuteScalar(const SQL: string; Params: TFDParams=Nil): Variant;
var
  Query: TFDQuery;
begin
  Result := Null;
  Query := CreateQueryComponent;
  try
    PrepareQueryComponent(Query, SQL, Params);
    InternalExecuteQuery(Query); // This opens Query
    try
      if not Query.Eof then
        Result := Query.Fields[0].Value
      else
        Result := Null;
    finally
      // Query is opened by InternalExecuteQuery. It needs to be closed if not already.
      // Freeing it will close it.
    end;
  finally
    Query.Free; // Ensure TFDQuery is always freed
  end;
end;

function TBaseConnection.ExecuteReader(const SQL: string; Params: TFDParams=Nil): TDataSet;
var
  Query: TFDQuery;
begin
  Result := nil; // Initialize Result
  Query := CreateQueryComponent;
  try
    PrepareQueryComponent(Query, SQL, Params);
    Result := InternalExecuteQuery(Query); // If successful, Result points to Query.
                                         // Query should NOT be freed here by this method.
  except
    on E: Exception do
    begin
      FreeAndNil(Query); // If anything fails before or during InternalExecuteQuery that doesn't return Query, free it.
      Result := nil;
      raise; // Re-raise the original or wrapped exception
    end;
  end;
  // If successful, Query (now Result) is owned by the caller.
end;

function TBaseConnection.ExecuteJSON(const SQL: string; Params: TFDParams=Nil): string;
var
  ReaderDataSet: TDataSet;
  MemTable: TFDMemTable;
  Stream: TStringStream;
begin
  Result := '[]';
  ReaderDataSet := nil;
  MemTable := nil;
  Stream := nil;

  try
    ReaderDataSet := ExecuteReader(SQL, Params); // Gets an active TFDQuery. This method now owns it.

    if Assigned(ReaderDataSet) then // Check if ExecuteReader succeeded
    begin
      if ReaderDataSet.RecordCount > 0 then // Check if there's data
      begin
        MemTable := TFDMemTable.Create(nil);
        try
          MemTable.CopyDataSet(ReaderDataSet, [coStructure, coAppend]); // Ensure coData is included

          if MemTable.RecordCount > 0 then // Double check after copy
          begin
            Stream := TStringStream.Create('', TEncoding.UTF8);
            try
              MemTable.SaveToStream(Stream, sfJSON);
              Result := Stream.DataString;
            finally
              FreeAndNil(Stream);
            end;
          end; // else Result remains '[]'
        finally
          FreeAndNil(MemTable);
        end;
      end; // else Result remains '[]'
    end; // else ExecuteReader failed or returned no dataset, Result remains '[]'
  finally
    if Assigned(ReaderDataSet) then // Free the dataset obtained from ExecuteReader
      FreeAndNil(ReaderDataSet);
  end;
end;

function TBaseConnection.GetTables: TStrings;
var
  List: TStringList;
begin
  Result := nil;
  if not IsConnected then
    if not Connect then
      raise EDBConnectionError.CreateFmt('Cannot get tables for config "%s": Failed to connect to database.', [FConfig.Name]);

  List := TStringList.Create;
  try
    FConnection.GetTableNames(FConfig.Database, Config.Schema, '', List, [osMy], [tkTable, tkView]);
    Result := List; // Transfer ownership
  except
    on E: Exception do
    begin
      FreeAndNil(List);
      HandleFireDACException('GetTables', E);
      raise EDBCommandError.CreateFmt('Error getting tables for config "%s": %s', [FConfig.Name, E.Message]);
    end;
  end;
end;

function TBaseConnection.GetFields(const TableName: string): TStrings;
var
  List: TStringList;
begin
  Result := nil;
  if TableName.Trim.IsEmpty then
    raise EDBCommandError.Create('Cannot get fields: TableName cannot be empty.');

  if not IsConnected then
    if not Connect then
      raise EDBConnectionError.CreateFmt('Cannot get fields for table "%s" (config "%s"): Failed to connect to database.', [TableName, FConfig.Name]);

  List := TStringList.Create;
  try
    FConnection.GetFieldNames(FConfig.Database, Config.Schema, TableName, '', List);
    Result := List; // Transfer ownership
  except
    on E: Exception do
    begin
      FreeAndNil(List);
      HandleFireDACException('GetFields for ' + TableName, E);
      raise EDBCommandError.CreateFmt('Error getting fields for table "%s" (config "%s"): %s', [TableName, FConfig.Name, E.Message]);
    end;
  end;
end;

function TBaseConnection.GetVersion: string;
begin
  Result := 'N/A';

  if not IsConnected then
    if not Connect then
      raise EDBConnectionError.CreateFmt('Cannot get version for config "%s": Failed to connect to database.', [FConfig.Name]);
  try
    // Try a generic SQL standard way first if available, e.g. for some drivers.
    // However, SELECT version() is common for PostgreSQL and MySQL.
    // For SQL Server, @@VERSION is used. This method is virtual and overridden by descendants.
    // This base implementation can be a very generic fallback or rely on overrides.
    (*
    if Assigned(FConnection.ConnectionIntf) then
    begin
      Result := FConnection.ConnectionIntf.GetInfoStr(TFDPhysDriverInfoKind.diDBMSName) + ' ' +
                FConnection.ConnectionIntf.GetInfoStr(TFDPhysDriverInfoKind.diDBMSVersion);
      if Result.Trim = '' then // If not populated, try major/minor
      begin
        var MajorVer := FConnection.ConnectionIntf.GetInfoInt(TFDPhysDriverInfoKind.diDBMSMajorVer);
        var MinorVer := FConnection.ConnectionIntf.GetInfoInt(TFDPhysDriverInfoKind.diDBMSMinorVer);
        if MajorVer > 0 then
           Result := Format('%s %d.%d', [FConnection.ConnectionIntf.GetInfoStr(TFDPhysDriverInfoKind.diDBMSName), MajorVer, MinorVer])
        else
           Result := FConnection.ConnectionIntf.GetInfoStr(TFDPhysDriverInfoKind.diDBMSName) + ' (Version number not available via GetInfoInt)';
      end;
    end;
    *)
    if (Result.Trim = '') or SameText(Result.Trim, '(Version number not available via GetInfoInt)') then
       Result := 'N/A (Could not determine version via FireDAC GetInfo)';

  except
    on E: Exception do
    begin
      HandleFireDACException('GetVersion (Base)', E);
      Result := 'N/A (Error retrieving version: ' + E.Message + ')';
    end;
  end;
end;

procedure TBaseConnection.SetQueryTimeout(const AValue: Integer); // Seconds
begin
  FLock.Acquire;
  try
    if AValue > 0 then
      FCurrentQueryTimeoutMs := AValue * 1000
    else
      FCurrentQueryTimeoutMs := FConfig.CommandTimeout * 1000;
    LogMessage(Format('Query timeout for config "%s" set to %d ms.', [FConfig.Name, FCurrentQueryTimeoutMs]), logDebug);
  finally
    FLock.Release;
  end;
end;

function TBaseConnection.GetQueryTimeout: Integer; // Seconds
begin
  FLock.Acquire;
  try
    Result := FCurrentQueryTimeoutMs div 1000;
  finally
    FLock.Release;
  end;
end;

procedure TBaseConnection.ApplyDriverSpecificSettings;
begin
  LogMessage(Format('TBaseConnection.ApplyDriverSpecificSettings called for config "%s". No base settings to apply.', [FConfig.Name]), logDebug);
end;

end.
