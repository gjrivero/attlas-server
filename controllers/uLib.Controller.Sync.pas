unit uLib.Controller.Sync;

interface

uses
  System.SysUtils, System.Classes, System.JSON, System.Generics.Collections,
  IdCustomHTTPServer,
  FireDAC.Stan.Param,

  uLib.Controller.Base,
  uLib.Database.Types, // Para IDBConnection
  uLib.Routes,         // Para TRouteHandler
  uLib.Sync.Types,     // Para TSyncEntity y nombres de campo de referencia
  uLib.Logger;         // Added uLib.Logger

type
  TSyncResult = record
    SuccessCount: Integer;
    FailCount: Integer;
    ErrorMessages: TArray<string>;
    TotalProcessed: Integer;

    procedure AddError(const AError: string);
    function IsSuccessful: Boolean;
    function GetErrorSummary: string;
  end;

  TSyncController = class(TBaseController)
  private
    class var
      LastIdle, LastKernel, LastUser: UInt64;
      LastCheck: Cardinal;
      LastTotalCPU, LastIdleCPU: UInt64; // Para Linux
    class var FPreparedStatements: TDictionary<string, string>;
    class constructor Create;
    class destructor Destroy;

    // Métodos privados para optimización
    class function ProcessSyncBatch<T>(const AItemsArray: TJSONArray;
      const ATableName: string; const ABatchSize: Integer;
      AProcessItemFunc: TFunc<TJSONObject, T, Boolean>): TSyncResult;
    class function ValidateAndConvertSyncItem(const AItemObject: TJSONObject;
      const AEntityName: string; out AEntityID: string): Boolean;
    class procedure LogBatchProgress(const AOperation: string; ASuccessCount, AFailCount, ATotal: Integer);
  protected
    procedure GetDataInfo( Request: TIdHTTPRequestInfo;
                           Response: TIdHTTPResponseInfo;
                           const sQry: String;
                           Params: TFDParams);
  public
    class procedure RegisterRoutes; override;

    class procedure SyncTables(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
    class procedure SyncOrders(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
    class procedure SyncOrderItems(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
    class procedure SyncProducts(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);

    class procedure GetTableChanges(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
    class procedure GetOrderChanges(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
    class procedure GetOrderItemChanges(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
    class procedure GetProductChanges(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
  end;


const
  SYNC_DB_POOL_NAME = 'MainDB_PG';
  BATCH_SIZE = 250; // Tamaño óptimo de lote para transacciones
  MAX_ERRORS_TO_TRACK = 10; // Limitar errores almacenados para evitar memory leak

  // Prepared Statements optimizados
  SQL_CHECK_TABLE_EXISTS = 'SELECT COUNT(*) FROM tables WHERE id = $1';
  SQL_UPDATE_TABLE = 'UPDATE tables SET "Number" = $2, "Capacity" = $3, "Status" = $4, "QRCode" = $5, "LastSync" = CURRENT_TIMESTAMP WHERE id = $1';
  SQL_INSERT_TABLE = 'INSERT INTO tables (id, "Number", "Capacity", "Status", "QRCode", "LastSync") VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)';
  SQL_GET_TABLE_CHANGES = 'SELECT id, "Number", "Capacity", "Status", "QRCode", "LastSync" FROM tables WHERE "LastSync" > $1 ORDER BY "LastSync" LIMIT 1000';

  SQL_CHECK_ORDER_EXISTS = 'SELECT COUNT(*) FROM orders WHERE id = $1';
  SQL_UPDATE_ORDER = 'UPDATE orders SET "TableId" = $2, "Status" = $3, "StartTime" = $4, "EndTime" = $5, "Total" = $6, "WaiterId" = $7, "LastSync" = CURRENT_TIMESTAMP WHERE id = $1';
  SQL_INSERT_ORDER = 'INSERT INTO orders (id, "TableId", "Status", "StartTime", "EndTime", "Total", "WaiterId", "LastSync") VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)';
  SQL_GET_ORDER_CHANGES = 'SELECT id, "TableId", "Status", "StartTime", "EndTime", "Total", "WaiterId", "LastSync" FROM orders WHERE "LastSync" > $1 ORDER BY "LastSync" LIMIT 1000';

  SQL_CHECK_ORDER_ITEM_EXISTS = 'SELECT COUNT(*) FROM orderitems WHERE id = $1';
  SQL_UPDATE_ORDER_ITEM = 'UPDATE orderitems SET "OrderId" = $2, "ProductId" = $3, "Quantity" = $4, "Price" = $5, "Notes" = $6, "Status" = $7, "LastSync" = CURRENT_TIMESTAMP WHERE id = $1';
  SQL_INSERT_ORDER_ITEM = 'INSERT INTO orderitems (id, "OrderId", "ProductId", "Quantity", "Price", "Notes", "Status", "LastSync") VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)';
  SQL_GET_ORDER_ITEM_CHANGES = 'SELECT id, "OrderId", "ProductId", "Quantity", "Price", "Notes", "Status", "LastSync" FROM orderitems WHERE "LastSync" > $1 ORDER BY "LastSync" LIMIT 1000';

  SQL_CHECK_PRODUCT_EXISTS = 'SELECT COUNT(*) FROM products WHERE id = $1';
  SQL_UPDATE_PRODUCT = 'UPDATE products SET "Name" = $2, "Description" = $3, "Price" = $4, "Category" = $5, "Image" = $6, "Available" = $7, "LastSync" = CURRENT_TIMESTAMP WHERE id = $1';
  SQL_INSERT_PRODUCT = 'INSERT INTO products (id, "Name", "Description", "Price", "Category", "Image", "Available", "LastSync") VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)';
  SQL_GET_PRODUCT_CHANGES = 'SELECT id, "Name", "Description", "Price", "Category", "Image", "Available", "LastSync" FROM products WHERE "LastSync" > $1 ORDER BY "LastSync" LIMIT 1000';

implementation

uses
  System.DateUtils,
  System.Variants,
  System.StrUtils,
  System.Math;

{ TSyncResult }

procedure TSyncResult.AddError(const AError: string);
begin
  if Length(ErrorMessages) < MAX_ERRORS_TO_TRACK then
  begin
    SetLength(ErrorMessages, Length(ErrorMessages) + 1);
    ErrorMessages[High(ErrorMessages)] := AError;
  end;
end;

function TSyncResult.IsSuccessful: Boolean;
begin
  Result := (FailCount = 0) and (SuccessCount > 0);
end;

function TSyncResult.GetErrorSummary: string;
var
  i: Integer;
begin
  Result := '';
  if Length(ErrorMessages) > 0 then
  begin
    Result := 'Errors: ';
    for i := 0 to Min(High(ErrorMessages), 2) do // Solo mostrar primeros 3 errores
    begin
      if i > 0 then Result := Result + '; ';
      Result := Result + ErrorMessages[i];
    end;
    if Length(ErrorMessages) > 3 then
      Result := Result + Format(' (and %d more)', [Length(ErrorMessages) - 3]);
  end;
end;

{ TSyncController }

class constructor TSyncController.Create;
begin
  FPreparedStatements := TDictionary<string, string>.Create;
  // Cache de prepared statements para optimización
  FPreparedStatements.Add('tables_check', SQL_CHECK_TABLE_EXISTS);
  FPreparedStatements.Add('tables_update', SQL_UPDATE_TABLE);
  FPreparedStatements.Add('tables_insert', SQL_INSERT_TABLE);
  // Agregar más según sea necesario
end;

class destructor TSyncController.Destroy;
begin
  FreeAndNil(FPreparedStatements);
end;

// Helper mejorado para obtener enteros con validación y logging optimizado
function GetIntegerFieldOptimized(const AJSONObject: TJSONObject; const AFieldName: string;
  const AEntityIDForLog: string; ADefaultValue: Integer = 0): Integer;
var
  LJsonValue: TJSONValue;
  LStrValue: string;
begin
  Result := ADefaultValue;
  if Assigned(AJSONObject) and
     AJSONObject.TryGetValue(AFieldName, LJsonValue) and
     Assigned(LJsonValue) then
  begin
    case LJsonValue.ClassType.ClassInfo.Name of
      'TJSONNumber': Result := Round(TJSONNumber(LJsonValue).AsExtended);
      'TJSONString':
      begin
        LStrValue := TJSONString(LJsonValue).Value;
        if not TryStrToInt(LStrValue, Result) then
        begin
          Result := ADefaultValue;
          // Solo log de errores críticos para evitar spam
          if AEntityIDForLog <> '' then
            LogMessage(Format('Sync field conversion error for entity "%s": Field "%s" value "%s" invalid. Using default %d.',
              [AEntityIDForLog, AFieldName, LStrValue, ADefaultValue]), logError);
        end;
      end;
    end;
  end;
end;

// Helper mejorado para obtener floats con validación optimizada
function GetFloatFieldOptimized(const AJSONObject: TJSONObject; const AFieldName: string;
  const AEntityIDForLog: string; ADefaultValue: Double = 0.0): Double;
var
  LJsonValue: TJSONValue;
  LStrValue: string;
begin
  Result := ADefaultValue;
  if Assigned(AJSONObject) and AJSONObject.TryGetValue(AFieldName, LJsonValue) and Assigned(LJsonValue) then
  begin
    case LJsonValue.ClassType.ClassInfo.Name of
      'TJSONNumber': Result := TJSONNumber(LJsonValue).AsExtended;
      'TJSONString':
      begin
        LStrValue := TJSONString(LJsonValue).Value;
        if not TryStrToFloat(LStrValue, Result) then
        begin
          Result := ADefaultValue;
          if AEntityIDForLog <> '' then
            LogMessage(Format('Sync field conversion error for entity "%s": Field "%s" value "%s" invalid. Using default %f.',
              [AEntityIDForLog, AFieldName, LStrValue, ADefaultValue]), logError);
        end;
      end;
    end;
  end;
end;

function GetDateTimeFieldOptimized(const AJSONObject: TJSONObject; const AFieldName: string;
  const AEntityIDForLog: string; ADefaultValue: TDateTime = 0): TDateTime;
var
  LStrValue: string;
begin
  Result := ADefaultValue;
  LStrValue := GetStr(AJSONObject, AFieldName, '');
  if LStrValue <> '' then
  begin
    try
      Result := ISO8601ToDate(LStrValue);
    except
      on E: Exception do
      begin
        Result := ADefaultValue;
        if AEntityIDForLog <> '' then
          LogMessage(Format('Sync datetime conversion error for entity "%s": Field "%s" value "%s" invalid. Using default. Error: %s',
            [AEntityIDForLog, AFieldName, LStrValue, E.Message]), logError);
      end;
    end;
  end;
end;

class function TSyncController.ValidateAndConvertSyncItem(const AItemObject: TJSONObject;
  const AEntityName: string; out AEntityID: string): Boolean;
begin
  Result := False;
  AEntityID := '';

  if not Assigned(AItemObject) then
  begin
    LogMessage(Format('Sync validation error: %s item is nil', [AEntityName]), logError);
    Exit;
  end;

  AEntityID := GetStr(AItemObject, 'id');
  if AEntityID.IsEmpty then
  begin
    LogMessage(Format('Sync validation error: %s missing required "id" field', [AEntityName]), logError);
    Exit;
  end;

  Result := True;
end;

class procedure TSyncController.LogBatchProgress(const AOperation: string; ASuccessCount, AFailCount, ATotal: Integer);
begin
  // Solo log cada 100 items o al final para evitar spam
  if (ASuccessCount + AFailCount) mod 100 = 0 or (ASuccessCount + AFailCount = ATotal) then
    LogMessage(Format('%s batch progress: %d/%d processed (%d success, %d failed)',
      [AOperation, ASuccessCount + AFailCount, ATotal, ASuccessCount, AFailCount]), logInfo);
end;

class function TSyncController.ProcessSyncBatch<T>(const AItemsArray: TJSONArray;
  const ATableName: string; const ABatchSize: Integer;
  AProcessItemFunc: TFunc<TJSONObject, T, Boolean>): TSyncResult;
var
  DBConn: IDBConnection;
  I, BatchCount: Integer;
  ItemObject: TJSONObject;
  EntityID: string;
  CurrentBatchSuccess, CurrentBatchFail: Integer;
begin
  // Inicializar resultado
  FillChar(Result, SizeOf(TSyncResult), 0);
  SetLength(Result.ErrorMessages, 0);
  Result.TotalProcessed := AItemsArray.Count;

  DBConn := nil;
  BatchCount := 0;
  CurrentBatchSuccess := 0;
  CurrentBatchFail := 0;

  try
    DBConn := AcquireDBConnection(SYNC_DB_POOL_NAME);

    for I := 0 to AItemsArray.Count - 1 do
    begin
      // Iniciar transacción al comienzo de cada lote
      if BatchCount = 0 then
      begin
        DBConn.StartTransaction;
        CurrentBatchSuccess := 0;
        CurrentBatchFail := 0;
      end;

      try
        if not (AItemsArray.Items[I] is TJSONObject) then
        begin
          Inc(CurrentBatchFail);
          Result.AddError(Format('Item at index %d is not a JSON object', [I]));
          Continue;
        end;

        ItemObject := AItemsArray.Items[I] as TJSONObject;

        if not ValidateAndConvertSyncItem(ItemObject, ATableName, EntityID) then
        begin
          Inc(CurrentBatchFail);
          Result.AddError(Format('Validation failed for item at index %d', [I]));
          Continue;
        end;

        // Procesar el item usando la función pasada como parámetro
        if AProcessItemFunc(ItemObject, Default(T), True) then
          Inc(CurrentBatchSuccess)
        else
        begin
          Inc(CurrentBatchFail);
          Result.AddError(Format('Processing failed for entity %s', [EntityID]));
        end;

      except
        on E: Exception do
        begin
          Inc(CurrentBatchFail);
          Result.AddError(Format('Exception processing item %d: %s', [I, E.Message]));
        end;
      end;

      Inc(BatchCount);

      // Commit al final del lote o al final de todos los items
      if (BatchCount >= ABatchSize) or (I = AItemsArray.Count - 1) then
      begin
        try
          if (CurrentBatchSuccess > 0) and (CurrentBatchFail = 0) then
          begin
            DBConn.Commit;
            Inc(Result.SuccessCount, CurrentBatchSuccess);
          end
          else if CurrentBatchFail > 0 then
          begin
            DBConn.Rollback;
            Inc(Result.FailCount, CurrentBatchSuccess + CurrentBatchFail);
            LogMessage(Format('Batch rollback for %s: %d items failed', [ATableName, CurrentBatchFail]), logWarning);
          end;
        except
          on E: Exception do
          begin
            Inc(Result.FailCount, CurrentBatchSuccess + CurrentBatchFail);
            Result.AddError(Format('Transaction error: %s', [E.Message]));
            if DBConn.InTransaction then
              DBConn.Rollback;
          end;
        end;

        LogBatchProgress(ATableName, Result.SuccessCount, Result.FailCount, Result.TotalProcessed);
        BatchCount := 0;
      end;
    end;

  finally
    if Assigned(DBConn) and DBConn.InTransaction then
    begin
      try
        DBConn.Rollback;
      except
        on E: Exception do
          LogMessage(Format('Error rolling back transaction in finally: %s', [E.Message]), logError);
      end;
    end;
    ReleaseDBConnection(DBConn, SYNC_DB_POOL_NAME);
  end;
end;

procedure TSyncController.GetDataInfo( Request: TIdHTTPRequestInfo;
                                       Response: TIdHTTPResponseInfo;
                                       const sQry: String;
                                       Params: TFDParams);
var
  DBConn: IDBConnection;
  LastSyncStr: string;
  LastSync: TDateTime;
  JsonResultString: string;
begin
  DBConn := nil;
  JsonResultString := '[]';
  try
    LastSyncStr := Request.Params.Values['lastSync'];
    if LastSyncStr = '' then
      raise EMissingParameterException.Create('Missing "lastSync" query parameter for GetTableChanges.');

    try
      LastSync := ISO8601ToDate(LastSyncStr);
    except
      on E: Exception do
        raise EInvalidParameterException.CreateFmt('Invalid "lastSync" date format: "%s". Use ISO8601 format. Error: %s', [LastSyncStr, E.Message]);
    end;

    DBConn := AcquireDBConnection(SYNC_DB_POOL_NAME);
    Params := TFDParams.Create;
    Params.Add('lastSync', LastSync);

    JsonResultString := DBConn.ExecuteJSON(sQry, Params);

    Response.ResponseNo := 200;
    Response.ContentType := 'application/json';
    Response.ContentText := JsonResultString;
  except
    on E: Exception do
      HandleError(E, Response, Request);
  finally
    // CORRECCIÓN: SIEMPRE en finally
    ReleaseDBConnection(DBConn, SYNC_DB_POOL_NAME); // ← MOVER AQUÍ
  end;
end;

class procedure TSyncController.RegisterRoutes;
begin
  if not Assigned(FRouteManager) then
  begin
    LogMessage('CRITICAL: FRouteManager not assigned in TSyncController.', logError);
    Exit;
  end;

  FRouteManager.AddRoute('POST', 'sync/tables', SyncTables, True);
  FRouteManager.AddRoute('POST', 'sync/orders', SyncOrders, True);
  FRouteManager.AddRoute('POST', 'sync/orderitems', SyncOrderItems, True);
  FRouteManager.AddRoute('POST', 'sync/products', SyncProducts, True);

  FRouteManager.AddRoute('GET', 'sync/tables/changes', GetTableChanges, True);
  FRouteManager.AddRoute('GET', 'sync/orders/changes', GetOrderChanges, True);
  FRouteManager.AddRoute('GET', 'sync/orderitems/changes', GetOrderItemChanges, True);
  FRouteManager.AddRoute('GET', 'sync/products/changes', GetProductChanges, True);

  LogMessage('TSyncController routes registered successfully.', logInfo);
end;

class procedure TSyncController.SyncTables(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
var
  RequestBodyJSON: TJSONValue;
  RequestData: TJSONObject;
  TablesArray: TJSONArray;
  SyncResult: TSyncResult;
  ProcessTableFunc: TFunc<TJSONObject, Integer, Boolean>;
begin
  RequestBodyJSON := nil;

  try
    RequestBodyJSON := GetRequestBody(Request);
    if not (Assigned(RequestBodyJSON) and (RequestBodyJSON is TJSONObject) and ((RequestBodyJSON as TJSONObject).Count > 0)) then
      raise EInvalidRequestException.Create('Request body must be a valid, non-empty JSON object for SyncTables.');

    RequestData := RequestBodyJSON as TJSONObject;

    if not RequestData.TryGetValue('tables', TablesArray) or not Assigned(TablesArray) then
      raise EMissingParameterException.Create('Missing "tables" array in request body for SyncTables.');

    // Función para procesar cada tabla individual
    ProcessTableFunc := function(TableObject: TJSONObject; Dummy: Integer): Boolean
    var
      DBConn: IDBConnection;
      EntityID: string;
      TableIdForDB: Integer;
      Exists: Boolean;
      Params: TFDParams;
    begin
      Result := False;
      DBConn := nil;
      Params := nil;

      try
        EntityID := GetStr(TableObject, 'id');
        if not TryStrToInt(EntityID, TableIdForDB) then
          Exit;

        DBConn := AcquireDBConnection(SYNC_DB_POOL_NAME);
        Params := TFDParams.Create;

        // Check if exists
        Params.Clear;
        Params.Add('id', TableIdForDB);
        Exists := DBConn.ExecuteScalar(SQL_CHECK_TABLE_EXISTS, Params) > 0;

        // Update or Insert
        Params.Clear;
        if Exists then
        begin
          Params.Add('id', TableIdForDB);
          Params.Add('Number', GetIntegerFieldOptimized(TableObject, 'Number', EntityID));
          Params.Add('Capacity', GetIntegerFieldOptimized(TableObject, 'Capacity', EntityID));
          Params.Add('Status', GetStr(TableObject, 'Status', ''));
          Params.Add('QRCode', GetStr(TableObject, 'QRCode', ''));
          DBConn.Execute(SQL_UPDATE_TABLE, Params);
        end
        else
        begin
          Params.Add('id', TableIdForDB);
          Params.Add('Number', GetIntegerFieldOptimized(TableObject, 'Number', EntityID));
          Params.Add('Capacity', GetIntegerFieldOptimized(TableObject, 'Capacity', EntityID));
          Params.Add('Status', GetStr(TableObject, 'Status', ''));
          Params.Add('QRCode', GetStr(TableObject, 'QRCode', ''));
          DBConn.Execute(SQL_INSERT_TABLE, Params);
        end;

        Result := True;

      finally
        FreeAndNil(Params);
        ReleaseDBConnection(DBConn, SYNC_DB_POOL_NAME);
      end;
    end;

    // Procesar en lotes
    SyncResult := ProcessSyncBatch<Integer>(TablesArray, 'SyncTables', BATCH_SIZE, ProcessTableFunc);

    // Preparar respuesta
    Response.ResponseNo := 200;
    Response.ContentType := 'application/json';

    if SyncResult.IsSuccessful then
      Response.ContentText := Format('{"success":true, "message":"Tables synchronized successfully", "processed":%d, "success":%d}',
        [SyncResult.TotalProcessed, SyncResult.SuccessCount])
    else
      Response.ContentText := Format('{"success":false, "message":"Tables partially synchronized", "processed":%d, "success":%d, "failed":%d, "errors":"%s"}',
        [SyncResult.TotalProcessed, SyncResult.SuccessCount, SyncResult.FailCount, SyncResult.GetErrorSummary]);

    LogMessage(Format('SyncTables completed: %d processed, %d success, %d failed',
      [SyncResult.TotalProcessed, SyncResult.SuccessCount, SyncResult.FailCount]),
      IfThen(SyncResult.IsSuccessful, logInfo, logWarning));

  except
    on E: Exception do
      HandleError(E, Response, Request);
  finally
    FreeAndNil(RequestBodyJSON);
  end;
end;

class procedure TSyncController.GetTableChanges(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
begin
  GetDataInfo(Request,Response,SQL_GET_TABLE_CHANGES, Params);
end;

class procedure TSyncController.SyncOrders(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
var
  DBConn: IDBConnection;
  RequestBodyJSON: TJSONValue;
  RequestData, OrderObject: TJSONObject;
  OrdersArray: TJSONArray;
  OrderItemValue: TJSONValue; // Corrected variable name for item in array
  EntityID: string;
  OrderIdForDB: Integer; // Assuming Order ID in DB is Integer
  RowsAffected: Integer;
  Exists: Boolean;
  I: Integer;
  SuccessCount, FailCount: Integer;
begin
  DBConn := nil;
  RequestBodyJSON := nil;
  SuccessCount := 0;
  FailCount := 0;

  try
    RequestBodyJSON := GetRequestBody(Request);
    if not (Assigned(RequestBodyJSON) and (RequestBodyJSON is TJSONObject) and ((RequestBodyJSON as TJSONObject).Count > 0)) then
      raise EInvalidRequestException.Create('Request body must be a valid, non-empty JSON object for SyncOrders.');
    RequestData := RequestBodyJSON as TJSONObject;

    if not RequestData.TryGetValue('orders', OrdersArray) or not Assigned(OrdersArray) then
      raise EMissingParameterException.Create('Missing "orders" array in request body for SyncOrders.');

    DBConn := AcquireDBConnection(SYNC_DB_POOL_NAME);
    DBConn.StartTransaction;
    try
      for I := 0 to OrdersArray.Count - 1 do
      begin
        OrderItemValue := OrdersArray.Items[I]; // Use OrderItemValue
        if not (Assigned(OrderItemValue) and (OrderItemValue is TJSONObject)) then
        begin
          LogMessage(Format('SyncOrders: Item at index %d in "orders" array is not a JSON object. Skipping.', [I]), logWarning);
          Inc(FailCount);
          Continue;
        end;
        OrderObject := OrderItemValue as TJSONObject;

        EntityID := GetStr(OrderObject, 'id');
        if EntityID.IsEmpty then
        begin
          LogMessage(Format('SyncOrders: Missing "id" for order item at index %d. Skipping.',[I]), logWarning);
          Inc(FailCount);
          Continue;
        end;
        if not TryStrToInt(EntityID, OrderIdForDB) then
        begin
          LogMessage(Format('SyncOrders: Invalid "id" (not an integer) for order item at index %d: "%s". Skipping.',[I, EntityID]), logWarning);
          Inc(FailCount);
          Continue;
        end;

        Exists := DBConn.ExecuteScalar(SQL_CHECK_ORDER_EXISTS, [OrderIdForDB]) > 0;

        if Exists then
           begin
             var Params := TFDParams.Create;
             try
               Params.Add('id', OrderIdForDB);
               Params.Add('TableId', GetIntegerField(OrderObject, 'TableId', EntityID));
               Params.Add('Status', GetStr(OrderObject, 'Status', ''));
               Params.Add('StartTime', GetDateTimeField(OrderObject, 'StartTime', EntityID));
               Params.Add('EndTime', GetDateTimeField(OrderObject, 'EndTime', EntityID, 0));
               Params.Add('Total', GetFloatField(OrderObject, 'Total', EntityID));
               Params.Add('WaiterId', GetIntegerField(OrderObject, 'WaiterId', EntityID, 0));

               RowsAffected := DBConn.Execute(SQL_UPDATE_ORDER, Params);
             finally
               FreeAndNil(Params);
             end;
           end
        else
           begin
             var Params := TFDParams.Create;
             try
               Params.Add('id', OrderIdForDB);
               Params.Add('TableId', GetIntegerField(OrderObject, 'TableId', EntityID));
               Params.Add('Status', GetStr(OrderObject, 'Status', ''));
               Params.Add('StartTime', GetDateTimeField(OrderObject, 'StartTime', EntityID));
               Params.Add('EndTime', GetDateTimeField(OrderObject, 'EndTime', EntityID, 0));
               Params.Add('Total', GetFloatField(OrderObject, 'Total', EntityID));
               Params.Add('WaiterId', GetIntegerField(OrderObject, 'WaiterId', EntityID, 0));
               RowsAffected := DBConn.Execute(SQL_INSERT_ORDER, Params);
             finally
               FreeAndNil(Params);
             end;
           end;
         LogMessage(Format('SyncOrders: Order ID %s (DB ID: %d) processed. Existed: %s, RowsAffected: %d',
           [EntityID, OrderIdForDB, BoolToStr(Exists,True), RowsAffected]), logDebug);
         Inc(SuccessCount);
      end; // end for

      if FailCount > 0 then
        LogMessage(Format('SyncOrders: Partial success. Succeeded: %d, Failed: %d. Committing successful changes.', [SuccessCount, FailCount]), logWarning);
      DBConn.Commit;
      LogMessage(Format('SyncOrders: Transaction committed. Processed: %d, Succeeded: %d, Failed: %d.', [OrdersArray.Count, SuccessCount, FailCount]), logInfo);
    except
      on E: Exception do
      begin
        if Assigned(DBConn) and DBConn.InTransaction then DBConn.Rollback;
        LogMessage(Format('SyncOrders: Transaction rolled back due to error: %s', [E.Message]), logError);
        raise;
      end;
    end;

    Response.ResponseNo := 200;
    Response.ContentType := 'application/json';
    if FailCount > 0 then
      Response.ContentText := Format('{"success":false, "message":"Orders partially synchronized. Succeeded: %d, Failed: %d"}', [SuccessCount, FailCount])
    else
      Response.ContentText := Format('{"success":true, "message":"%d Orders synchronized successfully"}', [SuccessCount]);
  except
    on E: Exception do
      HandleError(E, Response, Request);
  finally
    FreeAndNil(RequestBodyJSON);
    ReleaseDBConnection(DBConn, SYNC_DB_POOL_NAME);
  end;
end;

class procedure TSyncController.GetOrderChanges(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
begin
  GetDataInfo(Request,Response,SQL_GET_ORDER_CHANGES,Params);
end;

class procedure TSyncController.SyncOrderItems(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
var
  DBConn: IDBConnection;
  RequestBodyJSON: TJSONValue;
  RequestData, ItemObject: TJSONObject;
  ItemsArray: TJSONArray;
  ItemValue: TJSONValue;
  EntityID: string;
  OrderItemIdForDB: Integer; // Assuming OrderItem ID in DB is Integer
  RowsAffected: Integer;
  Exists: Boolean;
  I: Integer;
  SuccessCount, FailCount: Integer;
begin
  DBConn := nil;
  RequestBodyJSON := nil;
  SuccessCount := 0;
  FailCount := 0;

  try
    RequestBodyJSON := GetRequestBody(Request);
    if not (Assigned(RequestBodyJSON) and (RequestBodyJSON is TJSONObject) and ((RequestBodyJSON as TJSONObject).Count > 0)) then
      raise EInvalidRequestException.Create('Request body must be a valid, non-empty JSON object for SyncOrderItems.');
    RequestData := RequestBodyJSON as TJSONObject;

    if not RequestData.TryGetValue('items', ItemsArray) or not Assigned(ItemsArray) then // Assuming "items" for order items
      raise EMissingParameterException.Create('Missing "items" array in request body for SyncOrderItems.');

    DBConn := AcquireDBConnection(SYNC_DB_POOL_NAME);
    DBConn.StartTransaction;
    try
      for I := 0 to ItemsArray.Count - 1 do
      begin
        ItemValue := ItemsArray.Items[I];
        if not (Assigned(ItemValue) and (ItemValue is TJSONObject)) then
        begin
          LogMessage(Format('SyncOrderItems: Item at index %d in "items" array is not a JSON object. Skipping.', [I]), logWarning);
          Inc(FailCount);
          Continue;
        end;
        ItemObject := ItemValue as TJSONObject;

        EntityID := GetStr(ItemObject, 'id');
        if EntityID.IsEmpty then
        begin
          LogMessage(Format('SyncOrderItems: Missing "id" for order item at index %d. Skipping.',[I]), logWarning);
          Inc(FailCount);
          Continue;
        end;
        if not TryStrToInt(EntityID, OrderItemIdForDB) then
        begin
          LogMessage(Format('SyncOrderItems: Invalid "id" (not an integer) for order item at index %d: "%s". Skipping.',[I, EntityID]), logWarning);
          Inc(FailCount);
          Continue;
        end;

        Exists := DBConn.ExecuteScalar(SQL_CHECK_ORDER_ITEM_EXISTS, [OrderItemIdForDB]) > 0;

        if Exists then
          RowsAffected := DBConn.Execute(SQL_UPDATE_ORDER_ITEM, [
            GetIntegerField(ItemObject, 'OrderId', EntityID),
            GetIntegerField(ItemObject, 'ProductId', EntityID),
            GetIntegerField(ItemObject, 'Quantity', EntityID),
            GetFloatField(ItemObject, 'Price', EntityID),
            GetStr(ItemObject, 'Notes', ''),
            GetStr(ItemObject, 'Status', ''),
            OrderItemIdForDB
          ])
        else
          RowsAffected := DBConn.Execute(SQL_INSERT_ORDER_ITEM, [
            OrderItemIdForDB,
            GetIntegerField(ItemObject, 'OrderId', EntityID),
            GetIntegerField(ItemObject, 'ProductId', EntityID),
            GetIntegerField(ItemObject, 'Quantity', EntityID),
            GetFloatField(ItemObject, 'Price', EntityID),
            GetStr(ItemObject, 'Notes', ''),
            GetStr(ItemObject, 'Status', '')
          ]);
        LogMessage(Format('SyncOrderItems: OrderItem ID %s (DB ID: %d) processed. Existed: %s, RowsAffected: %d',
          [EntityID, OrderItemIdForDB, BoolToStr(Exists,True), RowsAffected]), logDebug);
        Inc(SuccessCount);
      end; // end for

      if FailCount > 0 then
        LogMessage(Format('SyncOrderItems: Partial success. Succeeded: %d, Failed: %d. Committing successful changes.', [SuccessCount, FailCount]), logWarning);
      DBConn.Commit;
      LogMessage(Format('SyncOrderItems: Transaction committed. Processed: %d, Succeeded: %d, Failed: %d.', [ItemsArray.Count, SuccessCount, FailCount]), logInfo);
    except
      on E: Exception do
      begin
        if Assigned(DBConn) and DBConn.InTransaction then DBConn.Rollback;
        LogMessage(Format('SyncOrderItems: Transaction rolled back due to error: %s', [E.Message]), logError);
        raise;
      end;
    end;

    Response.ResponseNo := 200;
    Response.ContentType := 'application/json';
    if FailCount > 0 then
      Response.ContentText := Format('{"success":false, "message":"OrderItems partially synchronized. Succeeded: %d, Failed: %d"}', [SuccessCount, FailCount])
    else
      Response.ContentText := Format('{"success":true, "message":"%d OrderItems synchronized successfully"}', [SuccessCount]);
  except
    on E: Exception do
      HandleError(E, Response, Request);
  finally
    FreeAndNil(RequestBodyJSON);
    ReleaseDBConnection(DBConn, SYNC_DB_POOL_NAME);
  end;
end;

class procedure TSyncController.GetOrderItemChanges(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
begin
  GetDataInfo(Request,Response,SQL_GET_ORDER_ITEM_CHANGES, Params);
end;

class procedure TSyncController.SyncProducts(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
var
  DBConn: IDBConnection;
  RequestBodyJSON: TJSONValue;
  RequestData, ProductObject: TJSONObject;
  ProductsArray: TJSONArray;
  ProductItemValue: TJSONValue;
  EntityID: string;
  ProductIdForDB: Integer; // Assuming Product ID in DB is Integer
  RowsAffected: Integer;
  Exists: Boolean;
  I: Integer;
  SuccessCount, FailCount: Integer;
begin
  DBConn := nil;
  RequestBodyJSON := nil;
  SuccessCount := 0;
  FailCount := 0;

  try
    RequestBodyJSON := GetRequestBody(Request);
    if not (Assigned(RequestBodyJSON) and (RequestBodyJSON is TJSONObject) and ((RequestBodyJSON as TJSONObject).Count > 0)) then
      raise EInvalidRequestException.Create('Request body must be a valid, non-empty JSON object for SyncProducts.');
    RequestData := RequestBodyJSON as TJSONObject;

    if not RequestData.TryGetValue('products', ProductsArray) or not Assigned(ProductsArray) then
      raise EMissingParameterException.Create('Missing "products" array in request body for SyncProducts.');

    DBConn := AcquireDBConnection(SYNC_DB_POOL_NAME);
    DBConn.StartTransaction;
    try
      for I := 0 to ProductsArray.Count - 1 do
      begin
        ProductItemValue := ProductsArray.Items[I];
        if not (Assigned(ProductItemValue) and (ProductItemValue is TJSONObject)) then
        begin
          LogMessage(Format('SyncProducts: Item at index %d in "products" array is not a JSON object. Skipping.', [I]), logWarning);
          Inc(FailCount);
          Continue;
        end;
        ProductObject := ProductItemValue as TJSONObject;

        EntityID := GetStr(ProductObject, 'id');
        if EntityID.IsEmpty then
        begin
          LogMessage(Format('SyncProducts: Missing "id" for product item at index %d. Skipping.',[I]), logWarning);
          Inc(FailCount);
          Continue;
        end;
        if not TryStrToInt(EntityID, ProductIdForDB) then
        begin
          LogMessage(Format('SyncProducts: Invalid "id" (not an integer) for product item at index %d: "%s". Skipping.',[I, EntityID]), logWarning);
          Inc(FailCount);
          Continue;
        end;

        Exists := DBConn.ExecuteScalar(SQL_CHECK_PRODUCT_EXISTS, [ProductIdForDB]) > 0;

        if Exists then
          RowsAffected := DBConn.Execute(SQL_UPDATE_PRODUCT, [
            GetStr(ProductObject, 'Name', ''),
            GetStr(ProductObject, 'Description', ''),
            GetFloatField(ProductObject, 'Price', EntityID),
            GetStr(ProductObject, 'Category', ''),
            GetStr(ProductObject, 'Image', ''),
            GetBool(ProductObject, 'Available', True), // Default to True if not specified
            ProductIdForDB
          ])
        else
          RowsAffected := DBConn.Execute(SQL_INSERT_PRODUCT, [
            ProductIdForDB,
            GetStr(ProductObject, 'Name', ''),
            GetStr(ProductObject, 'Description', ''),
            GetFloatField(ProductObject, 'Price', EntityID),
            GetStr(ProductObject, 'Category', ''),
            GetStr(ProductObject, 'Image', ''),
            GetBool(ProductObject, 'Available', True)
          ]);
        LogMessage(Format('SyncProducts: Product ID %s (DB ID: %d) processed. Existed: %s, RowsAffected: %d',
          [EntityID, ProductIdForDB, BoolToStr(Exists,True), RowsAffected]), logDebug);
        Inc(SuccessCount);
      end; // end for

      if FailCount > 0 then
        LogMessage(Format('SyncProducts: Partial success. Succeeded: %d, Failed: %d. Committing successful changes.', [SuccessCount, FailCount]), logWarning);
      DBConn.Commit;
      LogMessage(Format('SyncProducts: Transaction committed. Processed: %d, Succeeded: %d, Failed: %d.', [ProductsArray.Count, SuccessCount, FailCount]), logInfo);
    except
      on E: Exception do
      begin
        if Assigned(DBConn) and DBConn.InTransaction then DBConn.Rollback;
        LogMessage(Format('SyncProducts: Transaction rolled back due to error: %s', [E.Message]), logError);
        raise;
      end;
    end;

    Response.ResponseNo := 200;
    Response.ContentType := 'application/json';
    if FailCount > 0 then
      Response.ContentText := Format('{"success":false, "message":"Products partially synchronized. Succeeded: %d, Failed: %d"}', [SuccessCount, FailCount])
    else
      Response.ContentText := Format('{"success":true, "message":"%d Products synchronized successfully"}', [SuccessCount]);
  except
    on E: Exception do
      HandleError(E, Response, Request);
  finally
    FreeAndNil(RequestBodyJSON);
    ReleaseDBConnection(DBConn, SYNC_DB_POOL_NAME);
  end;
end;

class procedure TSyncController.GetProductChanges(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
begin
  GetDataInfo(Request,Response,SQL_GET_PRODUCT_CHANGES, Params);
end;

initialization
  TControllerRegistry.RegisterController(TSyncController);
end.

