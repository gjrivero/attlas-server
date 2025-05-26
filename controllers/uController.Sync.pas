unit uController.Sync;

interface

uses
  System.SysUtils, System.Classes, System.JSON, System.Generics.Collections,
  IdCustomHTTPServer,
  uController.Base,
  uLib.Database.Types, // Para IDBConnection
  uLib.Routes,         // Para TRouteHandler
  uLib.Sync.Types,     // Para TSyncEntity y nombres de campo de referencia
  uLib.Logger;         // Added uLib.Logger

type
  TSyncController = class(TBaseController)
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
  SYNC_DB_POOL_NAME = 'MainDB_PG'; // EXAMPLE: Adjust to your actual pool name in config.json

  SQL_CHECK_TABLE_EXISTS = 'SELECT COUNT(*) FROM tables WHERE id = :id';
  SQL_UPDATE_TABLE =
    'UPDATE tables SET "Number" = :Number, "Capacity" = :Capacity, "Status" = :Status, "QRCode" = :QRCode, "LastSync" = CURRENT_TIMESTAMP WHERE id = :id';
  SQL_INSERT_TABLE =
    'INSERT INTO tables (id, "Number", "Capacity", "Status", "QRCode", "LastSync") VALUES (:id, :Number, :Capacity, :Status, :QRCode, CURRENT_TIMESTAMP)';
  SQL_GET_TABLE_CHANGES =
    'SELECT id, "Number", "Capacity", "Status", "QRCode", "LastSync" FROM tables WHERE "LastSync" > :LastSync';

  SQL_CHECK_ORDER_EXISTS = 'SELECT COUNT(*) FROM orders WHERE id = :id';
  SQL_UPDATE_ORDER =
    'UPDATE orders SET "TableId" = :TableId, "Status" = :Status, "StartTime" = :StartTime, "EndTime" = :EndTime, "Total" = :Total, "WaiterId" = :WaiterId, "LastSync" = CURRENT_TIMESTAMP WHERE id = :id';
  SQL_INSERT_ORDER =
    'INSERT INTO orders (id, "TableId", "Status", "StartTime", "EndTime", "Total", "WaiterId", "LastSync") VALUES (:id, :TableId, :Status, :StartTime, :EndTime, :Total, :WaiterId, CURRENT_TIMESTAMP)';
  SQL_GET_ORDER_CHANGES =
    'SELECT id, "TableId", "Status", "StartTime", "EndTime", "Total", "WaiterId", "LastSync" FROM orders WHERE "LastSync" > :LastSync';

  SQL_CHECK_ORDER_ITEM_EXISTS = 'SELECT COUNT(*) FROM orderitems WHERE id = :id';
  SQL_UPDATE_ORDER_ITEM =
    'UPDATE orderitems SET "OrderId" = :OrderId, "ProductId" = :ProductId, "Quantity" = :Quantity, "Price" = :Price, "Notes" = :Notes, "Status" = :Status, "LastSync" = CURRENT_TIMESTAMP WHERE id = :id';
  SQL_INSERT_ORDER_ITEM =
    'INSERT INTO orderitems (id, "OrderId", "ProductId", "Quantity", "Price", "Notes", "Status", "LastSync") VALUES (:id, :OrderId, :ProductId, :Quantity, :Price, :Notes, :Status, CURRENT_TIMESTAMP)';
  SQL_GET_ORDER_ITEM_CHANGES =
    'SELECT id, "OrderId", "ProductId", "Quantity", "Price", "Notes", "Status", "LastSync" FROM orderitems WHERE "LastSync" > :LastSync';

  SQL_CHECK_PRODUCT_EXISTS = 'SELECT COUNT(*) FROM products WHERE id = :id';
  SQL_UPDATE_PRODUCT =
    'UPDATE products SET "Name" = :Name, "Description" = :Description, "Price" = :Price, "Category" = :Category, "Image" = :Image, "Available" = :Available, "LastSync" = CURRENT_TIMESTAMP WHERE id = :id';
  SQL_INSERT_PRODUCT =
    'INSERT INTO products (id, "Name", "Description", "Price", "Category", "Image", "Available", "LastSync") VALUES (:id, :Name, :Description, :Price, :Category, :Image, :Available, CURRENT_TIMESTAMP)';
  SQL_GET_PRODUCT_CHANGES =
    'SELECT id, "Name", "Description", "Price", "Category", "Image", "Available", "LastSync" FROM products WHERE "LastSync" > :LastSync';

implementation

uses
  System.DateUtils,
  System.Variants,
  System.StrUtils, // For IfThen, SameText
  uLib.Base;
  // uLib.Logger is in interface uses

// Helper to safely get an integer from JSON, logging a warning if not an integer
function GetIntegerField(const AJSONObject: TJSONObject; const AFieldName: string; const AEntityIDForLog: string; ADdefaultValue: Integer = 0): Integer;
var
  LJsonValue: TJSONValue;
  LStrValue: string;
begin
  Result := ADdefaultValue;
  if Assigned(AJSONObject) and AJSONObject.TryGetValue(AFieldName, LJsonValue) and Assigned(LJsonValue) then
  begin
    if LJsonValue is TJSONNumber then
      Result := Round(TJSONNumber(LJsonValue).AsExtended) // Or Trunc, depending on desired behavior
    else if LJsonValue is TJSONString then
    begin
      LStrValue := (LJsonValue as TJSONString).Value;
      if not TryStrToInt(LStrValue, Result) then
      begin
        Result := ADdefaultValue;
        LogMessage(Format('Sync field conversion warning for entity ID "%s": Field "%s" ("%s") is not a valid integer. Using default %d.',
          [AEntityIDForLog, AFieldName, LStrValue, ADdefaultValue]), logWarning);
      end;
    end
    else if not (LJsonValue is TJSONNull) then // Not a number, not a string, not null
    begin
        LogMessage(Format('Sync field type warning for entity ID "%s": Field "%s" is type %s, expected Number or String convertible to Integer. Using default %d.',
          [AEntityIDForLog, AFieldName, LJsonValue.ClassName, ADdefaultValue]), logWarning);
    end;
  end;
end;

// Helper to safely get a float from JSON
function GetFloatField(const AJSONObject: TJSONObject; const AFieldName: string; const AEntityIDForLog: string; ADdefaultValue: Double = 0.0): Double;
var
  LJsonValue: TJSONValue;
  LStrValue: string;
begin
  Result := ADdefaultValue;
  if Assigned(AJSONObject) and AJSONObject.TryGetValue(AFieldName, LJsonValue) and Assigned(LJsonValue) then
  begin
    if LJsonValue is TJSONNumber then
      Result := TJSONNumber(LJsonValue).AsExtended
    else if LJsonValue is TJSONString then
    begin
      LStrValue := (LJsonValue as TJSONString).Value;
      if not TryStrToFloat(LStrValue, Result) then // Uses system default decimal separator
      begin
        Result := ADdefaultValue;
        LogMessage(Format('Sync field conversion warning for entity ID "%s": Field "%s" ("%s") is not a valid float. Using default %f.',
          [AEntityIDForLog, AFieldName, LStrValue, ADdefaultValue]), logWarning);
      end;
    end
    else if not (LJsonValue is TJSONNull) then
    begin
        LogMessage(Format('Sync field type warning for entity ID "%s": Field "%s" is type %s, expected Number or String convertible to Float. Using default %f.',
          [AEntityIDForLog, AFieldName, LJsonValue.ClassName, ADdefaultValue]), logWarning);
    end;
  end;
end;

// Helper to safely get a DateTime from ISO8601 string in JSON
function GetDateTimeField(const AJSONObject: TJSONObject; const AFieldName: string; const AEntityIDForLog: string; ADefaultValue: TDateTime = 0): TDateTime;
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
        LogMessage(Format('Sync field conversion warning for entity ID "%s": Field "%s" ("%s") is not a valid ISO8601 DateTime: %s. Using default.',
          [AEntityIDForLog, AFieldName, LStrValue, E.Message]), logWarning);
      end;
    end;
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
  LogMessage('TSyncController routes registered.', logInfo);
end;

class procedure TSyncController.SyncTables(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
var
  DBConn: IDBConnection;
  RequestBodyJSON: TJSONValue;
  RequestData: TJSONObject;
  TablesArray: TJSONArray;
  TableItemValue: TJSONValue;
  TableObject: TJSONObject;
  EntityID: string;
  TableIdForDB: Integer;
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
    RequestBodyJSON := GetRequestBody(Request); // This now returns an empty TJSONObject if body is empty/null
    if not (Assigned(RequestBodyJSON) and (RequestBodyJSON is TJSONObject) and ((RequestBodyJSON as TJSONObject).Count > 0)) then
      raise EInvalidRequestException.Create('Request body must be a valid, non-empty JSON object for SyncTables.');
    RequestData := RequestBodyJSON as TJSONObject;

    if not RequestData.TryGetValue('tables', TablesArray) or not Assigned(TablesArray) then
      raise EMissingParameterException.Create('Missing "tables" array in request body for SyncTables.');

    DBConn := AcquireDBConnection(SYNC_DB_POOL_NAME);
    DBConn.StartTransaction;
    try
      for I := 0 to TablesArray.Count - 1 do
      begin
        TableItemValue := TablesArray.Items[I];
        if not (Assigned(TableItemValue) and (TableItemValue is TJSONObject)) then
        begin
          LogMessage(Format('SyncTables: Item at index %d in "tables" array is not a JSON object. Skipping.', [I]), logWarning);
          Inc(FailCount);
          Continue;
        end;
        TableObject := TableItemValue as TJSONObject;

        EntityID := GetStr(TableObject, 'id');
        if EntityID.IsEmpty then
        begin
          LogMessage(Format('SyncTables: Missing "id" for table item at index %d. Skipping.',[I]), logWarning);
          Inc(FailCount);
          Continue;
        end;

        if not TryStrToInt(EntityID, TableIdForDB) then
        begin
            LogMessage(Format('SyncTables: Invalid "id" (not an integer) for table item at index %d: "%s". Skipping.',[I, EntityID]), logWarning);
            Inc(FailCount);
            Continue;
        end;

        Exists := DBConn.ExecuteScalar(SQL_CHECK_TABLE_EXISTS, [TableIdForDB]) > 0;

        if Exists then
          RowsAffected := DBConn.Execute(SQL_UPDATE_TABLE, [
            GetIntegerField(TableObject, 'Number', EntityID),
            GetIntegerField(TableObject, 'Capacity', EntityID),
            GetStr(TableObject, 'Status', ''),
            GetStr(TableObject, 'QRCode', ''),
            TableIdForDB
          ])
        else
          RowsAffected := DBConn.Execute(SQL_INSERT_TABLE, [
            TableIdForDB,
            GetIntegerField(TableObject, 'Number', EntityID),
            GetIntegerField(TableObject, 'Capacity', EntityID),
            GetStr(TableObject, 'Status', ''),
            GetStr(TableObject, 'QRCode', '')
          ]);
        LogMessage(Format('SyncTables: Table ID %s (DB ID: %d) processed. Existed: %s, RowsAffected: %d',
          [EntityID, TableIdForDB, BoolToStr(Exists,True), RowsAffected]), logDebug);
        Inc(SuccessCount);
      end; // end for

      if FailCount > 0 then // If any items failed, consider rolling back or partial success
      begin
        // For simplicity, we commit successful ones and report failures.
        // A more robust approach might roll back the entire transaction if any item fails.
        LogMessage(Format('SyncTables: Partial success. Succeeded: %d, Failed: %d. Committing successful changes.', [SuccessCount, FailCount]), logWarning);
      end;
      DBConn.Commit;
      LogMessage(Format('SyncTables: Transaction committed. Processed: %d, Succeeded: %d, Failed: %d.', [TablesArray.Count, SuccessCount, FailCount]), logInfo);
    except
      on E: Exception do
      begin
        if Assigned(DBConn) and DBConn.InTransaction then DBConn.Rollback;
        LogMessage(Format('SyncTables: Transaction rolled back due to error: %s', [E.Message]), logError);
        raise; // Re-raise to be handled by HandleError
      end;
    end;

    Response.ResponseNo := 200;
    Response.ContentType := 'application/json';
    if FailCount > 0 then
      Response.ContentText := Format('{"success":false, "message":"Tables partially synchronized. Succeeded: %d, Failed: %d"}', [SuccessCount, FailCount])
    else
      Response.ContentText := Format('{"success":true, "message":"%d Tables synchronized successfully"}', [SuccessCount]);
  except
    on E: Exception do
      HandleError(E, Response, Request);
  finally
    FreeAndNil(RequestBodyJSON);
    ReleaseDBConnection(DBConn, SYNC_DB_POOL_NAME);
  end;
end;

class procedure TSyncController.GetTableChanges(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
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
    JsonResultString := DBConn.ExecuteJSON(SQL_GET_TABLE_CHANGES, [LastSync]);
    Response.ResponseNo := 200;
    Response.ContentType := 'application/json';
    Response.ContentText := JsonResultString;
  except
    on E: Exception do
      HandleError(E, Response, Request);
  finally
    ReleaseDBConnection(DBConn, SYNC_DB_POOL_NAME);
  end;
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
          RowsAffected := DBConn.Execute(SQL_UPDATE_ORDER, [
            GetIntegerField(OrderObject, 'TableId', EntityID),
            GetStr(OrderObject, 'Status', ''),
            GetDateTimeField(OrderObject, 'StartTime', EntityID),
            GetDateTimeField(OrderObject, 'EndTime', EntityID, 0), // Default to 0 if EndTime can be null
            GetFloatField(OrderObject, 'Total', EntityID),
            GetIntegerField(OrderObject, 'WaiterId', EntityID, 0), // Default WaiterId if can be null/optional
            OrderIdForDB
          ])
        else
          RowsAffected := DBConn.Execute(SQL_INSERT_ORDER, [
            OrderIdForDB,
            GetIntegerField(OrderObject, 'TableId', EntityID),
            GetStr(OrderObject, 'Status', ''),
            GetDateTimeField(OrderObject, 'StartTime', EntityID),
            GetDateTimeField(OrderObject, 'EndTime', EntityID, 0),
            GetFloatField(OrderObject, 'Total', EntityID),
            GetIntegerField(OrderObject, 'WaiterId', EntityID, 0)
          ]);
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
      raise EMissingParameterException.Create('Missing "lastSync" query parameter for GetOrderChanges.');
    try
      LastSync := ISO8601ToDate(LastSyncStr);
    except
      on E: Exception do
        raise EInvalidParameterException.CreateFmt('Invalid "lastSync" date format: "%s". Error: %s', [LastSyncStr, E.Message]);
    end;

    DBConn := AcquireDBConnection(SYNC_DB_POOL_NAME);
    JsonResultString := DBConn.ExecuteJSON(SQL_GET_ORDER_CHANGES, [LastSync]);
    Response.ResponseNo := 200;
    Response.ContentType := 'application/json';
    Response.ContentText := JsonResultString;
  except
    on E: Exception do
      HandleError(E, Response, Request);
  finally
    ReleaseDBConnection(DBConn, SYNC_DB_POOL_NAME);
  end;
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
      raise EMissingParameterException.Create('Missing "lastSync" query parameter for GetOrderItemChanges.');
    try
      LastSync := ISO8601ToDate(LastSyncStr);
    except
      on E: Exception do
        raise EInvalidParameterException.CreateFmt('Invalid "lastSync" date format: "%s". Error: %s', [LastSyncStr, E.Message]);
    end;

    DBConn := AcquireDBConnection(SYNC_DB_POOL_NAME);
    JsonResultString := DBConn.ExecuteJSON(SQL_GET_ORDER_ITEM_CHANGES, [LastSync]);
    Response.ResponseNo := 200;
    Response.ContentType := 'application/json';
    Response.ContentText := JsonResultString;
  except
    on E: Exception do
      HandleError(E, Response, Request);
  finally
    ReleaseDBConnection(DBConn, SYNC_DB_POOL_NAME);
  end;
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
      raise EMissingParameterException.Create('Missing "lastSync" query parameter for GetProductChanges.');
    try
      LastSync := ISO8601ToDate(LastSyncStr);
    except
      on E: Exception do
        raise EInvalidParameterException.CreateFmt('Invalid "lastSync" date format: "%s". Error: %s', [LastSyncStr, E.Message]);
    end;

    DBConn := AcquireDBConnection(SYNC_DB_POOL_NAME);
    JsonResultString := DBConn.ExecuteJSON(SQL_GET_PRODUCT_CHANGES, [LastSync]);
    Response.ResponseNo := 200;
    Response.ContentType := 'application/json';
    Response.ContentText := JsonResultString;
  except
    on E: Exception do
      HandleError(E, Response, Request);
  finally
    ReleaseDBConnection(DBConn, SYNC_DB_POOL_NAME);
  end;
end;

initialization
  TControllerRegistry.RegisterController(TSyncController);
end.

