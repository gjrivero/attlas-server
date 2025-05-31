unit uController.Customers;

interface

uses
  System.SysUtils, System.Classes, System.JSON, System.Generics.Collections,
  IdCustomHTTPServer,
  uLib.Controller.Base,    // Contiene AcquireDBConnection y ReleaseDBConnection
  uLib.Database.Types, // Para IDBConnection, TDBType y excepciones de BD
  uLib.Database.Connection, // Para TBaseConnection (para obtener DBType de la config)
  uLib.Routes,         // Para TRouteManager y TRouteHandler
  uLib.SQLQuery.Builder, // Para construir SQL dinámicamente
  uLib.Logger;          // Añadido para LogMessage

type
  TCustomerController = class(TBaseController)
  private
    class function ValidateCustomerData(const ACustomerData: TJSONObject; IsNew: Boolean): Boolean;
  public
    class procedure RegisterRoutes; override;

    class procedure GetCustomers(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
    class procedure GetCustomerById(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
    class procedure CreateCustomer(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
    class procedure UpdateCustomer(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
    class procedure DeleteCustomer(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
  end;

const
  CUSTOMER_DB_POOL_NAME = 'MainDB_PG'; // EJEMPLO, ¡AJUSTA ESTE VALOR según tu config.json!

  SQL_BASE_SELECT_CUSTOMERS =
    'SELECT id, name, email, phone, address, created_at, updated_at FROM customers';

  SQL_SELECT_CUSTOMER_BY_ID =
    'SELECT id, name, email, phone, address, created_at, updated_at ' +
    'FROM customers WHERE id = :id AND active = true'; // Asegurar que solo se obtengan activos

  SQL_INSERT_CUSTOMER =
    'INSERT INTO customers (name, email, phone, address, created_at, updated_at, active) ' +
    'VALUES (:name, :email, :phone, :address, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, true) ' + // Añadido active = true
    'RETURNING id, name, email, phone, address, created_at, updated_at'; // Asumiendo que la BD soporta RETURNING

  SQL_UPDATE_CUSTOMER =
    'UPDATE customers SET ' +
    'name = :name, email = :email, phone = :phone, address = :address, ' +
    'updated_at = CURRENT_TIMESTAMP ' +
    'WHERE id = :id AND active = true';

  SQL_DELETE_CUSTOMER = // Borrado lógico
    'UPDATE customers SET active = false, updated_at = CURRENT_TIMESTAMP ' +
    'WHERE id = :id AND active = true';

  ALLOWED_CUSTOMER_FILTER_FIELDS: array[0..4] of string = ('id', 'name', 'email', 'phone', 'address');
  ALLOWED_CUSTOMER_SORT_FIELDS: array[0..3] of string = ('id', 'name', 'email', 'created_at');

implementation

uses
  FireDAC.Stan.Param,
  System.RegularExpressions,
  System.StrUtils, // Para StrToIntDef, IfThen, etc.
  System.Variants, // Para TArray<Variant>
  uLib.Utils;


class procedure TCustomerController.RegisterRoutes;
begin
  if not Assigned(FRouteManager) then
  begin
    LogMessage('CRITICAL: FRouteManager not assigned in TCustomerController. Routes will not be registered.', logError);
    Exit;
  end;

  FRouteManager.AddRoute('GET',    'customers', GetCustomers, True);
  FRouteManager.AddRoute('GET',    'customers/:id(int)', GetCustomerById, true);
  FRouteManager.AddRoute('POST',   'customers', CreateCustomer, True);
  FRouteManager.AddRoute('PUT',    'customers/:id(int)', UpdateCustomer, True);
  FRouteManager.AddRoute('DELETE', 'customers/:id(int)', DeleteCustomer, True);
  LogMessage('TCustomerController routes registered.', logInfo);
end;

class function TCustomerController.ValidateCustomerData(const ACustomerData: TJSONObject; IsNew: Boolean): Boolean;
var
  Name, Email: string;
begin
  Result := False;
  if not Assigned(ACustomerData) then
    raise EInvalidRequestException.Create('Customer data (JSON body) cannot be nil.');

  Name := TJSONHelper.GetString(ACustomerData, 'name', '').Trim;
  Email := TJSONHelper.GetString(ACustomerData, 'email', '').Trim;

  if Name.IsEmpty then
    raise EMissingParameterException.Create('Customer name is required.');
  if Email.IsEmpty then
    raise EMissingParameterException.Create('Customer email is required.');

  // Validación básica de email.
  if (Pos('@', Email) <= 0) or (Pos('.', Email, Pos('@', Email)) <= Pos('@', Email)) then
    raise EInvalidParameterException.Create('Invalid email format provided.');

  Result := True;
  LogMessage('Customer data validated successfully.', logDebug);
end;

class procedure TCustomerController.GetCustomers(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
var
  DBConn: IDBConnection;
  SQLBuilder: TSQLQueryBuilder;
  DBType: TDBType;
  WhereClause,
  OrderByClause,
  PaginationClause,
  FinalSQL: string;
  FilteredRequestParams: TStringList;
  WhereFDParams: TFDParams;
  JsonResultString: string;
  I: integer;
  ParamName,
  ParamValue,
  FieldNameFromParam: string;
  SortFieldsArray: TArray<string>;
  ValidSortFieldsList: TStringList;
begin
  DBConn := nil;
  SQLBuilder := nil;
  DBType := dbtUnknown;
  JsonResultString := '[]';

  DBConn := nil;
  SQLBuilder := nil;
  FilteredRequestParams := nil; // Inicializar a nil
  ValidSortFieldsList := nil;   // Inicializar a nil
  WhereFDParams := nil;         // Inicializar a nil
  try
    FilteredRequestParams := TStringList.Create; // Crear
    ValidSortFieldsList := TStringList.Create;   // Crear
    WhereFDParams := TFDParams.Create;           // Crear

    DBConn := AcquireDBConnection(CUSTOMER_DB_POOL_NAME);
    // Determinar DBType para TSQLQueryBuilder
    // Idealmente, IDBConnection expondría DBType. Por ahora, usamos el cast.
    if (DBConn is TBaseConnection) then
      DBType := (DBConn as TBaseConnection).Config.DBType
    else
      LogMessage(Format('GetCustomers: Could not determine DBType from connection of pool "%s".'+
           ' SQLBuilder defaulting to dbtUnknown.', [CUSTOMER_DB_POOL_NAME]), logWarning);

    if DBType = dbtUnknown then
       LogMessage('GetCustomers: DBType is Unknown. SQL features like pagination might not work optimally.', logWarning);
    try
      if Assigned(Request.Params) then
      begin
        for I := 0 to Request.Params.Count - 1 do
        begin
          ParamName := Request.Params.Names[I];
          ParamValue := Request.Params.ValueFromIndex[I];

          if ParamName.StartsWith('_') then // Parámetros especiales (_sort, _limit, _offset)
          begin
            if SameText(ParamName, '_sort') then
            begin
              SortFieldsArray := ParamValue.Split([',']);
              for var SortFieldStr in SortFieldsArray do
              begin
                var ActualSortFieldRaw := Trim(SortFieldStr);
                var ActualSortFieldClean := ActualSortFieldRaw;
                var IsDesc := False;

                if ActualSortFieldClean.StartsWith('-') then
                begin
                  ActualSortFieldClean := Copy(ActualSortFieldClean, 2, Length(ActualSortFieldClean) - 1);
                  IsDesc := True;
                end
                else if ActualSortFieldClean.StartsWith('+') then
                begin
                  ActualSortFieldClean := Copy(ActualSortFieldClean, 2, Length(ActualSortFieldClean) - 1);
                end
                else if LowerCase(ActualSortFieldClean).EndsWith('_desc') then // Suffix
                begin
                  ActualSortFieldClean := Copy(ActualSortFieldClean, 1, Length(ActualSortFieldClean) - Length('_desc'));
                  IsDesc := True;
                end
                else if LowerCase(ActualSortFieldClean).EndsWith('_asc') then // Suffix
                begin
                   ActualSortFieldClean := Copy(ActualSortFieldClean, 1, Length(ActualSortFieldClean) - Length('_asc'));
                end;

                ActualSortFieldClean := Trim(ActualSortFieldClean);

                if IsStringInArray(ActualSortFieldClean, ALLOWED_CUSTOMER_SORT_FIELDS, True) then
                  ValidSortFieldsList.Add(ActualSortFieldRaw) // Añadir el string original (ej. 'name_desc' o '-email')
                else
                  LogMessage(Format('GetCustomers: Invalid or disallowed sort field "%s" (cleaned: "%s") requested. Ignoring.', [SortFieldStr, ActualSortFieldClean]), logWarning);
              end;
              if ValidSortFieldsList.Count > 0 then
                FilteredRequestParams.AddPair(ParamName, ValidSortFieldsList.CommaText);
            end
            else if SameText(ParamName, '_limit') or SameText(ParamName, '_offset') then
            begin
              // Estos serán validados numéricamente por TSQLQueryBuilder o por su uso.
              FilteredRequestParams.AddPair(ParamName, ParamValue);
            end
            // else: ignorar otros parámetros especiales desconocidos
          end
          else // Parámetros de filtro (field[op]=value o field=value)
          begin
            var Match := TRegEx.Match(ParamName, '^([\w\.]+)(?:\[(\w+)\])?$'); // Permite field.subfield
            if Match.Success then
            begin
              FieldNameFromParam := Match.Groups[1].Value;
              // Importante: Validar contra lista blanca
              if IsStringInArray(FieldNameFromParam, ALLOWED_CUSTOMER_FILTER_FIELDS, True) then
                FilteredRequestParams.AddPair(ParamName, ParamValue)
              else
                LogMessage(Format('GetCustomers: Invalid or disallowed filter field "%s" requested. Ignoring.', [FieldNameFromParam]), logWarning);
            end
            else // Nombre de parámetro malformado
                 LogMessage(Format('GetCustomers: Malformed filter parameter name "%s" requested. Ignoring.', [ParamName]), logWarning);
          end;
        end;
      end;

      SQLBuilder := TSQLQueryBuilder.Create(DBType, FilteredRequestParams); // Usar parámetros filtrados
      try
        WhereClause := SQLBuilder.GetWhereClause(WhereFDParams); // Popula WhereFDParams
        OrderByClause := SQLBuilder.GetOrderByClause(); // OrderByFDParams no es realmente poblado por SQLBuilder con la corrección anterior.
                                                                    // Se mantiene por si la firma de GetOrderByClause se extiende en el futuro.
        FinalSQL := SQL_BASE_SELECT_CUSTOMERS;
        var CombinedWhere: string := 'active = true'; // Siempre filtrar por activos
        if WhereClause <> '' then
          CombinedWhere := CombinedWhere + ' AND (' + WhereClause + ')';

        FinalSQL := FinalSQL + ' WHERE ' + CombinedWhere;

        if OrderByClause <> '' then
          FinalSQL := FinalSQL + ' ' + OrderByClause;

        PaginationClause := SQLBuilder.GetPaginationClause;
        if PaginationClause <> '' then
          FinalSQL := FinalSQL + ' ' + PaginationClause;

        LogMessage(Format('GetCustomers: Executing SQL: %s', [FinalSQL]), logDebug);
        if Assigned(WhereFDParams) and (WhereFDParams.Count > 0) then // Solo WhereFDParams tendrá parámetros
          LogMessage(Format('GetCustomers: With %d SQL parameters.', [WhereFDParams.Count]), logDebug);

        JsonResultString := DBConn.ExecuteJSON(FinalSQL, WhereFDParams); // Usar WhereFDParams
      finally
        FreeAndNil(SQLBuilder);
      end;

      Response.ContentType := 'application/json';
      Response.ResponseNo := 200;
      Response.ContentText := JsonResultString;

    except
      on E: Exception do
        HandleError(E, Response, Request);
    end;
  finally
    // CORRECCIÓN CRÍTICA: Liberar TODAS las variables en el orden correcto
    FreeAndNil(FilteredRequestParams);
    FreeAndNil(ValidSortFieldsList);
    FreeAndNil(WhereFDParams);

    FreeAndNil(SQLBuilder);
    ReleaseDBConnection(DBConn, CUSTOMER_DB_POOL_NAME);
  end;
end;

class procedure TCustomerController.GetCustomerById(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
var
  DBConn: IDBConnection;
  CustomerIdStr: string;
  CustomerId: Integer;
  JsonResultString: string;
  JsonValue: TJSONValue;
  JsonArray: TJSONArray;
  Params: TFDParams;
begin
  DBConn := nil;
  JsonValue := nil;
  Params := nil;
  JsonResultString := '[]';

  try
    if not RouteParams.TryGetValue('id', CustomerIdStr) then
      raise EMissingParameterException.Create('Customer ID is missing in path parameter.');

    CustomerId := StrToIntDef(CustomerIdStr, -1);
    if CustomerId <= 0 then
      raise EInvalidParameterException.Create('Invalid Customer ID format in path: ' + CustomerIdStr);

    Params := TFDParams.Create;
    Params.Add('id', CustomerId);

    DBConn := AcquireDBConnection(CUSTOMER_DB_POOL_NAME);
    JsonResultString := DBConn.ExecuteJSON(SQL_SELECT_CUSTOMER_BY_ID, Params);

    Response.ContentType := 'application/json';
    JsonValue := TJSONObject.ParseJSONValue(JsonResultString);
    try
      if Assigned(JsonValue) and (JsonValue is TJSONArray) then
      begin
        JsonArray := JsonValue as TJSONArray;
        if JsonArray.Count = 0 then
        begin
          Response.ResponseNo := 404;
          Response.ContentText := '{"success":false, "message":"Customer not found"}';
        end
        else
        begin
          Response.ResponseNo := 200;
          Response.ContentText := JsonArray.Items[0].ToJSON; // Devuelve el primer objeto
        end;
      end
      else
      begin
        LogMessage(Format('Error parsing JSON result from ExecuteJSON in GetCustomerById or result is not an array. Result: %s', [JsonResultString]), logError);
        Response.ResponseNo := 404; // O 500 si el formato es inesperado
        Response.ContentText := '{"success":false, "message":"Customer not found or data error."}';
      end;
    except
      on E: Exception do
        HandleError(E, Response, Request);
    end;
  finally
    FreeAndNil(Params);
    FreeAndNil(JsonValue);
    ReleaseDBConnection(DBConn, CUSTOMER_DB_POOL_NAME); // ← MOVER AQUÍ
  end;
end;

class procedure TCustomerController.CreateCustomer(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
var
  DBConn: IDBConnection;
  RequestBodyJSON: TJSONValue;
  CustomerDataJSON: TJSONObject;
  NewCustomerResultString: string;
  NewCustomerJSONValue: TJSONValue;
  NewCustomerJSONArray: TJSONArray;
  Params: TFDParams;
begin
  DBConn := nil;
  RequestBodyJSON := nil;
  NewCustomerJSONValue := nil;

  try
    RequestBodyJSON := GetRequestBody(Request);
    if not (Assigned(RequestBodyJSON) and (RequestBodyJSON is TJSONObject)) then
      raise EInvalidRequestException.Create('Request body must be a valid JSON object for creating a customer.');

    CustomerDataJSON := RequestBodyJSON as TJSONObject;
    ValidateCustomerData(CustomerDataJSON, True);
    try
      DBConn := AcquireDBConnection(CUSTOMER_DB_POOL_NAME);
      Params:=TFDParams.Create;
      Params.Add('name',TJSONHelper.GetString(CustomerDataJSON, 'name'));
      Params.Add('email',TJSONHelper.GetString(CustomerDataJSON, 'email'));
      Params.Add('phone',TJSONHelper.GetString(CustomerDataJSON, 'phone'));
      Params.Add('address',TJSONHelper.GetString(CustomerDataJSON, 'address'));
      // Asumiendo que ExecuteJSON con RETURNING devuelve un array con el objeto insertado/actualizado
      NewCustomerResultString := DBConn.ExecuteJSON(SQL_INSERT_CUSTOMER, Params);

      NewCustomerJSONValue := TJSONObject.ParseJSONValue(NewCustomerResultString);
      if Assigned(NewCustomerJSONValue) and (NewCustomerJSONValue is TJSONArray) then
      begin
        NewCustomerJSONArray := NewCustomerJSONValue as TJSONArray;
        if NewCustomerJSONArray.Count > 0 then
        begin
          Response.ContentType := 'application/json';
          Response.ResponseNo := 201; // Created
          Response.ContentText := NewCustomerJSONArray.Items[0].ToJSON;
        end
        else
          raise EDBCommandError.Create('Failed to retrieve created customer data after insert (empty array returned).');
      end
      else
        raise EDBCommandError.Create('Invalid data format received from database after insert (not a JSON array or nil).');

    except
      on E: Exception do
        HandleError(E, Response, Request);
    end;
  finally
    FreeAndNil(Params);
    FreeAndNil(RequestBodyJSON);
    FreeAndNil(NewCustomerJSONValue);
    ReleaseDBConnection(DBConn, CUSTOMER_DB_POOL_NAME);
  end;
end;

class procedure TCustomerController.UpdateCustomer(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
var
  DBConn: IDBConnection;
  RequestBodyJSON: TJSONValue;
  CustomerDataJSON: TJSONObject;
  Params: TFDParams;
  CustomerIdStr: string;
  CustomerId,
  RowsAffected: Integer;
begin
  DBConn := nil;
  RequestBodyJSON := nil;
  try
    if not RouteParams.TryGetValue('id', CustomerIdStr) then
      raise EMissingParameterException.Create('Customer ID is missing in path parameter.');

    CustomerId := StrToIntDef(CustomerIdStr, -1);
    if CustomerId <= 0 then
      raise EInvalidParameterException.Create('Invalid Customer ID format in path: ' + CustomerIdStr);

    RequestBodyJSON := GetRequestBody(Request);
    if not (Assigned(RequestBodyJSON) and (RequestBodyJSON is TJSONObject)) then
      raise EInvalidRequestException.Create('Request body must be a valid JSON object for updating a customer.');

    CustomerDataJSON := RequestBodyJSON as TJSONObject;
    ValidateCustomerData(CustomerDataJSON, False);
    try
      DBConn := AcquireDBConnection(CUSTOMER_DB_POOL_NAME);
      Params:=TFDParams.Create;
      Params.Add('name',TJSONHelper.GetString(CustomerDataJSON, 'name'));
      Params.Add('email',TJSONHelper.GetString(CustomerDataJSON, 'email'));
      Params.Add('phone',TJSONHelper.GetString(CustomerDataJSON, 'phone'));
      Params.Add('address',TJSONHelper.GetString(CustomerDataJSON, 'address'));
      Params.Add('id',CustomerId);

      RowsAffected := DBConn.Execute(SQL_UPDATE_CUSTOMER, Params);

      Response.ContentType := 'application/json';
      if RowsAffected > 0 then
      begin
        Response.ResponseNo := 200;
        Response.ContentText := '{"success":true, "message":"Customer updated successfully"}';
        // Opcional: Devolver el cliente actualizado. Para ello, se haría un SELECT después del UPDATE.
        // GetCustomerById(Request, Response, RouteParams); // Podría reutilizar, pero cuidado con la respuesta.
      end
      else
      begin
        Response.ResponseNo := 404;
        Response.ContentText := '{"success":false, "message":"Customer not found or no changes made"}';
      end;
    except
      on E: Exception do
         HandleError(E, Response, Request);
    end;
  finally
    FreeAndNil(Params);
    FreeAndNil(RequestBodyJSON);
    ReleaseDBConnection(DBConn, CUSTOMER_DB_POOL_NAME);
  end;
end;

class procedure TCustomerController.DeleteCustomer(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo; RouteParams: TDictionary<string, string>);
var
  DBConn: IDBConnection;
  CustomerIdStr: string;
  CustomerId,
  RowsAffected: Integer;
  Params: TFDParams;
begin
  DBConn := nil;
  try
    if not RouteParams.TryGetValue('id', CustomerIdStr) then
      raise EMissingParameterException.Create('Customer ID is missing in path parameter.');

    CustomerId := StrToIntDef(CustomerIdStr, -1);
    if CustomerId <= 0 then
      raise EInvalidParameterException.Create('Invalid Customer ID format in path: ' + CustomerIdStr);
    try
      DBConn := AcquireDBConnection(CUSTOMER_DB_POOL_NAME);
      Params:=TFDParams.Create;
      Params.Add('id',CustomerId);
      RowsAffected := DBConn.Execute(SQL_DELETE_CUSTOMER, Params);

      Response.ContentType := 'application/json';
      if RowsAffected > 0 then
      begin
        Response.ResponseNo := 200; // O 204 No Content si no se devuelve cuerpo
        Response.ContentText := '{"success":true, "message":"Customer logically deleted successfully"}';
      end
      else
      begin
        Response.ResponseNo := 404;
        Response.ContentText := '{"success":false, "message":"Customer not found or already inactive"}';
      end;
    except
      on E: Exception do
        HandleError(E, Response, Request);
    end;
  finally
    FreeAndNil(Params);
    ReleaseDBConnection(DBConn, CUSTOMER_DB_POOL_NAME);
  end;
end;

initialization
  TControllerRegistry.RegisterController(TCustomerController);
end.
