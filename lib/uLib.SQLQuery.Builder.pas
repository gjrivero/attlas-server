unit uLib.SQLQuery.Builder;

interface

uses
  System.SysUtils, System.Classes, System.Generics.Collections, System.Variants,
  System.RegularExpressions, System.StrUtils, System.Rtti,
  FireDAC.Stan.Param,
  uLib.Database.Types,
  uLib.Logger;

type
  TSQLOperator = (
    sqoEquals, sqoNotEquals,
    sqoGreaterThan, sqoGreaterThanOrEquals,
    sqoLessThan, sqoLessThanOrEquals,
    sqoLike, sqoNotLike,
    sqoIn, sqoNotIn,
    sqoIsNull, sqoIsNotNull
  );

  TSQLSortDirection = (sdAsc, sdDesc);

  TSQLSortField = record
    FieldName: string;
    Direction: TSQLSortDirection;
  end;

  TSQLQueryCondition = record // Estructura interna para una condición parseada
    FieldName: string;
    Operator: TSQLOperator;
    Value: Variant; // Podría ser un TArray<Variant> para el operador IN
    IsRawValue: Boolean; // Para IS NULL / IS NOT NULL donde Value no es un parámetro
  end;

  TSQLQueryBuilder = class
  private
    FDBType: TDBType;
    FURLQueryParams: TStrings;
    FConditions: TList<TSQLQueryCondition>;
    FSortFields: TList<TSQLSortField>;
    FLimit: Integer;
    FOffset: Integer;

    procedure ParseAllQueryParams;
    procedure ParseFilterParam(const AParamName, AParamValue: string);
    procedure ParseSortParam(const ASortString: string);
    procedure ParsePaginationParams;

    function GetOperatorFromString(const OpStr: string): TSQLOperator;
    function QuoteIdentifier(const Identifier: string): string;
    function FormatValueForSQL(const AValue: Variant; out IsParameter: Boolean): string;
    function GetOperatorSQL(AOperator: TSQLOperator): string;

  public
    constructor Create(ADBType: TDBType; AURLQueryParams: TStrings);
    destructor Destroy; override;

    function GetWhereClause(out AParams: TFDParams): string;
    function GetOrderByClause(out AParams: TFDParams): string;
    function GetPaginationClause: string; // Para PostgreSQL, MySQL y MSSQL 2012+ (LIMIT/OFFSET o OFFSET/FETCH)

    property Limit: Integer read FLimit;
    property Offset: Integer read FOffset;
  end;

implementation

Uses
    System.Math;

{ TSQLQueryBuilder }

constructor TSQLQueryBuilder.Create(ADBType: TDBType; AURLQueryParams: TStrings);
var
   ParamCount: Integer;
begin
  inherited Create;
  FDBType := ADBType;
  FURLQueryParams := AURLQueryParams;
  FConditions := TList<TSQLQueryCondition>.Create;
  FSortFields := TList<TSQLSortField>.Create;
  FLimit := -1;
  FOffset := 0;
  ParseAllQueryParams;

  ParamCount:=0;
  If Assigned(AURLQueryParams) then
     ParamCount:=AURLQueryParams.Count;

  LogMessage(Format('TSQLQueryBuilder created for DBType: %s, with %d URL query params.',
    [ TRttiEnumerationType.GetName<TDBType>(FDBType),ParamCount ]), logDebug);
end;

destructor TSQLQueryBuilder.Destroy;
begin
  FreeAndNil(FConditions);
  FreeAndNil(FSortFields);
  LogMessage('TSQLQueryBuilder destroyed.', logDebug);
  inherited;
end;

procedure TSQLQueryBuilder.ParseAllQueryParams;
var
  I: Integer;
  ParamName, ParamValue: string;
begin
  if not Assigned(FURLQueryParams) then Exit;

  for I := 0 to FURLQueryParams.Count - 1 do
  begin
    ParamName := FURLQueryParams.Names[I];
    ParamValue := FURLQueryParams.ValueFromIndex[I];

    if ParamName.StartsWith('_') then
    begin
      if SameText(ParamName, '_sort') then
        ParseSortParam(ParamValue)
      else if SameText(ParamName, '_limit') then // Handle _limit and _offset separately now
      begin
        if TryStrToInt(ParamValue, FLimit) then
        begin
          if FLimit < 0 then FLimit := -1;
          LogMessage(Format('TSQLQueryBuilder: Parsed limit: %d', [FLimit]), logDebug);
        end
        else if ParamValue <> '' then
        begin
          LogMessage(Format('TSQLQueryBuilder: Invalid value for _limit: "%s". Ignoring.', [ParamValue]), logWarning);
          FLimit := -1;
        end;
      end
      else if SameText(ParamName, '_offset') then
      begin
        if TryStrToInt(ParamValue, FOffset) then
        begin
          if FOffset < 0 then FOffset := 0;
          LogMessage(Format('TSQLQueryBuilder: Parsed offset: %d', [FOffset]), logDebug);
        end
        else if ParamValue <> '' then
        begin
          LogMessage(Format('TSQLQueryBuilder: Invalid value for _offset: "%s". Ignoring.', [ParamValue]), logWarning);
          FOffset := 0;
        end;
      end
      else
        LogMessage(Format('TSQLQueryBuilder: Ignoring unknown special query parameter: %s', [ParamName]), logDebug);
    end
    else
    begin
      ParseFilterParam(ParamName, ParamValue);
    end;
  end;
  // ParsePaginationParams was called once after loop, now _limit and _offset are handled directly.
end;

procedure TSQLQueryBuilder.ParseFilterParam(const AParamName, AParamValue: string);
var
  LFieldName, LOperatorStr: string;
  LOperator: TSQLOperator;
  LMatch: TMatch;
  LCondition: TSQLQueryCondition;
  LValues: TArray<string>;
  LVariantValue: Variant;
  I: Integer;
begin
  LMatch := TRegEx.Match(AParamName, '^([\w\.]+)(?:\[(\w+)\])?$'); // Allows dots in field names for potential relations

  if not LMatch.Success then
  begin
    LogMessage(Format('TSQLQueryBuilder: Invalid filter parameter format: %s', [AParamName]), logWarning);
    Exit;
  end;

  LFieldName := LMatch.Groups[1].Value;
  // SECURITY: Field names should be validated against an allow-list by the caller (Controller)
  // before being used to construct queries to prevent SQL injection if field names are user-influenced.
  if not TRegEx.IsMatch(LFieldName, '^[a-zA-Z0-9_.]+$') then // Basic check for valid characters
  begin
    LogMessage(Format('TSQLQueryBuilder: Potentially unsafe field name detected in filter: "%s". Skipping condition.', [LFieldName]), logError);
    Exit;
  end;

  LOperatorStr := '';
  if LMatch.Groups[2].Success then
    LOperatorStr := LMatch.Groups[2].Value.ToLower;

  LOperator := GetOperatorFromString(LOperatorStr);

  LCondition.FieldName := LFieldName;
  LCondition.Operator := LOperator;
  LCondition.IsRawValue := False;

  case LOperator of
    sqoIsNull, sqoIsNotNull:
      begin
        LCondition.Value := Null;
        LCondition.IsRawValue := True;
      end;
    sqoIn, sqoNotIn:
      begin
        LValues := AParamValue.Split([',']);
        if Length(LValues) = 0 then
        begin
          LogMessage(Format('TSQLQueryBuilder: Empty value list for IN/NOT IN operator on field "%s".', [LFieldName]), logWarning);
          Exit;
        end;
        var VarArray: Variant;
        VarArray := VarArrayCreate([0, Length(LValues) - 1], varVariant);
        for I := 0 to Length(LValues) - 1 do
          VarArray[I] := Trim(LValues[I]);
        LCondition.Value := VarArray;
      end;
  else
    LCondition.Value := AParamValue;
  end;

  FConditions.Add(LCondition);
  LogMessage(Format('TSQLQueryBuilder: Parsed filter: Field="%s", Op="%s", Value="%s"',
    [LFieldName, TRttiEnumerationType.GetName<TSQLOperator>(LOperator), VarToStrDef(LCondition.Value, AParamValue)]), logDebug);
end;

procedure TSQLQueryBuilder.ParseSortParam(const ASortString: string);
var
  LFields: TArray<string>;
  LFieldAndDir: TArray<string>;
  LSortField: TSQLSortField;
  LFieldName: string;
  LPart: string;
begin
  if ASortString.Trim = '' then Exit;
  LFields := ASortString.Split([',']);

  for var FieldStr in LFields do
  if FieldStr.Trim<>'' then
  begin
    LSortField.Direction := sdAsc; // Default
    LFieldName := FieldStr;

    // Check for common suffixes _asc, _desc or prefixes -, +
    if FieldStr.EndsWith('_desc', True) then
    begin
      LFieldName := FieldStr.Substring(0, FieldStr.Length - Length('_desc'));
      LSortField.Direction := sdDesc;
    end
    else if FieldStr.EndsWith('_asc', True) then
    begin
      LFieldName := FieldStr.Substring(0, FieldStr.Length - Length('_asc'));
      LSortField.Direction := sdAsc;
    end
    else if FieldStr.StartsWith('-') then // e.g., -fieldName
    begin
      LFieldName := FieldStr.Substring(1);
      LSortField.Direction := sdDesc;
    end
    else if FieldStr.StartsWith('+') then // e.g., +fieldName
    begin
      LFieldName := FieldStr.Substring(1);
      LSortField.Direction := sdAsc;
    end;

    LFieldName := Trim(LFieldName);
    if LFieldName = '' then Continue;

    // SECURITY: Field names for sorting should also be validated against an allow-list by the caller.
    if not TRegEx.IsMatch(LFieldName, '^[a-zA-Z0-9_.]+$') then
    begin
      LogMessage(Format('TSQLQueryBuilder: Potentially unsafe field name detected in sort: "%s". Skipping sort field.', [LFieldName]), logError);
      Continue;
    end;

    LSortField.FieldName := LFieldName;
    FSortFields.Add(LSortField);
    LogMessage(Format('TSQLQueryBuilder: Parsed sort field: "%s" %s',
      [LSortField.FieldName, IfThen(LSortField.Direction = sdAsc, 'ASC', 'DESC')]), logDebug);
  end;
end;

procedure TSQLQueryBuilder.ParsePaginationParams;
// This method is now effectively empty as _limit and _offset are handled directly in ParseAllQueryParams.
// Kept for structure if other global params were to be added and processed post-loop.
begin
  // FLimit and FOffset are now parsed directly in ParseAllQueryParams.
end;

function TSQLQueryBuilder.GetOperatorFromString(const OpStr: string): TSQLOperator;
begin
  if OpStr = 'eq' then Result := sqoEquals
  else if OpStr = 'ne' then Result := sqoNotEquals
  else if OpStr = 'gt' then Result := sqoGreaterThan
  else if OpStr = 'gte' then Result := sqoGreaterThanOrEquals
  else if OpStr = 'lt' then Result := sqoLessThan
  else if OpStr = 'lte' then Result := sqoLessThanOrEquals
  else if OpStr = 'like' then Result := sqoLike
  else if OpStr = 'nlike' then Result := sqoNotLike
  else if OpStr = 'in' then Result := sqoIn
  else if OpStr = 'nin' then Result := sqoNotIn
  else if OpStr = 'isnull' then Result := sqoIsNull
  else if OpStr = 'isnotnull' then Result := sqoIsNotNull
  else Result := sqoEquals;
end;

function TSQLQueryBuilder.QuoteIdentifier(const Identifier: string): string;
begin
  // SECURITY WARNING: This function ONLY quotes identifiers.
  // It DOES NOT validate if the identifier is a legitimate column/table name.
  // Callers (Controllers) MUST validate/sanitize identifiers against a schema
  // or an allow-list BEFORE passing them to this builder to prevent SQL injection.
  if not TRegEx.IsMatch(Identifier, '^[a-zA-Z0-9_.]+$') then // Basic check for common valid chars
  begin
     LogMessage(Format('TSQLQueryBuilder: Potentially unsafe characters in identifier "%s" for quoting. This identifier should be validated by the caller.', [Identifier]), logError);
     // Depending on policy, either raise an error or return unquoted (which might cause SQL errors later, preferred over silent quoting of bad input)
     // For now, return as is, relying on DB errors for truly invalid names if not caught by caller.
     Result := Identifier;
     Exit;
  end;

  case FDBType of
    dbtMSSQL: Result := '[' + Identifier + ']';
    dbtPostgreSQL: Result := '"' + Identifier + '"';
    dbtMySQL: Result := '`' + Identifier + '`';
  else
    Result := Identifier; // Default for unknown or DBs not needing special quotes for simple names
  end;
end;

function TSQLQueryBuilder.FormatValueForSQL(const AValue: Variant; out IsParameter: Boolean): string;
begin
  IsParameter := True;
  Result := '?'; // Placeholder for parameterized query
end;

function TSQLQueryBuilder.GetOperatorSQL(AOperator: TSQLOperator): string;
begin
  case AOperator of
    sqoEquals: Result := '=';
    sqoNotEquals: Result := '<>';
    sqoGreaterThan: Result := '>';
    sqoGreaterThanOrEquals: Result := '>=';
    sqoLessThan: Result := '<';
    sqoLessThanOrEquals: Result := '<=';
    sqoLike: Result := 'LIKE';
    sqoNotLike: Result := 'NOT LIKE';
    sqoIn: Result := 'IN';
    sqoNotIn: Result := 'NOT IN';
    sqoIsNull: Result := 'IS NULL';
    sqoIsNotNull: Result := 'IS NOT NULL';
  else
    Result := '=';
  end;
end;

function TSQLQueryBuilder.GetWhereClause(out AParams: TFDParams): string;
var
  WhereBuilder: TStringBuilder;
  Condition: TSQLQueryCondition;
  IsParam: Boolean;
  FormattedValue: string;
  OpSQL: string;
  i, j: Integer; // Added j for inner loop
  ValueArray: Variant; // For IN clause
begin
  Result := '';
  AParams:= TFDParams.Create;
  if FConditions.Count = 0 then
     Exit;
  WhereBuilder := TStringBuilder.Create;
  try
    for i := 0 to FConditions.Count - 1 do
    begin
      Condition := FConditions[i];
      if WhereBuilder.Length > 0 then // Use Length check instead of i > 0 to handle skipped conditions
        WhereBuilder.Append(' AND ');

      OpSQL := GetOperatorSQL(Condition.Operator);
      WhereBuilder.Append(QuoteIdentifier(Condition.FieldName)).Append(' ').Append(OpSQL);

      if Condition.IsRawValue then
      begin
        // No parameter for IS NULL / IS NOT NULL
      end
      else if (Condition.Operator = sqoIn) or (Condition.Operator = sqoNotIn) then
      begin
        WhereBuilder.Append(' (');
        if VarIsArray(Condition.Value) and
           (VarArrayHighBound(Condition.Value, 1) >= VarArrayLowBound(Condition.Value, 1)) then
        begin
          ValueArray := Condition.Value;
          for j := VarArrayLowBound(ValueArray, 1) to VarArrayHighBound(ValueArray, 1) do
          begin
            if j > VarArrayLowBound(ValueArray, 1) then
               WhereBuilder.Append(', ');
            WhereBuilder.Append(':'+Condition.FieldName);
            AParams.Add(Condition.FieldName,ValueArray[j]);
          end;
        end
        else
        begin
           LogMessage(Format('TSQLQueryBuilder: Invalid or empty array for IN operator on field "%s". Skipping this part of condition.', [Condition.FieldName]), logWarning);
           // Attempt to remove the incomplete part of the condition
           if WhereBuilder.ToString.EndsWith(OpSQL) then // e.g., "FieldName IN "
              WhereBuilder.Length := WhereBuilder.Length - Length(OpSQL) - Length(QuoteIdentifier(Condition.FieldName)) - 2; // Remove " FieldName IN "
           if WhereBuilder.ToString.EndsWith(' AND ') then // If it was preceded by AND
              WhereBuilder.Length := WhereBuilder.Length - Length(' AND ');
           Continue; // Skip to next condition
        end;
        WhereBuilder.Append(')');
      end
      else
      begin
        FormattedValue := FormatValueForSQL(Condition.Value, IsParam); // Returns '?'
        WhereBuilder.Append(' ').Append(FormattedValue);
        if IsParam then
          AParams.Add(':'+Condition.FieldName,Condition.Value);
      end;
    end;
    Result := WhereBuilder.ToString;
  finally
    WhereBuilder.Free;
  end;
  if Result <> '' then
    LogMessage(Format('TSQLQueryBuilder: Generated WHERE clause: "%s" with %d params.', [Result, AParams.Count]), logDebug)
  else
    LogMessage('TSQLQueryBuilder: No WHERE clause generated.', logDebug);
end;

function TSQLQueryBuilder.GetOrderByClause(out AParams: TFDParams): string; // AParams ya no es necesario aquí
var
  OrderByBuilder: TStringBuilder;
  SortField: TSQLSortField;
  I: Integer;
begin
  Result := '';
  // AParams ya no se crea ni se usa aquí para los nombres de campo de ORDER BY.
  // Si en el futuro ORDER BY necesitara parámetros para otra cosa (no común), se revisaría.
  // Por ahora, se asume que los parámetros son solo para WHERE.
  // El TFDParams de salida se mantiene por si se extiende, pero no se usa.
  AParams := TFDParams.Create; // Crear para cumplir la firma, pero no se poblará.

  if FSortFields.Count = 0 then
     Exit;

  OrderByBuilder := TStringBuilder.Create;
  try
    OrderByBuilder.Append('ORDER BY ');
    for I := 0 to FSortFields.Count - 1 do
    begin
      SortField := FSortFields[I];
      // VALIDACIÓN IMPORTANTE: Se asume que FSortFields.FieldName ya ha sido validado
      // contra una lista blanca por el código que llamó a ParseSortParam o en el controlador
      // antes de construir el SQLQueryBuilder o antes de llamar a GetOrderByClause.
      // QuoteIdentifier aquí solo escapa, no valida contra la lista blanca de la entidad.
      if I > 0 then
        OrderByBuilder.Append(', ');
      OrderByBuilder.Append(QuoteIdentifier(SortField.FieldName)); // El nombre del campo se concatena directamente

      if SortField.Direction = sdDesc then
        OrderByBuilder.Append(' DESC')
      else
        OrderByBuilder.Append(' ASC'); // Explicit ASC is good practice
    end;
    Result := OrderByBuilder.ToString;
  finally
    OrderByBuilder.Free;
  end;
  if Result <> '' then
    LogMessage(Format('TSQLQueryBuilder: Generated ORDER BY clause: "%s"', [Result]), logDebug);
end;

function TSQLQueryBuilder.GetPaginationClause: string;
begin
  Result := '';
  if FLimit < 0 then
  begin
    if FOffset > 0 then
      LogMessage('TSQLQueryBuilder: Offset specified without limit. Pagination clause might be incomplete or ignored by DB.', logDebug);
    Exit;
  end;

  case FDBType of
    dbtPostgreSQL, dbtMySQL:
      begin
        Result := Format('LIMIT %d', [FLimit]);
        if FOffset > 0 then
          Result := Result + Format(' OFFSET %d', [FOffset]);
      end;
    dbtMSSQL: // SQL Server 2012+
      begin
        if FSortFields.Count = 0 then // OFFSET/FETCH requires ORDER BY
           LogMessage('TSQLQueryBuilder: MSSQL pagination (OFFSET/FETCH) requested without an ORDER BY clause. This will cause a SQL error.', logError);
        // Ensure FOffset is not negative, though ParseAllQueryParams should handle this
        Result := Format('OFFSET %d ROWS FETCH NEXT %d ROWS ONLY', [Max(0, FOffset), FLimit]);
      end;
  else
    LogMessage(Format('TSQLQueryBuilder: Pagination not implemented or not standard for DBType: %s. Limit/Offset: %d/%d',
       [TRttiEnumerationType.GetName<TDBType>(FDBType), FLimit, FOffset]), logWarning);
  end;

  if Result <> '' then
     LogMessage(Format('TSQLQueryBuilder: Generated PAGINATION clause: "%s"', [Result]), logDebug);
end;

// GetPaginationClauseMSSQL removed as OFFSET/FETCH is preferred and handled by GetPaginationClause.

end.

