unit uLib.Utils;

interface

uses
   System.JSON, System.Classes,
   System.Types, System.SysUtils, System.SyncObjs,
   Data.DB, System.UITypes, System.Generics.Collections
   ;

type
   TJSONHelper = Class
     protected
       class function GetValueRecursive(ANode: TJSONObject; const APath: string; out AValue: TJSONValue): Boolean;
     private
     public
       class function TryConvertJSONValueToType<T>(AJsonValue: TJSONValue; out AConvertedValue: T; const ADefaultForLog: T): Boolean;

       class function GetValue<T>(ANode: TJSONObject; const APath: string; const ADefault: T): T;
       class function GetJSONClone(ANode: TJSONObject; const ASectionPath: string): TJSONObject; overload;
       class function GetString(ANode: TJSONObject; const APath: string; const ADefault: string=''): string; overload;
       class function GetInteger(ANode: TJSONObject; const APath: string; const ADefault: Integer=0): Integer; overload;
       class function GetInt64(ANode: TJSONObject; const APath: string; const ADefault: Int64=0): Int64; overload;
       class function GetDouble(ANode: TJSONObject; const APath: string; const ADefault: Double=0): Double; overload;
       class function GetBoolean(ANode: TJSONObject; const APath: string; const ADefault: Boolean=false): Boolean; overload;
       class function GetJSONArray(ANode: TJSONObject; const APath: string): TJSONArray; overload;
       class function GetJSONObject(ANode: TJSONObject; const APath: string): TJSONObject; overload;

       class function GetString(const ANode: string; const APath: string; const ADefault: string=''): string; overload;
       class function GetInteger(const ANode: string; const APath: string; const ADefault: Integer=0): Integer; overload;
       class function GetInt64(const ANode: string; const APath: string; const ADefault: Int64=0): Int64; overload;
       class function GetDouble(const ANode: string; const APath: string; const ADefault: Double=0): Double; overload;
       class function GetBoolean(const ANode: string; const APath: string; const ADefault: Boolean=false): Boolean; overload;
       class function GetJSONArray(const ANode: string; const APath: string): TJSONArray; overload;
       class function GetJSONObject(const ANode: string; const APath: string): TJSONObject; overload;
   End;

  TDataSetHelper = class helper for TDataSet
  public
    function AsJSONvalue(AReturnNilIfEOF: boolean = false): TJSONValue;

    function AsJSONObject(AReturnNilIfEOF: boolean = false): TJSONObject;
    function AsJSONObjectString(AReturnNilIfEOF: boolean = false): string;

    function AsJSONArray(AReturnNilIfEOF: boolean = false): TJSONArray;
    function AsJSONArrayString(AReturnNilIfEOF: boolean = false): string;

    function AsStringList(Separator: Char): TStringList;
    procedure LoadFromJSONArray( AJSONArray: TJSONArray); overload;
    procedure LoadFromJSONArray( AJSONArray: TJSONArray;
                                 AIgnoredFields: TArray<string>); overload;
    procedure LoadFromJSONArrayString( AJSONArrayString: string;
                                       AIgnoredFields: TArray<string>); overload;
    procedure LoadFromJSONArrayString( AJSONArrayString: string); overload;
    procedure AppendFromJSONArrayString( AJSONArrayString: string); overload;
    procedure AppendFromJSONArrayString( AJSONArrayString: string;
                                         AIgnoredFields: TArray<string>); overload;
  end;

function NowUTC: TDateTime;
function GetStrPair( const AParamString, AFieldName: String;
                     const ADefaultValue: string;
                     APairSeparator: Char = ';';
                     AValueSeparator: Char = '='): String;
function QuoteIdentifier(const Name: String; aBeginChar: Char; aEndChar: Char=#0): String;
function EnsurePathHasTrailingDelimiter(const APath: string): string;
function IsStringInArray(const AString: string; const AArray: array of string;
                               ACaseInsensitive: Boolean = False): Boolean;

implementation

uses
    System.DateUtils
   ,System.RTTI
   ,System.NetEncoding
   ,WinApi.Windows
   ,Soap.EncdDecd
   ,Data.SqlTimSt
   ,Data.FmtBcd
   ,uLib.Logger
   ;


function NowUTC: TDateTime;
var
  LocalTime, UTCTime: TSystemTime;
begin
  DateTimeToSystemTime(Now, LocalTime);
  if TzSpecificLocalTimeToSystemTime(nil, LocalTime, UTCTime) then
    Result := SystemTimeToDateTime(UTCTime)
  else
    Result := Now; // En caso de error, regresa la hora local
end;

function EnsurePathHasTrailingDelimiter(const APath: string): string;
begin
  Result := IncludeTrailingPathDelimiter(APath);
end;

function GetStrPair( const AParamString, AFieldName: String;
                     const ADefaultValue: string;
                     APairSeparator: Char = ';';
                     AValueSeparator: Char = '='): String;
var
  Pairs: TArray<string>;
  Pair: string;
  I,
  SeparatorPos: Integer;
  CurrentKey,
  CurrentValue: string;
begin
  Result := ADefaultValue;
  if AParamString.IsEmpty or AFieldName.IsEmpty then
     Exit;
  Pairs := AParamString.Split([APairSeparator]);
  for I := Low(Pairs) to High(Pairs) do
    begin
      Pair := Pairs[I].Trim; // <--- Usar la variable local y hacer Trim aqu�
      if Pair.IsEmpty then
        Continue;
      SeparatorPos := Pos(AValueSeparator, Pair);
      if SeparatorPos > 0 then // Asegurar que el separador exista y no est� al inicio
      begin
        CurrentKey := Copy(Pair, 1, SeparatorPos - 1).Trim; // Extraer y limpiar la clave
        if SameText(CurrentKey, AFieldName) then
        begin
          CurrentValue := Copy(Pair, SeparatorPos + 1, Length(Pair) - SeparatorPos);
          Result := CurrentValue;
          Exit; // Salir al encontrar la primera coincidencia
        end;
      end
      else
      begin
      end;
    end;
end;

function QuoteIdentifier(const Name: String; aBeginChar: Char; aEndChar: Char=#0): String;
var
   sResp: String;
begin
  if aEndChar = #0 then
     case aBeginChar of
      '[': aEndChar := ']';
      '{': aEndChar := '}';
      '<': aEndChar := '>';
      '(': aEndChar := ')';
      '"': aEndChar := '"';
      '''': aEndChar := '''';
      '`': aEndChar := '`';
      else
         aEndChar := aBeginChar;
     end;
  result:=abeginChar+Name+aEndChar;
end;

function IsStringInArray(const AString: string; const AArray: array of string; ACaseInsensitive: Boolean = False): Boolean;
var
  S: string;
  StringToCompare, ArrayItemToCompare: string;
begin
  Result := False;
  StringToCompare := AString;
  if ACaseInsensitive then
    StringToCompare := LowerCase(AString);

  for S in AArray do
  begin
    ArrayItemToCompare := S;
    if ACaseInsensitive then
      ArrayItemToCompare := LowerCase(S);

    if StringToCompare = ArrayItemToCompare then
    begin
      Result := True;
      Exit;
    end;
  end;
end;


class function TJSONHelper.TryConvertJSONValueToType<T>( AJsonValue: TJSONValue; out AConvertedValue: T; const ADefaultForLog: T): Boolean;
var
  LIntValue: Integer;
  LInt64Value: Int64;
  LDoubleValue: Double;
  LBoolValue: Boolean;
  LStrValue: string;
begin
  Result := True; // Assume success initially

  if not Assigned(AJsonValue) or (AJsonValue is TJSONNull) then
  begin
    AConvertedValue := ADefaultForLog; // Use default for type consistency if conversion isn't possible
    Result := False; // Indicates that we used default because value was null or not convertible
    if Assigned(AJsonValue) and (AJsonValue is TJSONNull) then Result := True; // Null is a valid "value" that results in default
    Exit;
  end;

  try
    if TypeInfo(T) = TypeInfo(string) then
    begin
      if AJsonValue is TJSONString then
        AConvertedValue := TValue.From<string>(TJSONString(AJsonValue).Value).AsType<T>
      else if AJsonValue is TJSONNumber then // Permitir conversi�n de n�mero a string
        AConvertedValue := TValue.From<string>(TJSONNumber(AJsonValue).Value).AsType<T>
      else if AJsonValue is TJSONBool then // Permitir conversi�n de booleano a string
        begin
          LStrValue := LowerCase(Trim(TJSONString(AJsonValue).Value));
          if (LStrValue = 'true') or (LStrValue = '1') or (LStrValue = 'yes') or (LStrValue = 'on') then
             AConvertedValue := TValue.From<Boolean>(True).AsType<T>
          else if (LStrValue = 'false') or (LStrValue = '0') or (LStrValue = 'off') then
             AConvertedValue := TValue.From<Boolean>(False).AsType<T>
          else
             Result := False;
        end
      else
        Result := False;
    end
    else if TypeInfo(T) = TypeInfo(Integer) then
    begin
      if AJsonValue is TJSONNumber then
        AConvertedValue := TValue.From<Integer>(Trunc(TJSONNumber(AJsonValue).AsInt)).AsType<T>
      else if AJsonValue is TJSONString then
      begin
        if TryStrToInt(TJSONString(AJsonValue).Value, LIntValue) then
          AConvertedValue := TValue.From<Integer>(LIntValue).AsType<T>
        else
          Result := False;
      end
      else
        Result := False;
    end
    else if TypeInfo(T) = TypeInfo(Int64) then
    begin
      if AJsonValue is TJSONNumber then
        AConvertedValue := TValue.From<Int64>(TJSONNumber(AJsonValue).AsInt64).AsType<T>
      else if AJsonValue is TJSONString then
      begin
        if TryStrToInt64(TJSONString(AJsonValue).Value, LInt64Value) then
          AConvertedValue := TValue.From<Int64>(LInt64Value).AsType<T>
        else
          Result := False;
      end
      else
        Result := False;
    end
    else if TypeInfo(T) = TypeInfo(Boolean) then
    begin
      if AJsonValue is TJSONBool then
        AConvertedValue := TValue.From<Boolean>(TJSONBool(AJsonValue).AsBoolean).AsType<T>
      else if AJsonValue is TJSONString then
      begin
        LStrValue := LowerCase(Trim(TJSONString(AJsonValue).Value));  // CORRECCIÓN: Era TJSONBool, debe ser TJSONString
        if (LStrValue = 'true') or (LStrValue = '1') or (LStrValue = 'yes') or (LStrValue = 'on') then
          AConvertedValue := TValue.From<Boolean>(True).AsType<T>
        else if (LStrValue = 'false') or (LStrValue = '0') or (LStrValue = 'off') then
          AConvertedValue := TValue.From<Boolean>(False).AsType<T>
        else
          Result := False;
      end
      else if AJsonValue is TJSONNumber then // 0 es false, no-cero es true
        AConvertedValue := TValue.From<Boolean>(TJSONNumber(AJsonValue).AsInt <> 0).AsType<T>
      else
        Result := False;
    end
    else if TypeInfo(T) = TypeInfo(Double) then
    begin
      if AJsonValue is TJSONNumber then
        AConvertedValue := TValue.From<Double>(TJSONNumber(AJsonValue).AsDouble).AsType<T>
      else if AJsonValue is TJSONString then
      begin
        // Use FormatSettings sensible a la localizaci�n para TryStrToFloat si es necesario,
        // pero los valores JSON suelen usar '.' como separador decimal.
        if TryStrToFloat(TJSONString(AJsonValue).Value, LDoubleValue) then // TFormatSettings.Invariant?
          AConvertedValue := TValue.From<Double>(LDoubleValue).AsType<T>
        else
          Result := False;
      end
      else
        Result := False;
    end
    else if (TypeInfo(T) = TypeInfo(TJSONObject)) and (AJsonValue is TJSONObject) then
    begin
      // Devolver un CLON para que el llamador pueda poseerlo y modificarlo sin afectar FConfigData
      AConvertedValue := TValue.From<TJSONObject>((AJsonValue as TJSONObject).Clone as TJSONObject).AsType<T>;
    end
    else if (TypeInfo(T) = TypeInfo(TJSONArray)) and (AJsonValue is TJSONArray) then
    begin
      // Devolver un CLON
      AConvertedValue := TValue.From<TJSONArray>((AJsonValue as TJSONArray).Clone as TJSONArray).AsType<T>;
    end
    else
    begin
      // Tipo T no soportado directamente por esta funci�n de conversi�n.
      Result := False;
    end;

    if not Result then // Si la conversi�n fall� para un tipo conocido pero valor incompatible
    begin
      AConvertedValue := ADefaultForLog; // Asignar el default del tipo T
      LogMessage(Format('TConfigManager.TryConvertJSONValueToType: Failed to convert JSON value of type "%s" (Value: "%s") to target type "". Using default.',
        [AJsonValue.ClassName, Copy(AJsonValue.Value, 1, 50),TypeInfo(T)]), logWarning);
    end;

  except
    on E: Exception do // Excepci�n durante la conversi�n (ej. TValue.AsType<T>)
    begin
      AConvertedValue := ADefaultForLog;
      Result := False;
      LogMessage(Format('TConfigManager.TryConvertJSONValueToType: Exception converting JSON value of type "%s" to target type "". Error: %s. Using default.',
        [AJsonValue.ClassName,  E.Message]), logError);
    end;
  end;
end;

class function TJSONHelper.GetJSONClone(ANode: TJSONObject; const ASectionPath: string): TJSONObject;
var
  LJsonValue: TJSONValue;
begin
  Result := nil;
  if Assigned(ANode) then
  begin
    if GetValueRecursive(ANode, ASectionPath, LJsonValue) and Assigned(LJsonValue) and (LJsonValue is TJSONObject) then
      Result := (LJsonValue as TJSONObject).Clone as TJSONObject
    else
      Result := TJSONObject.Create;
  end
  else
    Result := TJSONObject.Create;
end;

class function TJSONHelper.GetValueRecursive(ANode: TJSONObject; const APath: string; out AValue: TJSONValue): Boolean;
var
  PathParts: TArray<string>;
  CurrentNode: TJSONObject;
  CurrentValue: TJSONValue;
  I: Integer;
  Key: string;
begin
  Result := False;
  AValue := nil;
  if not Assigned(ANode) or (APath = '') then
     Exit;

  PathParts := APath.Split(['.']);
  CurrentNode := ANode;

  for I := 0 to High(PathParts) do
  begin
    Key := PathParts[I];
    if not Assigned(CurrentNode) then Exit; // No se puede seguir el path

    if not CurrentNode.TryGetValue(Key, CurrentValue) then
      Exit; // Clave no encontrada

    if I = High(PathParts) then // �ltima parte del path, hemos encontrado el valor
    begin
      AValue := CurrentValue;
      Result := True;
      Exit;
    end
    else // No es la �ltima parte, esperamos un TJSONObject para seguir navegando
    begin
      if CurrentValue is TJSONObject then
        CurrentNode := CurrentValue as TJSONObject
      else
        Exit; // Parte intermedia no es un objeto, no se puede seguir
    end;
  end;
end;

class function TJSONHelper.GetValue<T>(ANode: TJSONObject; const APath: string; const ADefault: T): T;
var
  LJsonValue: TJSONValue;
begin
  if Assigned(ANode) and GetValueRecursive(ANode, APath, LJsonValue) then
     begin
       // LJsonValue es el TJSONValue encontrado en APath.
       // Si LJsonValue es nil (porque GetValueRecursive devolvi� true pero el valor era JSON null),
       // o si la conversi�n falla, TryConvertJSONValueToType deber�a manejarlo y devolver ADefault.
       if TryConvertJSONValueToType<T>(LJsonValue, Result, ADefault) then
          ;
     end
  else // Path no encontrado o FConfigData es nil
     Result := ADefault;
end;


class function TJSONHelper.GetString(ANode: TJSONObject; const APath: string; const ADefault: string): string;
var
  LJsonValue: TJSONValue;
begin
  Result := ADefault;
  if Assigned(ANode) and GetValueRecursive(ANode, APath, LJsonValue) and Assigned(LJsonValue) then
  begin
    if LJsonValue is TJSONString then
      Result := TJSONString(LJsonValue).Value
    else if not (LJsonValue is TJSONNull) then // Permitir que n�meros o booleanos se conviertan a string
      Result := LJsonValue.Value
    // else (es TJSONNull o no se encontr�), Result queda como ADefault
  end;
end;

class function TJSONHelper.GetInteger(ANode: TJSONObject; const APath: string; const ADefault: Integer): Integer;
var
  LJsonValue: TJSONValue;
  LStrValue: string;
begin
  Result := ADefault;
  if Assigned(ANode) and GetValueRecursive(ANode, APath, LJsonValue) and Assigned(LJsonValue) then
  begin
    if LJsonValue is TJSONNumber then
      Result := Trunc(TJSONNumber(LJsonValue).AsInt) // O AsInt, AsInt64 si se sabe el rango
    else if LJsonValue is TJSONString then // Intentar convertir desde string
    begin
      LStrValue := TJSONString(LJsonValue).Value;
      if not TryStrToInt(LStrValue, Result) then // Si la conversi�n falla, Result mantiene ADefault
        Result := ADefault;
    end;
  end;
end;

class function TJSONHelper.GetDouble(ANode: TJSONObject; const APath: string; const ADefault: Double): Double;
var
  LJsonValue: TJSONValue;
  LStrValue: string;
begin
  Result := ADefault;
  if Assigned(ANode) and GetValueRecursive(ANode, APath, LJsonValue) and Assigned(LJsonValue) then
  begin
    if LJsonValue is TJSONNumber then
      Result := TJSONNumber(LJsonValue).AsDouble  // CORRECCIÓN: Quitar Trunc
    else if LJsonValue is TJSONString then
    begin
      LStrValue := TJSONString(LJsonValue).Value;
      if not TryStrToFloat(LStrValue, Result) then
        Result := ADefault;
    end;
  end;
end;

class function TJSONHelper.GetInt64(ANode: TJSONObject; const APath: string; const ADefault: Int64): Int64;
var
  LJsonValue: TJSONValue;
  LStrValue: string;
begin
  Result := ADefault;
  if Assigned(ANode) and GetValueRecursive(ANode, APath, LJsonValue) and Assigned(LJsonValue) then
  begin
    if LJsonValue is TJSONNumber then
      Result := TJSONNumber(LJsonValue).AsInt64  // CORRECCIÓN: Usar AsInt64 en lugar de Trunc(AsDouble)
    else if LJsonValue is TJSONString then
    begin
      LStrValue := TJSONString(LJsonValue).Value;
      if not TryStrToInt64(LStrValue, Result) then
        Result := ADefault;
    end;
  end;
end;

class function TJSONHelper.GetBoolean(ANode: TJSONObject; const APath: string; const ADefault: Boolean): Boolean;
var
  LJsonValue: TJSONValue;
  LStrValue: string;
begin
  Result := ADefault;
  if Assigned(ANode) and GetValueRecursive(ANode, APath, LJsonValue) and Assigned(LJsonValue) then
  begin
    if LJsonValue is TJSONBool then
      Result := TJSONBool(LJsonValue).AsBoolean
    else if LJsonValue is TJSONString then
    begin
      LStrValue := TJSONString(LJsonValue).Value.ToLower;
      if (LStrValue = 'true') or (LStrValue = '1') or
         (LStrValue = 'yes') or (LStrValue = 'on') then
        Result := True
      else if (LStrValue = 'false') or (LStrValue = '0') or
               (LStrValue = 'no') or (LStrValue = 'off') then  // CORRECCIÓN: Agregar 'no'
        Result := False;
      // else Result mantiene ADefault
    end
    else if LJsonValue is TJSONNumber then
    begin
      Result := (TJSONNumber(LJsonValue).AsInt <> 0);
    end;
  end;
end;

class function TJSONHelper.GetJSONArray(ANode: TJSONObject; const APath: string): TJSONArray;
var
  LJsonValue: TJSONValue;
begin
  Result := nil; // Devuelve nil si no se encuentra o no es un array
  if Assigned(ANode) and GetValueRecursive(ANode, APath, LJsonValue) and Assigned(LJsonValue) then
  begin
    if LJsonValue is TJSONArray then
      Result := LJsonValue as TJSONArray; // Devuelve la referencia, no un clon
  end;
end;

class function TJSONHelper.GetJSONObject(ANode: TJSONObject; const APath: string): TJSONObject;
var
  LJsonValue: TJSONValue;
begin
  Result := nil; // Devuelve nil si no se encuentra o no es un objeto
  if Assigned(ANode) and GetValueRecursive(ANode, APath, LJsonValue) and Assigned(LJsonValue) then
  begin
    if LJsonValue is TJSONObject then
      Result := LJsonValue as TJSONObject; // Devuelve la referencia, no un clon
  end;
end;


class function TJSONHelper.GetString(const ANode: string; const APath: string; const ADefault: string): string;
var
  AJSON: TJSONObject;
begin
  Result := ADefault; // CORRECCIÓN: Inicializar con default

  if ANode.Trim.IsEmpty then Exit; // CORRECCIÓN: Validar entrada

  try
    AJSON := TJSONObject.ParseJSONValue(TEncoding.UTF8.GetBytes(ANode), 0) as TJSONObject;
    try
      if Assigned(AJSON) then
        Result := GetString(AJSON, APath, ADefault) // CORRECCIÓN: Usar método existente
      // else Result queda como ADefault
    finally
      FreeAndNil(AJSON); // CORRECCIÓN: Siempre liberar
    end;
  except
    on E: Exception do
    begin
      LogMessage(Format('TJSONHelper.GetString: Error parsing JSON string: %s', [E.Message]), logError);
      Result := ADefault;
    end;
  end;
end;

class function TJSONHelper.GetInteger(const ANode: string; const APath: string; const ADefault: Integer): Integer;
var
  AJSON: TJSONObject;
begin
  Result := ADefault; // CORRECCIÓN: Inicializar con default

  if ANode.Trim.IsEmpty then Exit; // CORRECCIÓN: Validar entrada

  try
    AJSON := TJSONObject.ParseJSONValue(TEncoding.UTF8.GetBytes(ANode), 0) as TJSONObject;
    try
      if Assigned(AJSON) then
        Result := GetInteger(AJSON, APath, ADefault) // CORRECCIÓN: Usar método existente
    finally
      FreeAndNil(AJSON); // CORRECCIÓN: Siempre liberar
    end;
  except
    on E: Exception do
    begin
      LogMessage(Format('TJSONHelper.GetInteger: Error parsing JSON string: %s', [E.Message]), logError);
      Result := ADefault;
    end;
  end;
end;

class function TJSONHelper.GetInt64(const ANode: string; const APath: string; const ADefault: Int64): Int64;
var
  AJSON: TJSONObject;
begin
  Result := ADefault;

  if ANode.Trim.IsEmpty then Exit;

  try
    AJSON := TJSONObject.ParseJSONValue(TEncoding.UTF8.GetBytes(ANode), 0) as TJSONObject;
    try
      if Assigned(AJSON) then
        Result := GetInt64(AJSON, APath, ADefault);
    finally
      FreeAndNil(AJSON);
    end;
  except
    on E: Exception do
    begin
      LogMessage(Format('TJSONHelper.GetInt64: Error parsing JSON string: %s', [E.Message]), logError);
      Result := ADefault;
    end;
  end;
end;

class function TJSONHelper.GetDouble(const ANode: string; const APath: string; const ADefault: Double): Double;
var
  AJSON: TJSONObject;
begin
  Result := ADefault;

  if ANode.Trim.IsEmpty then Exit;

  try
    AJSON := TJSONObject.ParseJSONValue(TEncoding.UTF8.GetBytes(ANode), 0) as TJSONObject;
    try
      if Assigned(AJSON) then
        Result := GetDouble(AJSON, APath, ADefault);
    finally
      FreeAndNil(AJSON);
    end;
  except
    on E: Exception do
    begin
      LogMessage(Format('TJSONHelper.GetDouble: Error parsing JSON string: %s', [E.Message]), logError);
      Result := ADefault;
    end;
  end;
end;

class function TJSONHelper.GetBoolean(const ANode: string; const APath: string; const ADefault: Boolean): Boolean;
var
  AJSON: TJSONObject;
begin
  Result := ADefault;

  if ANode.Trim.IsEmpty then Exit;

  try
    AJSON := TJSONObject.ParseJSONValue(TEncoding.UTF8.GetBytes(ANode), 0) as TJSONObject;
    try
      if Assigned(AJSON) then
        Result := GetBoolean(AJSON, APath, ADefault);
    finally
      FreeAndNil(AJSON);
    end;
  except
    on E: Exception do
    begin
      LogMessage(Format('TJSONHelper.GetBoolean: Error parsing JSON string: %s', [E.Message]), logError);
      Result := ADefault;
    end;
  end;
end;

// CORRECCIÓN CRÍTICA: Los métodos GetJSONArray y GetJSONObject tienen lógica incorrecta
class function TJSONHelper.GetJSONArray(const ANode: string; const APath: string): TJSONArray;
var
  AJSON: TJSONObject;
  ResultArray: TJSONArray;
begin
  Result := nil;

  if ANode.Trim.IsEmpty then Exit;

  try
    AJSON := TJSONObject.ParseJSONValue(TEncoding.UTF8.GetBytes(ANode), 0) as TJSONObject;
    try
      if Assigned(AJSON) then
      begin
        ResultArray := GetJSONArray(AJSON, APath);
        if Assigned(ResultArray) then
          Result := ResultArray.Clone as TJSONArray; // CORRECCIÓN: Retornar clon
      end;
    finally
      FreeAndNil(AJSON);
    end;
  except
    on E: Exception do
    begin
      LogMessage(Format('TJSONHelper.GetJSONArray: Error parsing JSON string: %s', [E.Message]), logError);
      Result := nil;
    end;
  end;
end;

class function TJSONHelper.GetJSONObject(const ANode: string; const APath: string): TJSONObject;
var
  AJSON: TJSONObject;
  ResultObject: TJSONObject;
begin
  Result := nil;

  if ANode.Trim.IsEmpty then Exit;

  try
    AJSON := TJSONObject.ParseJSONValue(TEncoding.UTF8.GetBytes(ANode), 0) as TJSONObject;
    try
      if Assigned(AJSON) then
      begin
        ResultObject := GetJSONObject(AJSON, APath);
        if Assigned(ResultObject) then
          Result := ResultObject.Clone as TJSONObject; // CORRECCIÓN: Retornar clon
      end;
    finally
      FreeAndNil(AJSON);
    end;
  except
    on E: Exception do
    begin
      LogMessage(Format('TJSONHelper.GetJSONObject: Error parsing JSON string: %s', [E.Message]), logError);
      Result := nil;
    end;
  end;
end;

{ TDataSetHelper }

procedure DataSetToJSONObject(
             ADataSet: TDataSet;
             AJSONObject: TJSONObject;
             ADataSetInstanceOwner: boolean);
var
  I: Integer;
  key: string;
  //ts: TSQLTimeStamp;
  MS: TMemoryStream;
  SS: TStringStream;
begin
  for I := 0 to ADataSet.FieldCount - 1 do
  begin
    key := ADataSet.Fields[I].FieldName.ToLower;

    if ADataSet.Fields[I].IsNull then
    begin
      AJSONObject.AddPair(key,TJSONNull.Create);
      Continue;
    end;
    case ADataSet.Fields[I].DataType of
      TFieldType.ftInteger, TFieldType.ftLongWord, TFieldType.ftAutoInc, TFieldType.ftSmallint,
        TFieldType.ftShortint:
        AJSONObject.AddPair(key,TJSONNumber.Create(ADataSet.Fields[I].AsInteger));
      TFieldType.ftLargeint:
        begin
          AJSONObject.AddPair(key,TJSONNumber.Create(ADataSet.Fields[I].AsLargeInt));
        end;
      TFieldType.ftSingle,
      TFieldType.ftFloat: AJSONObject.AddPair(key,TJSONNumber.Create(ADataSet.Fields[I].AsFloat));
      ftWideString, ftMemo, ftWideMemo:
        begin
          AJSONObject.AddPair(key,TJSONString.Create(ADataSet.Fields[I].AsWideString));
        end;
      ftString:
        begin
          AJSONObject.AddPair(key,TJSONString.Create(ADataSet.Fields[I].AsString));
        end;
      TFieldType.ftDate,
      TFieldType.ftDateTime:
        AJSONObject.AddPair(key,TJSONString.Create(
           DateToISO8601(ADataSet.Fields[I].AsDateTime)));
      TFieldType.ftTimeStamp:
          AJSONObject.AddPair(key,TJSONString.Create(
              DateToISO8601(SQLTimeStampToDateTime(ADataSet.Fields[I].AsSQLTimeStamp))));
      TFieldType.ftCurrency:
        AJSONObject.AddPair(key,TJSONNumber.Create(ADataSet.Fields[I].AsCurrency));
      TFieldType.ftBCD, TFieldType.ftFMTBcd:
        AJSONObject.AddPair(key,TJSONNumber.Create(BcdToDouble(ADataSet.Fields[I].AsBcd)));
      TFieldType.ftGraphic,
      TFieldType.ftBlob,
      TFieldType.ftStream:
        begin
          MS := TMemoryStream.Create;
          try
            TBlobField(ADataSet.Fields[I]).SaveToStream(MS);
            MS.Position := 0;
            SS := TStringStream.Create('', TEncoding.UTF8);
            try
              EncodeStream(MS, SS);
              SS.Position := 0;
              AJSONObject.AddPair(key,TJSONString.Create(SS.DataString));
            finally
              SS.Free;
            end;
          finally
            MS.Free;
          end;
        end;
      TFieldType.ftBoolean:
        begin
          AJSONObject.AddPair(key,TJSONNumber.Create(Ord(ADataSet.Fields[I].AsBoolean)));
        end;
    end;
  end;
  if ADataSetInstanceOwner then
    FreeAndNil(ADataSet);
end;

procedure DataSetToJSONvalue(
             ADataSet: TDataSet;
             AJSONValue: TJSONValue;
             ADataSetInstanceOwner: boolean);
var AJSON: TJSONObject;
begin
  DataSetToJSONObject(ADataSet,AJSON,ADataSetInstanceOwner);
  AJSONValue:=AJSON.AsType<TJSONValue>;
end;

procedure DataSetToString(
             ADataSet: TDataSet;
             ADataRow: String;
             Separator: Char;
             ADataSetInstanceOwner: boolean);
var
  I: Integer;
  MS: TMemoryStream;
  SS: TStringStream;
begin
  ADataRow:='';
  for I := 0 to ADataSet.FieldCount - 1 do
  begin
    if ADataSet.Fields[I].IsNull then
    begin
      ADataRow:=ADataRow+''+Separator;
      Continue;
    end;
    case ADataSet.Fields[I].DataType of
      ftInteger, ftLongWord,
      ftAutoInc, ftSmallint,
      ftShortint, ftLargeint,

      ftSingle, ftFloat:
         ADataRow:=ADataRow+ADataSet.Fields[I].AsString+Separator;

      ftWideString, ftMemo, ftWideMemo, ftString:
         ADataRow:=ADataRow+ADataSet.Fields[I].AsString+Separator;
      ftDate,ftDateTime, ftTimeStamp:
         ADataRow:=ADataRow+ADataSet.Fields[I].AsString+Separator;
      ftCurrency, ftBCD, ftFMTBcd:
        ADataRow:=ADataRow+ADataSet.Fields[I].AsString+Separator;
      ftGraphic,
      ftBlob,
      ftStream:
        begin
          MS := TMemoryStream.Create;
          try
            TBlobField(ADataSet.Fields[I]).SaveToStream(MS);
            MS.Position := 0;
            SS := TStringStream.Create('', TEncoding.UTF8);
            try
              EncodeStream(MS, SS);
              SS.Position := 0;
              ADataRow:=ADataRow+SS.DataString+Separator;
            finally
              SS.Free;
            end;
          finally
            MS.Free;
          end;
        end;
      TFieldType.ftBoolean:
        begin
          ADataRow:=ADataRow+IntToStr(Ord(ADataSet.Fields[I].AsBoolean))+Separator;
        end;
    end;
  end;
  if ADataSetInstanceOwner then
    FreeAndNil(ADataSet);
end;

function ContainsFieldName(const FieldName: string;
  var FieldsArray: TArray<string>): boolean;
var
  I: Integer;
begin
  for I := 0 to Length(FieldsArray) - 1 do
  begin
    if SameText(FieldsArray[I], FieldName) then
      Exit(True);
  end;
  Result := false;
end;

procedure JSONObjectToDataSet(
            AJSONObject: TJSONObject;
            ADataSet: TDataSet;
            AIgnoredFields: TArray<string>;
            AJSONObjectInstanceOwner: boolean);
var
  I: Integer;
  key: string;
  fs: TFormatSettings;
  MS: TMemoryStream;
  SS: TStringStream;
begin
  for I := 0 to ADataSet.FieldCount - 1 do
  begin
    if ContainsFieldName(ADataSet.Fields[I].FieldName, AIgnoredFields) then
      Continue;
    key := ADataSet.Fields[I].FieldName;
    case ADataSet.Fields[I].DataType of
      TFieldType.ftInteger, TFieldType.ftLongWord,
      TFieldType.ftAutoInc, TFieldType.ftSmallint,
      TFieldType.ftShortint:
        begin
          ADataSet.Fields[I].AsInteger := StrToInt(AJSONObject.GetValue(key).Value);
        end;
      TFieldType.ftLargeint:
        begin
          ADataSet.Fields[I].AsLargeInt := StrToInt(AJSONObject.GetValue(key).Value);
        end;
      TFieldType.ftSingle, TFieldType.ftFloat:
        begin
          ADataSet.Fields[I].AsFloat := StrToFloat(AJSONObject.GetValue(key).Value);
        end;
      ftString, ftWideString, ftMemo, ftWideMemo:
        begin
          ADataSet.Fields[I].AsString := AJSONObject.GetValue(key).Value;
        end;
      TFieldType.ftDate:
        begin
          ADataSet.Fields[I].AsDateTime :=ISO8601ToDate(AJSONObject.GetValue(key).Value);
        end;
      TFieldType.ftDateTime:
        begin
          ADataSet.Fields[I].AsDateTime :=ISO8601ToDate(AJSONObject.GetValue(key).Value);
        end;
      TFieldType.ftTimeStamp:
        begin
          ADataSet.Fields[I].AsSQLTimeStamp :=DateTimeToSQLTimeStamp(ISO8601ToDate(AJSONObject.GetValue(key).Value));
        end;
      TFieldType.ftCurrency:
        begin
          fs.DecimalSeparator := '.';
          { ,$IFNDEF TOJSON }
          // ADataSet.Fields[I].AsCurrency :=
          // StrToCurr((v as TJSONString).Value, fs);
          { .$ELSE } // Delphi XE7 introduces method "ToJSON" to fix some old bugs...
          ADataSet.Fields[I].AsCurrency :=
            StrToCurr( floatToStr(StrToFloat(AJSONObject.GetValue(key).Value)), fs);
          { .$IFEND }
        end;
      TFieldType.ftFMTBcd:
        begin
          ADataSet.Fields[I].AsBcd := DoubleToBcd(StrToFloat(AJSONObject.GetValue(key).Value));
        end;
      TFieldType.ftGraphic, TFieldType.ftBlob, TFieldType.ftStream:
        begin
          MS := TMemoryStream.Create;
          try
            SS := TStringStream.Create(AJSONObject.GetValue(key).Value,
              TEncoding.UTF8);
            try
              DecodeStream(SS, MS);
              MS.Position := 0;
              TBlobField(ADataSet.Fields[I]).LoadFromStream(MS);
            finally
              SS.Free;
            end;
          finally
            MS.Free;
          end;
        end;
      TFieldType.ftBoolean:
        begin
{$IFDEF JSONBOOL}
           ADataSet.Fields[I].AsBoolean := AJSONObject.GetValue(key).Value;
{$ELSE}
          if StrToInt(AJSONObject.GetValue(key).Value)=1 then
            ADataSet.Fields[I].AsBoolean := True
          else
            ADataSet.Fields[I].AsBoolean := false;
{$ENDIF}
        end;
      // else
      // raise EMapperException.Create('Cannot find type for field ' + key);
    end;
  end;
  if AJSONObjectInstanceOwner then
    FreeAndNil(AJSONObject);
end;

procedure DataSetToJSONArray(
            ADataSet: TDataSet;
            AJSONArray: TJSONArray;
            ADataSetInstanceOwner: boolean);
var
  Obj: TJSONObject;
begin
  while not ADataSet.Eof do
  begin
    Obj := TJSONObject.Create;
    AJSONArray.Add(Obj);
    DataSetToJSONObject(ADataSet, Obj, false);
    ADataSet.Next;
  end;
  if ADataSetInstanceOwner then
    FreeAndNil(ADataSet);
end;

procedure DataSetToStringList(
            ADataSet: TDataSet;
            AList: TStringList;
            Separator: Char;
            ADataSetInstanceOwner: boolean);
var
  sRow: String;
begin
  while not ADataSet.Eof do
  begin
    DataSetToString(ADataSet, sRow, Separator, false);
    AList.Add(sRow);

    ADataSet.Next;
  end;
  if ADataSetInstanceOwner then
    FreeAndNil(ADataSet);
end;

procedure JSONArrayToDataSet(
            AJSONArray: TJSONArray;
            ADataSet: TDataSet;
            AIgnoredFields: TArray<string>;
            AJSONArrayInstanceOwner: boolean); overload;
var
  I: Integer;
begin
  for I := 0 to AJSONArray.Count - 1 do
  begin
    ADataSet.Append;
    JSONObjectToDataSet(TJSONObject(AJSONArray.Items[I]), ADataSet,  AIgnoredFields, false);
    ADataSet.Post;
  end;
  if AJSONArrayInstanceOwner then
    AJSONArray.Free;
end;

procedure JSONArrayToDataSet( AJSONArray: TJSONArray;
                              ADataSet: TDataSet;
                              AJSONArrayInstanceOwner: boolean); overload;
begin
  JSONArrayToDataSet(AJSONArray, ADataSet, TArray<string>.Create(),AJSONArrayInstanceOwner);
end;

function TDataSetHelper.AsJSONObject(AReturnNilIfEOF: boolean = false): TJSONObject;
var
  JObj: TJSONObject;
begin
  JObj := TJSONObject.Create;
  try
    DataSetToJSONObject(Self, JObj, false);
    if AReturnNilIfEOF and (JObj.Count = 0) then
      FreeAndNil(JObj);
    Result := JObj;
  except
    FreeAndNil(JObj);
    raise;
  end;
end;

function TDataSetHelper.AsJSONvalue(AReturnNilIfEOF: boolean = false): TJSONValue;
var
  BookmarkPos: TBookmark;
  HasMultipleRecords: Boolean;
  JObj: TJSONObject;
  JArr: TJSONArray;
  OriginalPosition: Integer;
begin
  Result := nil;

  // Verificar si el dataset está vacío
  if Self.IsEmpty then
  begin
    if AReturnNilIfEOF then
      Result := nil
    else
      Result := TJSONObject.Create; // Retornar objeto vacío
    Exit;
  end;

  // CORRECCIÓN: Verificar múltiples registros sin usar RecordCount
  HasMultipleRecords := False;

  // Guardar posición actual si el dataset soporta bookmarks
  BookmarkPos := nil;
  OriginalPosition := -1;

  if Self.BookmarkValid(Self.Bookmark) then
  begin
    try
      BookmarkPos := Self.Bookmark;
    except
      // Si falla bookmark, usar navegación simple
      BookmarkPos := nil;
    end;
  end;

  // Si no hay bookmark disponible, guardar posición por RecNo si es confiable
  if not Assigned(BookmarkPos) and (Self.RecNo >= 0) then
    OriginalPosition := Self.RecNo;

  try
    // Verificar si hay más de un registro navegando
    if not Self.Eof then
    begin
      Self.Next;
      HasMultipleRecords := not Self.Eof;

      // Restaurar posición
      if Assigned(BookmarkPos) then
      begin
        try
          Self.Bookmark := BookmarkPos;
        except
          // Si falla restaurar bookmark, ir al primero
          Self.First;
        end;
      end
      else if OriginalPosition >= 0 then
      begin
        try
          Self.RecNo := OriginalPosition;
        except
          // Si falla RecNo, ir al primero
          Self.First;
        end;
      end
      else
      begin
        // Sin bookmark ni RecNo confiable, ir al primero
        Self.First;
      end;
    end;

    // Crear el resultado apropiado
    if HasMultipleRecords then
    begin
      // CORRECCIÓN: Usar AsJSONArray que ya maneja la iteración correctamente
      JArr := AsJSONArray(AReturnNilIfEOF);
      Result := JArr; // Transfer ownership
    end
    else
    begin
      // CORRECCIÓN: Usar AsJSONObject que ya maneja el registro actual
      JObj := AsJSONObject(AReturnNilIfEOF);
      Result := JObj; // Transfer ownership
    end;

  except
    on E: Exception do
    begin
      LogMessage(Format('TDataSetHelper.AsJSONvalue: Error processing dataset: %s', [E.Message]), logError);

      // En caso de error, limpiar y retornar objeto vacío o nil
      if Assigned(Result) then
        FreeAndNil(Result);

      if AReturnNilIfEOF then
        Result := nil
      else
        Result := TJSONObject.Create;
    end;
  end;
end;

function TDataSetHelper.AsJSONObjectString(AReturnNilIfEOF: boolean = false): String;
Var JSON: TJSONObject;
begin
  JSON:=AsJSONObject(AReturnNilIfEOF);
  try
    result:=JSON.ToString; // ToJSON;
  finally
    JSON.Free;
  end;
end;

function TDataSetHelper.AsJSONArray(AReturnNilIfEOF: boolean = false): TJSONArray;
var
  JArr: TJSONArray;
  OriginalPos: TBookmark;
  BookmarkValid: Boolean;
begin
  JArr := TJSONArray.Create;

  // CORRECCIÓN: Preservar posición original si es posible
  OriginalPos := nil;
  BookmarkValid := False;

  if Self.BookmarkValid(Self.Bookmark) then
  begin
    try
      OriginalPos := Self.Bookmark;
      BookmarkValid := True;
    except
      BookmarkValid := False;
    end;
  end;

  try
    try
      // Ir al primer registro para procesar todos
      Self.First;

      if not Self.Eof then
        DataSetToJSONArray(Self, JArr, false);

      // Si retorna nil cuando está vacío y se solicita
      if AReturnNilIfEOF and (JArr.Count = 0) then
      begin
        FreeAndNil(JArr);
        Result := nil;
      end
      else
        Result := JArr;

    except
      on E: Exception do
      begin
        FreeAndNil(JArr);
        raise;
      end;
    end;
  finally
    // CORRECCIÓN: Restaurar posición original si es posible
    if BookmarkValid and Assigned(OriginalPos) then
    begin
      try
        Self.Bookmark := OriginalPos;
      except
        // Si falla restaurar, al menos ir al primer registro
        try
          Self.First;
        except
          // Ignorar errores de navegación en finally
        end;
      end;
    end;
  end;
end;

function TDataSetHelper.AsJSONArrayString(AReturnNilIfEOF: boolean = false): string;
var
  Arr: TJSONArray;
begin
  Arr := AsJSONArray;
  try
    Result := Arr.ToString; // ToJSON;
  finally
    Arr.Free;
  end;
end;

function TDataSetHelper.AsStringList(Separator: Char): TStringList;
var
  Arr: TStringList;
begin
  Arr:=TStringList.Create;
  try
    if not Eof then
       DataSetToStringList(Self, Arr,Separator,false);
    Result := Arr;
  except
    FreeAndNil(Arr);
    raise;
  end;
end;

procedure TDataSetHelper.LoadFromJSONArray(AJSONArray: TJSONArray);
begin
  Self.DisableControls;
  try
    JSONArrayToDataSet(AJSONArray, Self, TArray<string>.Create(), false);
  finally
    Self.EnableControls;
  end;
end;

procedure TDataSetHelper.LoadFromJSONArray(
             AJSONArray: TJSONArray;
             AIgnoredFields: TArray<string>);
begin
  Self.DisableControls;
  try
    JSONArrayToDataSet(AJSONArray, Self, AIgnoredFields, false);
  finally
    Self.EnableControls;
  end;
end;

procedure TDataSetHelper.LoadFromJSONArrayString(
             AJSONArrayString: string;
             AIgnoredFields: TArray<string>);
begin
  AppendFromJSONArrayString(AJSONArrayString, AIgnoredFields);
end;

procedure TDataSetHelper.LoadFromJSONArrayString(
             AJSONArrayString: string);
begin
  AppendFromJSONArrayString(AJSONArrayString, TArray<String>.Create());
end;

procedure TDataSetHelper.AppendFromJSONArrayString(
             AJSONArrayString: string;
             AIgnoredFields: TArray<string>);
var
  JV: TJSONArray;
begin
  JV := TJSONArray.Create.ParseJSONValue(
           TEncoding.UTF8.GetBytes(AJSONArrayString),0) as TJSONArray;
  try
    if JV.Count>0 then
      LoadFromJSONArray(JV, AIgnoredFields)
    else
      raise Exception.Create
        ('Expected JSONArray in LoadFromJSONArrayString');
  finally
    JV.Free;
  end;
end;

procedure TDataSetHelper.AppendFromJSONArrayString(AJSONArrayString: string);
begin
  AppendFromJSONArrayString(AJSONArrayString, TArray<string>.Create());
end;


end.