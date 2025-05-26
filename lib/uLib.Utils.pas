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

implementation

uses
    System.DateUtils
   ,System.RTTI
   ,System.NetEncoding
   ,WinApi.Windows
   ,Soap.EncdDecd
   ,Data.SqlTimSt
   ,Data.FmtBcd
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
      Pair := Pairs[I].Trim; // <--- Usar la variable local y hacer Trim aquí
      if Pair.IsEmpty then
        Continue;
      SeparatorPos := Pos(AValueSeparator, Pair);
      if SeparatorPos > 0 then // Asegurar que el separador exista y no esté al inicio
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

    if I = High(PathParts) then // Última parte del path, hemos encontrado el valor
    begin
      AValue := CurrentValue;
      Result := True;
      Exit;
    end
    else // No es la última parte, esperamos un TJSONObject para seguir navegando
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
  if Assigned(ANode) and GetValueRecursive(ANode, APath, LJsonValue) and Assigned(LJsonValue) then
  begin
    Result := ANode.GetValue<T>(APath,ADefault);
  end
  else
    Result := ADefault; // Path no encontrado o FConfigData es nil
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
    else if not (LJsonValue is TJSONNull) then // Permitir que números o booleanos se conviertan a string
      Result := LJsonValue.Value
    // else (es TJSONNull o no se encontró), Result queda como ADefault
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
      if not TryStrToInt(LStrValue, Result) then // Si la conversión falla, Result mantiene ADefault
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
       Result := Trunc(TJSONNumber(LJsonValue).AsDouble)// O AsInt, AsInt64 si se sabe el rango
    else if LJsonValue is TJSONString then // Intentar convertir desde string
    begin
      LStrValue := TJSONString(LJsonValue).Value;
      if not TryStrToInt64(LStrValue, Result) then // Si la conversión falla, Result mantiene ADefault
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
       Result := Trunc(TJSONNumber(LJsonValue).AsDouble)// O AsInt, AsInt64 si se sabe el rango
    else if LJsonValue is TJSONString then // Intentar convertir desde string
    begin
      LStrValue := TJSONString(LJsonValue).Value;
      if not TryStrToFloat(LStrValue, Result) then // Si la conversión falla, Result mantiene ADefault
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
    else if LJsonValue is TJSONString then // Intentar convertir desde string "true" o "false"
    begin
      LStrValue := TJSONString(LJsonValue).Value.ToLower;
      if LStrValue = 'true' then Result := True
      else if LStrValue = 'false' then Result := False;
      // else Result mantiene ADefault
    end
    else if LJsonValue is TJSONNumber then // Considerar 0 como false, no-cero como true
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

class function TJSONHelper.GetString(const ANode: string; const APath: string; const ADefault: string=''): string;
var
  AJSON: TJSONObject;
begin
  AJSON:=TJSONObject.ParseJSONValue(TEncoding.ANSI.GetBytes(ANode),0) as TJSONObject;
  If AJSON<>Nil then
     begin
       Result:=AJSON.GetValue<String>(APath,ADefault);
     end;
  FreeAndNil(AJSON);
end;

class function TJSONHelper.GetInteger(const ANode: string; const APath: string; const ADefault: Integer=0): Integer;
var
  AJSON: TJSONObject;
begin
  AJSON:=TJSONObject.ParseJSONValue(TEncoding.ANSI.GetBytes(ANode),0) as TJSONObject;
  If AJSON<>Nil then
     begin
       Result:=AJSON.GetValue<Integer>(APath,ADefault);
     end;
  FreeAndNil(AJSON);
end;

class function TJSONHelper.GetInt64(const ANode: string; const APath: string; const ADefault: Int64=0): Int64;
var
  AJSON: TJSONObject;
begin
  AJSON:=TJSONObject.ParseJSONValue(TEncoding.ANSI.GetBytes(ANode),0) as TJSONObject;
  If AJSON<>Nil then
     begin
       Result:=AJSON.GetValue<Int64>(APath,ADefault);
     end;
  FreeAndNil(AJSON);
end;

class function TJSONHelper.GetDouble(const ANode: string; const APath: string; const ADefault: Double=0): Double;
var
  AJSON: TJSONObject;
begin
  AJSON:=TJSONObject.ParseJSONValue(TEncoding.ANSI.GetBytes(ANode),0) as TJSONObject;
  If AJSON<>Nil then
     begin
       Result:=AJSON.GetValue<Double>(APath,ADefault);
     end;
  FreeAndNil(AJSON);
end;

class function TJSONHelper.GetBoolean(const ANode: string; const APath: string; const ADefault: Boolean=false): Boolean;
var
  AJSON: TJSONObject;
begin
  AJSON:=TJSONObject.ParseJSONValue(TEncoding.ANSI.GetBytes(ANode),0) as TJSONObject;
  If AJSON<>Nil then
     begin
       Result:=AJSON.GetValue<Boolean>(APath,ADefault);
     end;
  FreeAndNil(AJSON);
end;

class function TJSONHelper.GetJSONArray(const ANode: string; const APath: string): TJSONArray;
var
  AJSON: TJSONArray;
begin
  AJSON:=TJSONArray.ParseJSONValue(TEncoding.ANSI.GetBytes(ANode),0) as TJSONArray;
  If AJSON<>Nil then
     begin
       Result:=AJSON.GetValue<TJSONArray>(APath);
     end;
  FreeAndNil(AJSON);
end;

class function TJSONHelper.GetJSONObject(const ANode: string; const APath: string): TJSONObject;
var
  AJSON: TJSONObject;
begin
  AJSON:=TJSONObject.ParseJSONValue(TEncoding.ANSI.GetBytes(ANode),0) as TJSONObject;
  If AJSON<>Nil then
     begin
       Result:=AJSON.GetValue<TJSONObject>(APath);
     end;
  FreeAndNil(AJSON);
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
            SS := TStringStream.Create('', TEncoding.ANSI);
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
            SS := TStringStream.Create('', TEncoding.ANSI);
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
              TEncoding.ANSI);
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
  JObj: TJSONObject;
  JArr: TJSONArray;
begin
  if Self.RecordCount>1 then
     begin
       JArr := TJSONArray.Create;
       try
         if not Eof then
            DataSetToJSONArray(Self, JArr, false);
         Result := JArr.AsType<TJSONValue>;
       except
         FreeAndNil(JArr);
         raise;
       end;
     end
  else
     begin
       JObj := TJSONObject.Create;
       try
         DataSetToJSONObject(Self, JObj, false);
         if AReturnNilIfEOF and (JObj.Count = 0) then
            FreeAndNil(JObj);
         Result := JObj.AsType<TJSONValue>;
       except
         FreeAndNil(JObj);
         raise;
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
begin
  JArr := TJSONArray.Create;
  try
    if not Eof then
       DataSetToJSONArray(Self, JArr, false);
    Result := JArr;
  except
    FreeAndNil(JArr);
    raise;
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
           TEncoding.ANSI.GetBytes(AJSONArrayString),0) as TJSONArray;
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