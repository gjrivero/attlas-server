unit uLib.Routes;

interface

uses
  System.Classes, System.SysUtils,
  System.Generics.Collections, System.SyncObjs,
  System.RegularExpressions, IdCustomHTTPServer,
  uLib.Logger;


Const
   URL_API_VERSION = '/api/v1/';

type
  TRouteParamType = (rptString, rptInteger, rptFloat, rptBoolean, rptUUID);

  TRouteParam = record
    Name: string;
    ParamType: TRouteParamType;
  end;

  TRouteHandler = reference to procedure(
    Request: TIdHTTPRequestInfo;
    Response: TIdHTTPResponseInfo;
    RouteParams: TDictionary<string, string>
  );

  TRoute = record
    Method: string;
    PathDefinition: string;
    PatternRegex: string;
    ParamsDef: TArray<TRouteParam>;
    Handler: TRouteHandler;
    RequiresAuth: Boolean;
    CacheEnabled: Boolean;
    RateLimit: Integer;
  end;

  TRouteManager = class
  private
    FRoutes: TList<TRoute>;
    FLock: TCriticalSection;

    function BuildRouteRegexAndParams(const APathDefinition: string;
      out AParamsDef: TArray<TRouteParam>): string;
    function MatchPathAndExtractParams(const APatternRegex, ARequestPath: string;
      const ARouteParamsDef: TArray<TRouteParam>;
      var AExtractedValues: TDictionary<string, string>): Boolean;
    function ValidateExtractedRouteParams(const AExtractedValues: TDictionary<string, string>;
      const ARouteParamsDef: TArray<TRouteParam>): Boolean;
    function TryParseParamValue(const AValue: string; AParamType: TRouteParamType; out AConvertedValue: Variant): Boolean;
    procedure LogRouteEvent(const AMethod, APath: string; ASuccess: Boolean; const AReason: string = '');

  public
    constructor Create;
    destructor Destroy; override;

    procedure AddRoute(const AMethod, APathDefinition: string; AHandler: TRouteHandler;
      ARequiresAuth: Boolean = True; ACacheEnabled: Boolean = False;
      ARateLimit: Integer = 0);

    // Nueva función para encontrar información de la ruta sin ejecutarla
    function FindRouteInfo(const AMethod, APath: string; out ARouteRec: TRoute): Boolean;

    function HandleRoute(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo): Boolean;

    procedure ClearRoutes;
    function GetRouteCount: Integer;
    function DumpRoutes: string;
  end;

implementation

uses
   System.Variants, // Para TryStrToFloat, TryStrToInt, Variant
   System.Rtti,     // Para TRttiEnumerationType
   System.StrUtils, // Para IfThen, UpperCase, SameText, Copy
   System.Math;     // Para Max, etc.

constructor TRouteManager.Create;
begin
  inherited Create;
  FRoutes := TList<TRoute>.Create;
  FLock := TCriticalSection.Create;
  LogMessage('TRouteManager created.', logInfo);
end;

destructor TRouteManager.Destroy;
begin
  FLock.Acquire;
  try
    FreeAndNil(FRoutes);
  finally
    FLock.Release;
  end;
  FreeAndNil(FLock);
  LogMessage('TRouteManager destroyed.', logInfo);
  inherited;
end;

function TRouteManager.BuildRouteRegexAndParams(const APathDefinition: string;
  out AParamsDef: TArray<TRouteParam>): string;
var
  LParamList: TList<TRouteParam>;
  LMatches: TMatchCollection;
  LMatch: TMatch;
  LParamName, LParamTypeStr: string;
  LRouteParam: TRouteParam;
  LRegexBuilder: TStringBuilder;
  LCurrentPos: Integer;
begin
  LRegexBuilder := TStringBuilder.Create;
  LParamList := TList<TRouteParam>.Create;
  LCurrentPos := 1;

  try
    LMatches := TRegEx.Matches(APathDefinition, ':(\w+)(?:\(([^)]+)\))?');

    LRegexBuilder.Append('^');

    for LMatch in LMatches do
    begin
      if LMatch.Index > LCurrentPos then
        LRegexBuilder.Append(TRegEx.Escape(APathDefinition.Substring(LCurrentPos -1, LMatch.Index - LCurrentPos)));

      LParamName := LMatch.Groups[1].Value;
      LParamTypeStr := '';
      if LMatch.Groups[2].Success then
        LParamTypeStr := LMatch.Groups[2].Value.ToLower;

      LRouteParam.Name := LParamName;

      if LParamTypeStr = 'int' then LRouteParam.ParamType := rptInteger
      else if LParamTypeStr = 'float' then LRouteParam.ParamType := rptFloat
      else if LParamTypeStr = 'bool' then LRouteParam.ParamType := rptBoolean
      else if LParamTypeStr = 'uuid' then LRouteParam.ParamType := rptUUID
      else LRouteParam.ParamType := rptString;

      LParamList.Add(LRouteParam);
      LRegexBuilder.Append('(?<').Append(LParamName).Append('>[^/]+)');
      LCurrentPos := LMatch.Index + LMatch.Length;
    end;

    if LCurrentPos <= APathDefinition.Length then
      LRegexBuilder.Append(TRegEx.Escape(APathDefinition.Substring(LCurrentPos -1, APathDefinition.Length - LCurrentPos + 1)));

    LRegexBuilder.Append('$');
    Result := LRegexBuilder.ToString;
    AParamsDef := LParamList.ToArray;
  finally
    LParamList.Free;
    LRegexBuilder.Free;
  end;
  LogMessage(Format('Built regex "%s" for path "%s" with %d params defined.', [Result, APathDefinition, Length(AParamsDef)]), logDebug);
end;

function TRouteManager.MatchPathAndExtractParams(
            const APatternRegex,
                  ARequestPath: string;
            const ARouteParamsDef: TArray<TRouteParam>;
            var AExtractedValues: TDictionary<string, string>): Boolean;
var
  LMatch: TMatch;
  LRouteParamDef: TRouteParam;
begin
  Result := False;
  AExtractedValues.Clear; // Limpiar el diccionario proporcionado por el llamador

  LMatch := TRegEx.Match(ARequestPath, APatternRegex);

  if LMatch.Success then
  begin
    Result := True; // Asumir éxito hasta que un parámetro falle
    for LRouteParamDef in ARouteParamsDef do
    begin
      if LMatch.Groups[LRouteParamDef.Name].Success then
      begin
        AExtractedValues.Add(LRouteParamDef.Name, LMatch.Groups[LRouteParamDef.Name].Value);
      end
      else
      begin
        LogMessage(Format('MatchPath: Regex matched, but named group "%s" failed for pattern "%s", path "%s".',
          [LRouteParamDef.Name, APatternRegex, ARequestPath]), logError);
        Result := False; // Falló la extracción de un parámetro
        AExtractedValues.Clear; // Limpiar lo que se haya añadido
        Break;
      end;
    end;
  end;

  if not Result then // Si en algún punto falló
    AExtractedValues.Clear; // Asegurar que esté limpio al retornar False
end;

function TRouteManager.TryParseParamValue(const AValue: string; AParamType: TRouteParamType; out AConvertedValue: Variant): Boolean;
var
  LIntValue: Integer;
  LExtValue: Extended;
  //LGuid: TGuid; // No usado en esta versión, pero rptUUID existe
begin
  AConvertedValue := Null;
  Result := False;
  case AParamType of
    rptString: begin Result := True; AConvertedValue := AValue; end;
    rptInteger: begin Result := TryStrToInt(AValue, LIntValue); if Result then AConvertedValue := LIntValue; end;
    rptFloat: begin Result := TryStrToFloat(AValue, LExtValue); if Result then AConvertedValue := LExtValue; end;
    rptBoolean:
      begin
        if SameText(AValue, 'true') or (AValue = '1') then begin Result := True; AConvertedValue := True; end
        else if SameText(AValue, 'false') or (AValue = '0') then begin Result := True; AConvertedValue := False; end
        else Result := False;
      end;
    rptUUID: // Validación de UUID puede ser más compleja, esto es un placeholder
      begin
        // Result := TryStringToGUID(AValue, LGuid); // Si se usa System.SysUtils.TryStringToGUID
        // Por ahora, si es rptUUID, se trata como string y se asume que el formato es correcto.
        // Una validación regex para UUID podría añadirse aquí.
        Result := True; AConvertedValue := AValue; // Tratar como string por ahora
        if Result then LogMessage(Format('UUID parameter "%s" passed as string. Consider specific validation.', [AValue]), logSpam);
      end;
  else
    Result := False;
  end;
end;

function TRouteManager.ValidateExtractedRouteParams(const AExtractedValues: TDictionary<string, string>;
  const ARouteParamsDef: TArray<TRouteParam>): Boolean;
var
  LRouteParamDef: TRouteParam;
  LParamValueStr: string;
  LConvertedValue: Variant;
begin
  Result := True;
  if not Assigned(AExtractedValues) then Exit(False); // No se pueden validar si no hay valores

  for LRouteParamDef in ARouteParamsDef do
  begin
    if AExtractedValues.TryGetValue(LRouteParamDef.Name, LParamValueStr) then
    begin
      if not TryParseParamValue(LParamValueStr, LRouteParamDef.ParamType, LConvertedValue) then
      begin
        LogRouteEvent('', '', False, Format('Route parameter validation failed for "%s": value "%s" is not a valid %s.',
          [LRouteParamDef.Name, LParamValueStr, TRttiEnumerationType.GetName<TRouteParamType>(LRouteParamDef.ParamType)]));
        Result := False;
        Break;
      end;
    end
    else
    begin
      LogRouteEvent('', '', False, Format('Internal Error: Defined route parameter "%s" not found in extracted values after match.', [LRouteParamDef.Name]));
      Result := False;
      Break;
    end;
  end;
end;

procedure TRouteManager.AddRoute(const AMethod, APathDefinition: string; AHandler: TRouteHandler;
  ARequiresAuth: Boolean = True; ACacheEnabled: Boolean = False; ARateLimit: Integer = 0);
var
  LRoute: TRoute;
  APathRoute: String;
begin
  if not Assigned(AHandler) then
  begin
    LogMessage(Format('Cannot add route "%s %s": Handler is nil.', [AMethod, APathDefinition]), logError);
    Exit;
  end;

  FLock.Acquire;
  try
    APathRoute:=URL_API_VERSION;
    if APathDefinition.StartsWith('/') then
       APathRoute:=APathRoute+Copy(APathDefinition,2,Length(APathDefinition))
    else
       APathRoute:=APathRoute+APathDefinition;
    LRoute.Method := UpperCase(AMethod);
    LRoute.PathDefinition := APathRoute;
    LRoute.PatternRegex := BuildRouteRegexAndParams(APathRoute, LRoute.ParamsDef);
    LRoute.Handler := AHandler;
    LRoute.RequiresAuth := ARequiresAuth;
    LRoute.CacheEnabled := ACacheEnabled;
    LRoute.RateLimit := ARateLimit;
    FRoutes.Add(LRoute);
  finally
    FLock.Release;
  end;
  LogMessage(Format('Route added: %s %s. AuthRequired: %s. Regex: %s',
    [LRoute.Method, LRoute.PathDefinition, BoolToStr(LRoute.RequiresAuth, True), LRoute.PatternRegex]), logInfo);
end;

// Nueva función para encontrar información de la ruta sin ejecutarla
function TRouteManager.FindRouteInfo(const AMethod, APath: string; out ARouteRec: TRoute): Boolean;
var
  LList: TDictionary<string, string>;
  LCurrentRoute: TRoute;
  LUpperMethod: string;
begin
  Result := False;
  FillChar(ARouteRec, SizeOf(TRoute), 0); // Inicializar el registro de salida

  LUpperMethod := UpperCase(AMethod);

  FLock.Acquire;
  try
    for LCurrentRoute in FRoutes do
    begin
      if (LCurrentRoute.Method = LUpperMethod) then
      begin
        // Para FindRouteInfo, no necesitamos extraer los parámetros, solo verificar si el patrón coincide.
        // MatchPathAndExtractParams puede ser llamado con AExtractedValues = nil.

        if MatchPathAndExtractParams( LCurrentRoute.PatternRegex,
                                      APath,
                                      LCurrentRoute.ParamsDef, LList ) then
        begin
          ARouteRec := LCurrentRoute; // Copiar la información de la ruta encontrada
          Result := True;
          LogMessage(Format('FindRouteInfo: Route matched: %s %s. AuthRequired: %s',
            [LCurrentRoute.Method, LCurrentRoute.PathDefinition, BoolToStr(LCurrentRoute.RequiresAuth, True)]), logDebug);
          Exit; // Salir al encontrar la primera coincidencia
        end;
      end;
    end;
  finally
    FLock.Release;
  end;
  // Si llega aquí, no se encontró la ruta
  LogMessage(Format('FindRouteInfo: No route matched for %s %s.', [LUpperMethod, APath]), logSpam);
end;

function TRouteManager.HandleRoute(Request: TIdHTTPRequestInfo; Response: TIdHTTPResponseInfo): Boolean;
var
  LExtractedRouteParams: TDictionary<string, string>;
  LRoute: TRoute;
  LRequestPath: string;
  LRouteFound: Boolean;
begin
  Result := False; // Indica si una ruta fue encontrada y su handler invocado (o un error 400/404 fue manejado)
  LRequestPath := Request.Document;

  LExtractedRouteParams := TDictionary<string, string>.Create;
  try
    LRouteFound := False;
    FLock.Acquire;
    try
      for LRoute in FRoutes do
      begin
        if (LRoute.Method = UpperCase(Request.Command)) then
        begin
          // Intentar hacer coincidir la ruta y extraer los parámetros
          if MatchPathAndExtractParams(LRoute.PatternRegex, LRequestPath, LRoute.ParamsDef, LExtractedRouteParams) then
          begin
            LRouteFound := True; // Marcador de que la ruta coincidió
            // Validar los tipos de los parámetros extraídos
            if not ValidateExtractedRouteParams(LExtractedRouteParams, LRoute.ParamsDef) then
            begin
              Response.ResponseNo := 400; // Bad Request
              Response.ContentType := 'application/json';
              Response.ContentText := '{"success":false, "message":"Invalid route parameter format."}';
              LogRouteEvent(Request.Command, LRequestPath, False, 'Invalid route parameter format');
              FLock.Release;
              Result := True; // Ruta "manejada" (con error de validación de parámetro)
              Exit;
            end;

            LogMessage(Format('Route matched: %s %s. Handler: %p. Extracted Route Params Count: %d',
              [LRoute.Method, LRoute.PathDefinition, Pointer(LRoute.Handler), LExtractedRouteParams.Count]), logDebug);

            LRoute.Handler(Request, Response, LExtractedRouteParams); // Invocar el handler del controlador

            LogRouteEvent(Request.Command, LRequestPath, True);
            FLock.Release;
            Result := True; // Ruta manejada exitosamente por un handler
            Exit;
          end;
        end;
      end;
    finally
      FLock.Release;
    end;

    // Si llegamos aquí, ninguna ruta definida coincidió con el método y el patrón de path.
    if not LRouteFound then // (Result será False aquí si no se encontró y ejecutó un handler)
    begin
      Response.ResponseNo := 404; // Not Found
      Response.ContentType := 'application/json';
      Response.ContentText := '{"success":false, "message":"Endpoint not found."}';
      LogRouteEvent(Request.Command, LRequestPath, False, 'Endpoint not found');
      Result := True; // Consideramos 404 como "manejado" por el router (no encontró ruta aplicable)
    end;

  finally
    LExtractedRouteParams.Free;
  end;
end;

procedure TRouteManager.LogRouteEvent(const AMethod, APath: string; ASuccess: Boolean; const AReason: string = '');
var
  Loglevel: TLogLevel;
  sMsg: string;
begin
  if ASuccess then
  begin
    Loglevel := logInfo;
    sMsg := 'matched and handled';
  end
  else
  begin
    Loglevel := logWarning;
    sMsg := IfThen(AReason <> '', 'failed: ' + AReason, 'not matched or validation failed');
  end;
  LogMessage(Format('Route Event: %s %s - %s', [AMethod, APath, sMsg]), Loglevel);
end;

procedure TRouteManager.ClearRoutes;
begin
  FLock.Acquire;
  try
    FRoutes.Clear;
  finally
    FLock.Release;
  end;
  LogMessage('All routes cleared.', logInfo);
end;

function TRouteManager.GetRouteCount: Integer;
begin
  FLock.Acquire;
  try
    Result := FRoutes.Count;
  finally
    FLock.Release;
  end;
end;

function TRouteManager.DumpRoutes: string;
var
  SB: TStringBuilder;
  LRoute: TRoute;
  LParamDef: TRouteParam;
begin
  SB := TStringBuilder.Create;
  FLock.Acquire;
  try
    SB.AppendLine(Format('Registered Routes (%d):', [FRoutes.Count]));
    for LRoute in FRoutes do
    begin
      SB.Append('  - ').Append(LRoute.Method).Append(' ').AppendLine(LRoute.PathDefinition);
      SB.Append('    Regex: ').AppendLine(LRoute.PatternRegex);
      SB.Append('    Auth: ').Append(BoolToStr(LRoute.RequiresAuth, True));
      SB.Append(', Cache: ').Append(BoolToStr(LRoute.CacheEnabled, True));
      if LRoute.RateLimit > 0 then
        SB.Append(', RateLimit: ').Append(LRoute.RateLimit.ToString);
      SB.AppendLine;
      if Length(LRoute.ParamsDef) > 0 then
      begin
        SB.Append('    ParamsDef: ');
        for LParamDef in LRoute.ParamsDef do
          SB.Append(LParamDef.Name).Append(' (').Append(TRttiEnumerationType.GetName<TRouteParamType>(LParamDef.ParamType)).Append('), ');

        // CORRECCIÓN: Usar Remove en lugar de manipular Length directamente
        if SB.Length > 2 then
          SB.Remove(SB.Length - 2, 2); // Remove trailing comma and space
        SB.AppendLine;
      end;
    end;
    Result := SB.ToString;
  finally
    FLock.Release;
    SB.Free;
  end;
end;

end.

