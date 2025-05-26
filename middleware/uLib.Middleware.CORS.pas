unit uLib.Middleware.CORS;

interface

uses
  System.SysUtils, System.Classes, System.JSON,
  System.Generics.Collections, // Para TArray<string>
  IdCustomHTTPServer, System.SyncObjs,
  uLib.Logger; // Para LogMessage

type
  TCORSConfig = record
    AllowedOrigins: TArray<string>;
    AllowedMethods: TArray<string>;
    AllowedHeaders: TArray<string>;
    ExposedHeaders: TArray<string>;
    MaxAgeSeconds: Integer;
    AllowCredentials: Boolean;
  end;

  TCORSMiddleware = class
  private
    FConfig: TCORSConfig;
    FConfigJSONClone: TJSONObject; // Almacena el clon de la configuración JSON pasada
    FLock: TCriticalSection;

    function IsOriginAllowed(const AOriginHeaderValue: string): Boolean;
    function BuildCommaSeparatedString(const AValues: TArray<string>): string;
    procedure LogCORSCheck(const AOrigin: string; const AMethod: string; AIsPreflight: Boolean; AAllowed: Boolean; const AReason: string = '');
    procedure PopulateConfigFromJSON(AConfigSectionToProcess: TJSONObject);

  public
    constructor Create(AConfigSection: TJSONObject); // AConfigSection es la sección "cors" del config.json
    destructor Destroy; override;

    // Aplica las cabeceras CORS. Devuelve True si la solicitud fue completamente manejada (ej. preflight).
    function ApplyCORSPolicy(ARequest: TIdHTTPRequestInfo; AResponse: TIdHTTPResponseInfo): Boolean;

    procedure UpdateConfiguration(ANewConfigSection: TJSONObject);
  end;

implementation

uses
  System.StrUtils, // Para SameText, SplitString (si se usa), Join
  uLib.Utils;      // Para GetStr, GetInt, GetBool (y JSONArrayToStringArray si está allí)

// Helper local si no está en uLib.Base
function JSONArrayToStringArrayLocal(ANode: TJSONArray): TArray<string>;
var
  I: Integer;
begin
  if not Assigned(ANode) then
  begin
    SetLength(Result, 0);
    Exit;
  end;
  SetLength(Result, ANode.Count);
  for I := 0 to ANode.Count - 1 do
  begin
    if Assigned(ANode.Items[I]) then // Verificar que el item no sea nil
      Result[I] := Trim(ANode.Items[I].Value) // Value es string
    else
      Result[I] := '';
  end;
end;

function CreateDefaultCORSConfig: TCORSConfig;
begin
  Result.AllowedOrigins := ['*'];
  Result.AllowedMethods := ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'PATCH'];
  Result.AllowedHeaders := ['Content-Type', 'Authorization', 'X-Requested-With', 'X-CSRF-Token'];
  Result.ExposedHeaders := ['Content-Length', 'X-Request-ID'];
  Result.MaxAgeSeconds := 86400; // 24 horas
  Result.AllowCredentials := False;
end;

constructor TCORSMiddleware.Create(AConfigSection: TJSONObject);
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FConfig := CreateDefaultCORSConfig;

  if Assigned(AConfigSection) then
  begin
    FConfigJSONClone := AConfigSection.Clone as TJSONObject;
    PopulateConfigFromJSON(FConfigJSONClone);
  end
  else
  begin
    FConfigJSONClone := nil;
    LogMessage('TCORSMiddleware.Create: No configuration section provided. Using default CORS settings.', logWarning);
  end;

  LogMessage('TCORSMiddleware created. AllowedOrigins: ' + BuildCommaSeparatedString(FConfig.AllowedOrigins), logInfo);
end;

destructor TCORSMiddleware.Destroy;
begin
  LogMessage('TCORSMiddleware destroying...', logDebug);
  FreeAndNil(FConfigJSONClone);
  FreeAndNil(FLock);
  inherited;
end;

procedure TCORSMiddleware.PopulateConfigFromJSON(AConfigSectionToProcess: TJSONObject);
var
  JsonArrayNode: TJSONArray;
  TempString: string;
begin
  FLock.Acquire;
  try
    // AllowedOrigins: Espera un TJSONArray o un string '*'
    if AConfigSectionToProcess.TryGetValue('allowedOrigins', JsonArrayNode) and Assigned(JsonArrayNode) then
      FConfig.AllowedOrigins := JSONArrayToStringArrayLocal(JsonArrayNode)
    else if AConfigSectionToProcess.TryGetValue('allowedOrigins', TempString) then
    begin
        if TempString = '*' then
        begin
          SetLength(FConfig.AllowedOrigins, 1);
          FConfig.AllowedOrigins[0] := '*';
        end
        else
          FConfig.AllowedOrigins := TempString.Split([',']);
    end;

    if AConfigSectionToProcess.TryGetValue('allowedMethods', JsonArrayNode) and Assigned(JsonArrayNode) then
      FConfig.AllowedMethods := JSONArrayToStringArrayLocal(JsonArrayNode);

    if AConfigSectionToProcess.TryGetValue('allowedHeaders', JsonArrayNode) and Assigned(JsonArrayNode) then
      FConfig.AllowedHeaders := JSONArrayToStringArrayLocal(JsonArrayNode);

    if AConfigSectionToProcess.TryGetValue('exposedHeaders', JsonArrayNode) and Assigned(JsonArrayNode) then
      FConfig.ExposedHeaders := JSONArrayToStringArrayLocal(JsonArrayNode);

    FConfig.MaxAgeSeconds     := TJSONHelper.GetInteger(AConfigSectionToProcess, 'maxAge', FConfig.MaxAgeSeconds);
    FConfig.AllowCredentials := TJSONHelper.GetBoolean(AConfigSectionToProcess, 'allowCredentials', FConfig.AllowCredentials);

    LogMessage('CORS configuration populated from JSON.', logInfo);
  finally
    FLock.Release;
  end;
end;

procedure TCORSMiddleware.UpdateConfiguration(ANewConfigSection: TJSONObject);
begin
  if not Assigned(ANewConfigSection) then
  begin
    LogMessage('TCORSMiddleware.UpdateConfiguration: Provided config section is nil. No update.', logError);
    Exit;
  end;

  FLock.Acquire;
  try
    FreeAndNil(FConfigJSONClone);
    FConfigJSONClone := ANewConfigSection.Clone as TJSONObject;
    PopulateConfigFromJSON(FConfigJSONClone);
    LogMessage('CORS configuration updated successfully.', logInfo);
  finally
    FLock.Release;
  end;
end;

function TCORSMiddleware.IsOriginAllowed(const AOriginHeaderValue: string): Boolean;
var
  AllowedOriginPattern: string;
  TempAllowedOrigins: TArray<string>;
  I: Integer;
begin
  Result := False;
  if AOriginHeaderValue.IsEmpty then Exit;

  FLock.Acquire;
  try
    SetLength(TempAllowedOrigins, Length(FConfig.AllowedOrigins));
    for I := Low(FConfig.AllowedOrigins) to High(FConfig.AllowedOrigins) do
      TempAllowedOrigins[I] := FConfig.AllowedOrigins[I];
  finally
    FLock.Release;
  end;

  if Length(TempAllowedOrigins) = 0 then
  begin
    LogMessage('IsOriginAllowed: No origins configured in CORS policy (empty list). Denying origin: ' + AOriginHeaderValue, logWarning);
    Exit;
  end;

  if (Length(TempAllowedOrigins) = 1) and (TempAllowedOrigins[0] = '*') then
  begin
    Result := True;
    Exit;
  end;

  for AllowedOriginPattern in TempAllowedOrigins do
  begin
    if SameText(Trim(AllowedOriginPattern), AOriginHeaderValue) then
    begin
      Result := True;
      Exit;
    end;
  end;
end;

function TCORSMiddleware.BuildCommaSeparatedString(const AValues: TArray<string>): string;
var
  SB: TStringBuilder;
  Value: string;
  First: Boolean;
begin
  SB := TStringBuilder.Create;
  First := True;
  try
    for Value in AValues do
    begin
      if Value.Trim <> '' then
      begin
        if not First then
          SB.Append(', ');
        SB.Append(Value.Trim);
        First := False;
      end;
    end;
    Result := SB.ToString;
  finally
    SB.Free;
  end;
end;

procedure TCORSMiddleware.LogCORSCheck(const AOrigin: string; const AMethod: string; AIsPreflight: Boolean; AAllowed: Boolean; const AReason: string = '');
var
  LogLevelToUse: TLogLevel;
  StatusMsg: string;
  Details: string;
begin
  StatusMsg := IfThen(AAllowed, 'Allowed', 'Denied');
  if not AAllowed then LogLevelToUse := logWarning else LogLevelToUse := logDebug;

  Details := '';
  if AIsPreflight then Details := ' (Preflight)';
  if AReason <> '' then Details := IfThen(Details<>'', Details + '; Reason: ' + AReason, ' Reason: ' + AReason);

  LogMessage(Format('CORS Check: Origin="%s", Method="%s"%s. Status: %s%s',
    [AOrigin, AMethod, IfThen(AIsPreflight, ' (OPTIONS)', ''), StatusMsg, Details]), LogLevelToUse);
end;

function TCORSMiddleware.ApplyCORSPolicy(ARequest: TIdHTTPRequestInfo; AResponse: TIdHTTPResponseInfo): Boolean;
var
  OriginHeader: string;
  IsPreflight: Boolean;
begin
  Result := False;
  OriginHeader := ARequest.CustomHeaders.Values['Origin'];

  if OriginHeader.IsEmpty then
    Exit;

  IsPreflight := SameText(ARequest.Command, 'OPTIONS') and
                 (ARequest.CustomHeaders.Values['Access-Control-Request-Method'] <> '');

  if not IsOriginAllowed(OriginHeader) then
  begin
    LogCORSCheck(OriginHeader, ARequest.Command, IsPreflight, False, 'Origin not in allowed list');
    Exit;
  end;

  FLock.Acquire;
  try
    if (Length(FConfig.AllowedOrigins) = 1) and (FConfig.AllowedOrigins[0] = '*') then
      AResponse.CustomHeaders.Values['Access-Control-Allow-Origin'] := '*'
    else
      AResponse.CustomHeaders.Values['Access-Control-Allow-Origin'] := OriginHeader;

    if FConfig.AllowCredentials then
      AResponse.CustomHeaders.Values['Access-Control-Allow-Credentials'] := 'true';

    if IsPreflight then
    begin
      AResponse.CustomHeaders.Values['Access-Control-Allow-Methods'] := BuildCommaSeparatedString(FConfig.AllowedMethods);
      AResponse.CustomHeaders.Values['Access-Control-Allow-Headers'] := BuildCommaSeparatedString(FConfig.AllowedHeaders);
      if FConfig.MaxAgeSeconds > 0 then
        AResponse.CustomHeaders.Values['Access-Control-Max-Age'] := FConfig.MaxAgeSeconds.ToString;

      AResponse.ResponseNo := 204;
      AResponse.ContentText := '';
      AResponse.CloseConnection := True; // Explicitly close connection for preflight 204
      Result := True;
    end
    else
    begin
      if Length(FConfig.ExposedHeaders) > 0 then
        AResponse.CustomHeaders.Values['Access-Control-Expose-Headers'] := BuildCommaSeparatedString(FConfig.ExposedHeaders);
    end;
  finally
    FLock.Release;
  end;

  LogCORSCheck(OriginHeader, ARequest.Command, IsPreflight, True);
end;

end.

