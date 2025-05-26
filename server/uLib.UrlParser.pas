unit uLib.UrlParser;

interface

uses
  System.SysUtils, System.Classes, System.JSON, System.RegularExpressions,
  System.Generics.Collections, System.NetEncoding;

type
  // Los tipos TQueryOperator, TLogicalOperator, TQueryCondition han sido eliminados
  // ya que no se utilizan en esta unidad y su funcionalidad (si se relaciona con
  // la construcción de WHERE clauses) se manejaría en uLib.SQLQueryBuilder.pas.

  TURLParser = class
  private
    FUrl: string;
    FScheme: string;
    FUsername: string;
    FPassword: string;
    FHost: string;
    FPort: Integer;
    FPath: string;
    FQuery: string; // La cadena de consulta completa (ej. "name=test&type=1")
    FFragment: string;
    FQueryParams: TStringList; // Pares clave-valor de la cadena de consulta

    procedure Clear;
    procedure DoParseUrl; // Renombrado de ParseUrl para evitar posible colisión con método Parse
    procedure ParseQueryStringToParams; // Renombrado de ParseQueryString

    // Métodos de extracción usando Regex
    function ExtractSchemePart: string; // Renombrado
    function ExtractUserInfoPart: string; // Renombrado
    function ExtractHostPortPart: string; // Renombrado
    function ExtractPathPart: string; // Renombrado
    function ExtractQueryPart: string; // Renombrado
    function ExtractFragmentPart: string; // Renombrado

  public
    constructor Create;
    destructor Destroy; override;

    function Parse(const AUrl: string): Boolean; // Método principal para parsear una URL
    function BuildUrl: string;                   // Reconstruye la URL desde las partes
    function ToJSONObject: TJSONObject;          // Devuelve las partes como un objeto JSON

    property Url: string read FUrl;
    property Scheme: string read FScheme write FScheme;
    property Username: string read FUsername write FUsername;
    property Password: string read FPassword write FPassword;
    property Host: string read FHost write FHost;
    property Port: Integer read FPort write FPort;
    property Path: string read FPath write FPath;
    property QueryString: string read FQuery write FQuery; // Renombrado de Query
    property Fragment: string read FFragment write FFragment;
    property QueryParams: TStringList read FQueryParams; // Acceso a los parámetros de consulta parseados
  end;

implementation

uses
  uLib.Logger; // Para LogMessage

constructor TURLParser.Create;
begin
 inherited Create;
 FQueryParams := TStringList.Create;
 FQueryParams.NameValueSeparator := '='; // Aunque ParseQueryStringToParams lo hace manualmente
 FQueryParams.StrictDelimiter := True; // Para manejar correctamente múltiples '&'
 Clear;
 LogMessage('TURLParser instance created.', logDebug);
end;

destructor TURLParser.Destroy;
begin
 LogMessage('TURLParser instance destroying.', logDebug);
 FreeAndNil(FQueryParams);
 inherited;
end;

procedure TURLParser.Clear;
begin
 FUrl := '';
 FScheme := '';
 FUsername := '';
 FPassword := '';
 FHost := '';
 FPort := 0; // 0 indica que no se especificó o es el puerto por defecto del esquema
 FPath := '';
 FQuery := '';
 FFragment := '';
 FQueryParams.Clear;
end;

function TURLParser.Parse(const AUrl: string): Boolean;
begin
 Result := False;
 Clear;

 if AUrl.Trim.IsEmpty then
 begin
   LogMessage('TURLParser.Parse: Input URL is empty.', logDebug);
   Exit;
 end;

 try
   FUrl := AUrl;
   DoParseUrl; // Llama al método de parseo interno
   Result := True;
   LogMessage(Format('TURLParser.Parse: Successfully parsed URL: "%s"', [AUrl]), logSpam);
 except
   on E: Exception do
   begin
     LogMessage(Format('TURLParser.Parse: Error parsing URL "%s": %s - %s', [AUrl, E.ClassName, E.Message]), logError);
     Clear; // Asegurar que el estado esté limpio después de un error
     Result := False;
   end;
 end;
end;

procedure TURLParser.DoParseUrl;
var
 UserInfoPart, HostPortPart: string;
 UserInfoParts: TArray<string>;
 HostPortParts: TArray<string>;
begin
  // Extraer todas las partes principales usando regex
  FScheme       := ExtractSchemePart;
  UserInfoPart  := ExtractUserInfoPart;
  HostPortPart  := ExtractHostPortPart;
  FPath         := ExtractPathPart;
  FQuery        := ExtractQueryPart;    // Esto es solo el string después de '?'
  FFragment     := ExtractFragmentPart;

  // Parsear la información de usuario (username:password)
  if not UserInfoPart.IsEmpty then
  begin
    // UserInfoPart es 'username:password' o 'username'
    UserInfoParts := UserInfoPart.Split([':']);
    if Length(UserInfoParts) > 0 then
      FUsername := TNetEncoding.URL.Decode(UserInfoParts[0]);
    if Length(UserInfoParts) > 1 then
      FPassword := TNetEncoding.URL.Decode(UserInfoParts[1]);
  end;

  // Parsear host y puerto
  if not HostPortPart.IsEmpty then
  begin
    // HostPortPart es 'host' o 'host:port'
    // Regex para separar host de puerto, considerando IPv6 [::1]:80
    var Match := TRegEx.Match(HostPortPart, '^(\[[^\]]+\]|[^:]+)(?::(\d+))?$');
    if Match.Success then
    begin
      FHost := Match.Groups[1].Value;
      if Match.Groups[2].Success then // Si el grupo del puerto existe
        FPort := StrToIntDef(Match.Groups[2].Value, 0)
      else // No hay puerto explícito
        FPort := 0; // Se usará el default del esquema luego si es necesario
    end
    else // Fallback simple si la regex no coincide (ej. solo host)
    begin
       HostPortParts := HostPortPart.Split([':']);
       FHost := HostPortParts[0];
       if Length(HostPortParts) > 1 then
         FPort := StrToIntDef(HostPortParts[1], 0);
    end;
  end;

  // Parsear la cadena de consulta en FQueryParams
  if not FQuery.IsEmpty then
    ParseQueryStringToParams;
end;

procedure TURLParser.ParseQueryStringToParams;
var
  Pairs: TArray<string>;
  Parts: TArray<string>;
  Key, Value: string;
  PairStr: string;
begin
 FQueryParams.Clear;
 if FQuery.Trim = '' then Exit;

 Pairs := FQuery.Split(['&']); // Dividir por '&'
 for PairStr in Pairs do
 begin
   if PairStr.Trim = '' then Continue;

   Parts := PairStr.Split(['='], 2); // Dividir por '=' como máximo en 2 partes (para valores con '=')
   if Length(Parts) > 0 then
   begin
     Key := TNetEncoding.URL.Decode(Parts[0].Trim);
     if Key = '' then Continue; // Ignorar claves vacías

     if Length(Parts) > 1 then
       Value := TNetEncoding.URL.Decode(Parts[1]) // No hacer Trim al valor, puede ser intencional
     else
       Value := ''; // Parámetro sin valor (ej. ?flag)

     FQueryParams.AddPair(Key, Value); // TStringList.AddPair maneja duplicados (concatena o reemplaza según Delimiter/Duplicates)
                                      // Para URLs, múltiples valores para la misma clave son posibles y a veces significativos.
                                      // TStringList.Add podría ser mejor si se quieren preservar todos.
                                      // Por ahora, AddPair (que usa Values[Key] :=) sobrescribirá.
                                      // Si se necesita manejar arrays de query params (ej. ?id=1&id=2), se necesitaría TList<TPair<string,string>>.
                                      // Para la mayoría de los usos de API REST, el último valor para una clave suele ser el que se toma.
   end;
 end;
end;

function TURLParser.ExtractSchemePart: string;
var
 Match: TMatch;
begin
 Result := '';
 // Regex: ^ (inicio de string), ([^:/?#]+) (grupo 1: uno o más caracteres que no sean :, /, ?, #), seguido de :
 Match := TRegEx.Match(FUrl, '^([^:/?#]+):');
 if Match.Success then
   Result := Match.Groups[1].Value.ToLower; // Esquemas son case-insensitive
end;

function TURLParser.ExtractUserInfoPart: string;
var
 Match: TMatch;
begin
 Result := '';
 // Regex: :// (después del esquema), ([^/?#@]*@) (grupo 1: cualquier caracter excepto /, ?, #, @, cero o más veces, seguido de @)
 // El @ final se elimina después.
 Match := TRegEx.Match(FUrl, '://([^/?#@]*@)');
 if Match.Success then
   Result := Match.Groups[1].Value.Substring(0, Match.Groups[1].Value.Length - 1); // Quitar el @ final
end;

function TURLParser.ExtractHostPortPart: string;
var
 Match: TMatch;
begin
 Result := '';
 // Regex: ://, opcionalmente userinfo@, ([^/?#]*) (grupo 1: cualquier caracter excepto /, ?, #, cero o más veces)
 // Esto captura el host y el puerto juntos.
 Match := TRegEx.Match(FUrl, '://(?:[^/?#@]*@)?([^/?#]*)');
 if Match.Success then
   Result := Match.Groups[1].Value;
end;

function TURLParser.ExtractPathPart: string;
var
 Match: TMatch;
begin
 Result := '';
 // Regex: ://, opcionalmente userinfo@, opcionalmente hostport, ([^?#]*) (grupo 1: cualquier caracter excepto ?, #, cero o más veces)
 // Esto captura la ruta.
 Match := TRegEx.Match(FUrl, '://(?:[^/?#@]*@)?(?:[^/?#]*)([^?#]*)'); // Ajustado para ser no-capturador para hostport
 if Match.Success then
   Result := Match.Groups[1].Value;
end;

function TURLParser.ExtractQueryPart: string;
var
 Match: TMatch;
begin
 Result := '';
 // Regex: \? (literal ?), ([^#]*) (grupo 1: cualquier caracter excepto #, cero o más veces)
 Match := TRegEx.Match(FUrl, '\?([^#]*)');
 if Match.Success then
   Result := Match.Groups[1].Value;
end;

function TURLParser.ExtractFragmentPart: string;
var
 Match: TMatch;
begin
 Result := '';
 // Regex: # (literal #), (.*) (grupo 1: cualquier caracter, cero o más veces, hasta el final)
 Match := TRegEx.Match(FUrl, '#(.*)$');
 if Match.Success then
   Result := Match.Groups[1].Value;
end;

function TURLParser.BuildUrl: string;
var
 Builder: TStringBuilder;
 I: Integer;
begin
 Builder := TStringBuilder.Create;
 try
   // Esquema
   if FScheme.Trim <> '' then
     Builder.Append(FScheme).Append('://');

   // Usuario y contraseña
   if FUsername.Trim <> '' then
   begin
     Builder.Append(TNetEncoding.URL.Encode(FUsername));
     if FPassword.Trim <> '' then // Solo añadir ':' si hay contraseña
       Builder.Append(':').Append(TNetEncoding.URL.Encode(FPassword));
     Builder.Append('@');
   end;

   // Host y puerto
   if FHost.Trim <> '' then
     Builder.Append(FHost);

   if FPort > 0 then // Solo añadir puerto si es explícito y no es el default (que sería 0 aquí)
   begin
     // No añadir puerto si es el default para el esquema (ej. 80 para http, 443 para https)
     // Esta lógica puede ser compleja, por ahora, si FPort > 0, se añade.
     // if not ((SameText(FScheme, 'http') and (FPort = 80)) or (SameText(FScheme, 'https') and (FPort = 443))) then
     Builder.Append(':').Append(FPort.ToString);
   end;

   // Path
   // Asegurarse de que el path comience con '/' si hay autoridad (host) y el path no está vacío
   if (FHost.Trim <> '') and (FPath.Trim <> '') and (not FPath.StartsWith('/')) then
     Builder.Append('/')
   else if (FHost.Trim = '') and (FScheme.Trim <> '') and (FPath.Trim <> '') and (not FPath.StartsWith('/')) and (FScheme <> 'file') then // ej. mailto:user@example.com
      Builder.Append('/'); // Algunos esquemas sin autoridad pueden tener un path que no empieza con /

   Builder.Append(FPath); // FPath ya debería tener el '/' inicial si es necesario por la extracción

   // Query params
   if FQueryParams.Count > 0 then
   begin
     Builder.Append('?');
     for I := 0 to FQueryParams.Count - 1 do
     begin
       if I > 0 then
         Builder.Append('&');
       Builder.Append(TNetEncoding.URL.Encode(FQueryParams.Names[I]))
              .Append('=')
              .Append(TNetEncoding.URL.Encode(FQueryParams.ValueFromIndex[I]));
     end;
   end;

   // Fragment
   if FFragment.Trim <> '' then
     Builder.Append('#').Append(FFragment); // El fragmento no se codifica con URL encoding

   Result := Builder.ToString;
 finally
   Builder.Free;
 end;
end;

function TURLParser.ToJSONObject: TJSONObject;
var
  QueryObj: TJSONObject;
  I: Integer;
begin
 Result := TJSONObject.Create;
 try
   Result.AddPair('url_original', FUrl);
   Result.AddPair('scheme', FScheme);
   Result.AddPair('username', FUsername);
   // No incluir password en JSON por seguridad, a menos que sea explícitamente necesario y seguro.
   // Result.AddPair('password', FPassword);
   Result.AddPair('host', FHost);
   if FPort > 0 then // Solo añadir si se especificó
     Result.AddPair('port', FPort);
   Result.AddPair('path', FPath);
   Result.AddPair('query_string', FQuery); // La cadena de consulta original

   QueryObj := TJSONObject.Create;
   for I := 0 to FQueryParams.Count - 1 do
     QueryObj.AddPair(FQueryParams.Names[I], FQueryParams.ValueFromIndex[I]);
   Result.AddPair('query_params', QueryObj); // QueryObj es ahora propiedad de Result

   if FFragment.Trim <> '' then
     Result.AddPair('fragment', FFragment);

 except
   FreeAndNil(Result); // Liberar si ocurre un error durante la creación del JSON
   raise;
 end;
end;

end.

