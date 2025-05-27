unit uLib.UrlParser;

interface

uses
  System.SysUtils, System.Classes, System.JSON, System.RegularExpressions,
  System.Generics.Collections, System.NetEncoding, System.Net.URLClient;

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
    FQuery: string;
    FFragment: string;
    FQueryParams: TStringList;

    procedure Clear;
    procedure DoParseUrl; // Este método cambiará drásticamente
    procedure ParseQueryStringToParams(const AQueryStr: string); // Ahora tomará el string de TURI
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
  Clear; // Limpia todos los campos F...

  if AUrl.Trim.IsEmpty then
  begin
    LogMessage('TURLParser.Parse: Input URL is empty.', logDebug);
    Exit;
  end;

  FUrl := AUrl; // Almacenar la URL original
  try
    DoParseUrl; // Llamar al método de parseo interno que ahora usa TURI
    Result := True;
    LogMessage(Format('TURLParser.Parse: Successfully parsed URL using TURI: "%s"', [AUrl]), logSpam);
  except
    on E: Exception do // Capturar excepciones específicas de TURI
    begin
      LogMessage(Format('TURLParser.Parse: Error parsing URL "%s" with TURI: %s - %s', [AUrl, E.ClassName, E.Message]), logError);
      Clear;
      Result := False;
    end;
    on E: Exception do // Otras excepciones
    begin
      LogMessage(Format('TURLParser.Parse: General error parsing URL "%s": %s - %s', [AUrl, E.ClassName, E.Message]), logError);
      Clear;
      Result := False;
    end;
  end;
end;

procedure TURLParser.DoParseUrl;
var
  LURI: TURI;
begin
  // FUrl ya contiene la URL a parsear
  LURI := TURI.Create(FUrl); // TURI parsea la URL en su constructor
  try
    FScheme   := LURI.Scheme;
    // TURI.Username y TURI.Password ya están decodificados (percent-decoded)
    FUsername := LURI.Username;
    FPassword := LURI.Password;
    FHost     := LURI.Host;
    FPort     := LURI.Port; // Es Integer; 0 si no especificado o es el default para el scheme.
    FPath     := LURI.Path; // Incluye el '/' inicial si hay autoridad.
    FQuery    := LURI.Query;    // Query string cruda, sin el '?'
    FFragment := LURI.Fragment; // Fragmento sin el '#'

    if not FQuery.IsEmpty then
      ParseQueryStringToParams(FQuery);
  finally
    //LURI.Free;
  end;
end;

procedure TURLParser.ParseQueryStringToParams(const AQueryStr: string);
var
  Pairs: TArray<string>;
  Parts: TArray<string>;
  Key, Value: string;
  PairStr: string;
begin
 FQueryParams.Clear;
 if AQueryStr.Trim = '' then Exit;

 Pairs := AQueryStr.Split(['&']);
 for PairStr in Pairs do
 begin
   if PairStr.Trim = '' then Continue;

   Parts := PairStr.Split(['='], 2); // Dividir por '=' como máximo en 2 partes
   if Length(Parts) > 0 then
   begin
     // TNetEncoding.URL.Decode ya no es necesario aquí si TURI.Query ya está decodificado,
     // o si los valores individuales de query params se esperan codificados y TURI no los decodifica.
     // TURI.Query devuelve la cadena tal cual. La decodificación debe hacerse por parámetro.
     Key := TNetEncoding.URL.Decode(Trim(Parts[0]));
     if Key = '' then Continue;

     if Length(Parts) > 1 then
       Value := TNetEncoding.URL.Decode(Parts[1]) // Decodificar el valor
     else
       Value := ''; // Parámetro sin valor (ej. ?flag)

     FQueryParams.AddPair(Key, Value);
   end;
 end;
end;

function TURLParser.BuildUrl: string;
var
  Builder: TStringBuilder;
  I: Integer;
  UserInfoProvided, AuthorityProvided: Boolean;
begin
  Builder := TStringBuilder.Create;
  try
    // Esquema
    if FScheme.Trim <> '' then
      Builder.Append(FScheme).Append(':');

    // Autoridad (UserInfo, Host, Puerto)
    // TURI reconstruye esto de forma más fiable. Aquí intentamos replicar.
    AuthorityProvided := (FHost.Trim <> '') or (FUsername.Trim <> ''); // Simplificación, TURI es más preciso
    if AuthorityProvided or ((FScheme.Trim <> '') and (FScheme = 'file')) then // file puede no tener '//'
      Builder.Append('//');

    // Usuario y contraseña
    UserInfoProvided := FUsername.Trim <> '';
    if UserInfoProvided then
    begin
      Builder.Append(TNetEncoding.URL.Encode(FUsername));
      if FPassword.Trim <> '' then
        Builder.Append(':').Append(TNetEncoding.URL.Encode(FPassword));
      Builder.Append('@');
    end;

    // Host
    if FHost.Trim <> '' then
    begin
      // Manejar IPv6 literal
      if FHost.Contains(':') and not (FHost.StartsWith('[') and FHost.EndsWith(']')) then
        Builder.Append('[').Append(FHost).Append(']') // TURI.Host ya lo devuelve así
      else
        Builder.Append(FHost);
    end;

    // Puerto
    if FPort > 0 then
    begin
      var DefaultPortForScheme := 0;
      if SameText(FScheme, 'http') then DefaultPortForScheme := 80
      else if SameText(FScheme, 'https') then DefaultPortForScheme := 443
      else if SameText(FScheme, 'ftp') then DefaultPortForScheme := 21;
      // Añadir otros esquemas comunes si es necesario

      if (DefaultPortForScheme = 0) or (FPort <> DefaultPortForScheme) then
        Builder.Append(':').Append(FPort.ToString);
    end;

    // Path
    // FPath de TURI ya incluye el '/' inicial si la URL base tenía autoridad.
    // Si FPath está vacío pero hay autoridad, se necesita un '/'.
    if AuthorityProvided and (FPath.Trim = '') then
        Builder.Append('/')
    else if FPath.Trim <> '' then // TURI.Path puede ser vacío
    begin
        // Si hay autoridad y el path no empieza con '/', añadirlo (TURI.Path debería ser correcto)
        if AuthorityProvided and (not FPath.StartsWith('/')) then
             Builder.Append('/');
        Builder.Append(FPath); // Usar FPath tal cual lo da TURI.
    end;
    // Si no hay autoridad y no hay path, no añadir nada para el path.

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

    // Fragmento
    if FFragment.Trim <> '' then
      Builder.Append('#').Append(FFragment); // Fragmento no se codifica

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
   // No incluir password en JSON por seguridad, a menos que sea explícitamente necesario.
   Result.AddPair('host', FHost);
   if FPort > 0 then // TURI.Port es 0 si no está o es el default del scheme.
     Result.AddPair('port', FPort);
   Result.AddPair('path', FPath);
   Result.AddPair('query_string', FQuery); // Query string cruda de TURI

   QueryObj := TJSONObject.Create;
   for I := 0 to FQueryParams.Count - 1 do
     QueryObj.AddPair(FQueryParams.Names[I], FQueryParams.ValueFromIndex[I]);
   Result.AddPair('query_params', QueryObj);

   if FFragment.Trim <> '' then
     Result.AddPair('fragment', FFragment);
 except
   on E: Exception do // Asegurar que Result se libere si hay error creando el JSON
   begin
     FreeAndNil(Result);
     raise;
   end;
 end;
end;

end.

