unit uLib.Server.Types;

interface

uses
  System.SysUtils, System.Classes, System.JSON,
  System.Rtti, System.DateUtils;

type
  // Estados del servidor
  TServerState = (
    ssInitializing,
    ssStarting,
    ssRunning,
    ssStopping,
    ssStopped,
    ssError,
    ssUnknown // Añadido para un estado indefinido
  );

  // Configuración básica del motor HTTP (subconjunto de la configuración general)
  TServerHTTPConfig = record // Renombrado de TServerConfig para evitar confusión con config global
    Port: Integer;
    MaxConnections: Integer;    // Para TIdHTTPServer.MaxConnections
    ThreadPoolSize: Integer;    // Para TIdSchedulerOfThreadPool.PoolSize
    ConnectionTimeout: Integer; // En milisegundos, para TIdTCPServer.TerminateWaitTime o similar
    KeepAliveEnabled: Boolean;  // Para TIdHTTPServer.KeepAlive
    // RequestTimeout: Integer; // Indy no tiene un timeout de solicitud global simple; se maneja por ReadTimeout en IOHandler o lógicas de app
    SSLEnabled: Boolean;
    SSLCertFile: string;
    SSLKeyFile: string;
    SSLRootCertFile: string; // Opcional
    ServerName: string; // Para la cabecera 'Server'
    BasePath: string; // Ruta base para archivos del servidor (ej. PID file en Linux)
    PIDFile: string; // Nombre del archivo PID (ej. server.pid)
    Daemonize: Boolean; // Solo para Linux
    ShutdownGracePeriodSeconds: Integer; // Tiempo para esperar conexiones activas al apagar
  end;

  // Estadísticas del servidor
  TServerStats = record
    State: TServerState;
    StartupTimeUTC: TDateTime; // Renombrado y especificado UTC
    ActiveConnections: Integer;
    TotalRequests: Int64;
    FailedRequests: Integer;
    BytesSent: Int64;         // Podría ser difícil de obtener de forma precisa con Indy genéricamente
    BytesReceived: Int64;     // Ídem
    AverageResponseTimeMs: Double; // En milisegundos

    function ToJSON: TJSONObject;
  end;

 // Información de conexión activa
 TConnectionInfo = record
   ID: string; // Ej. Handle del Contexto de Indy o un ID generado
   RemoteIP: string;
   RemotePort: Integer;
   Protocol: string; // Ej. HTTP/1.1
   ConnectTimeUTC: TDateTime; // Renombrado y especificado UTC
   LastActivityUTC: TDateTime;
   // BytesSentToClient: Int64; // Difícil de rastrear por conexión en Indy sin IOHandler personalizado
   // BytesReceivedFromClient: Int64; // Ídem

   function ToJSON: TJSONObject;
 end;

 // Tipos de notificación del servidor
 TServerNotificationType = (
   sntInfo,
   sntWarning,
   sntError,
   sntCritical,
   sntUserLogin,
   sntUserLogout,
   sntDataUpdate // Ejemplo
 );

 // Notificación del servidor
 TServerNotification = record
   NotifyType: TServerNotificationType;
   Message: string;
   TimeUTC: TDateTime;
   Data: TJSONObject;  // Payload JSON opcional

   function ToJSON: TJSONObject;
 end;

  // Excepciones específicas del Servidor
  EServerException = class(Exception);
  EConfigurationError = class(EServerException); // Definición única y centralizada aquí
  EServerStartError = class(EServerException);
  EConnectionError = class(EServerException); // Podría colisionar con EDBConnectionError.
                                               // Usar nombres más específicos si es necesario,
                                               // o cualificar completamente.

function CreateDefaultServerHTTPConfig: TServerHTTPConfig; // Renombrado
function GetServerMonitoringConfig(out CheckIntervalMs, MaxFileDescriptors: Integer): Boolean;

implementation

uses
   uLib.Logger,
   uLib.Utils,
   uLib.Config.Manager;

const
  DEFAULT_HTTP_PORT = 8088;
  DEFAULT_CONNECTION_TIMEOUT_MS = 120000;
  DEFAULT_SHUTDOWN_GRACE_PERIOD_SEC = 30;
  DEFAULT_SERVER_NAME = 'Delphi HTTP Server/1.0';
  DEFAULT_PID_FILE = 'server.pid';
  DEFAULT_SSL_CERT_FILE = 'cert.pem';
  DEFAULT_SSL_KEY_FILE = 'key.pem';


function CreateDefaultServerHTTPConfig: TServerHTTPConfig;
var
  ConfigMgr: TConfigManager;
  ServerConfig: TJSONObject;
begin
  // Valores por defecto hardcoded como fallback
  Result.Port := DEFAULT_HTTP_PORT;
  Result.MaxConnections := 0;
  Result.ThreadPoolSize := 0;
  Result.ConnectionTimeout := DEFAULT_CONNECTION_TIMEOUT_MS;
  Result.KeepAliveEnabled := True;
  Result.SSLEnabled := False;
  Result.SSLCertFile := DEFAULT_SSL_CERT_FILE;
  Result.SSLKeyFile := DEFAULT_SSL_KEY_FILE;
  Result.SSLRootCertFile := '';
  Result.ServerName := DEFAULT_SERVER_NAME;
  Result.BasePath := '';
  Result.PIDFile := DEFAULT_PID_FILE;
  Result.Daemonize := False;
  Result.ShutdownGracePeriodSeconds := DEFAULT_SHUTDOWN_GRACE_PERIOD_SEC;

  // Intentar obtener valores de configuración usando TJSONHelper
  try
    ConfigMgr := TConfigManager.GetInstance;
    if Assigned(ConfigMgr) and Assigned(ConfigMgr.ConfigData) then
    begin
      // Configuración básica del servidor usando paths con punto
      Result.Port := TJSONHelper.GetInteger(ConfigMgr.ConfigData, 'server.port', Result.Port);
      Result.ThreadPoolSize := TJSONHelper.GetInteger(ConfigMgr.ConfigData, 'server.threadPoolSize', Result.ThreadPoolSize);
      Result.KeepAliveEnabled := TJSONHelper.GetBoolean(ConfigMgr.ConfigData, 'server.keepAlive', Result.KeepAliveEnabled);
      Result.ServerName := TJSONHelper.GetString(ConfigMgr.ConfigData, 'server.serverName', Result.ServerName);
      Result.BasePath := TJSONHelper.GetString(ConfigMgr.ConfigData, 'server.basePath', Result.BasePath);
      Result.PIDFile := TJSONHelper.GetString(ConfigMgr.ConfigData, 'server.pidFile', Result.PIDFile);
      Result.Daemonize := TJSONHelper.GetBoolean(ConfigMgr.ConfigData, 'server.daemonize', Result.Daemonize);
      Result.ShutdownGracePeriodSeconds := TJSONHelper.GetInteger(ConfigMgr.ConfigData, 'server.shutdownGracePeriodSeconds', Result.ShutdownGracePeriodSeconds);

      // Configuración de timeouts
      Result.ConnectionTimeout := TJSONHelper.GetInteger(ConfigMgr.ConfigData, 'server.timeouts.connectionTimeoutMs', Result.ConnectionTimeout);

      // SSL Configuration
      Result.SSLEnabled := TJSONHelper.GetBoolean(ConfigMgr.ConfigData, 'server.ssl.enabled', Result.SSLEnabled);
      Result.SSLCertFile := TJSONHelper.GetString(ConfigMgr.ConfigData, 'server.ssl.certFile', Result.SSLCertFile);
      Result.SSLKeyFile := TJSONHelper.GetString(ConfigMgr.ConfigData, 'server.ssl.keyFile', Result.SSLKeyFile);
      Result.SSLRootCertFile := TJSONHelper.GetString(ConfigMgr.ConfigData, 'server.ssl.rootCertFile', Result.SSLRootCertFile);
    end;
  except
    on E: Exception do
    begin
      // Si falla, usar defaults hardcoded y log warning
      try
        LogMessage('Warning: Could not load server configuration from config file, using defaults: ' + E.Message, logWarning);
      except
        // Si el logger tampoco está disponible, silenciar
      end;
    end;
  end;
end;

{ TServerStats }

function TServerStats.ToJSON: TJSONObject;
begin
 Result := TJSONObject.Create;
 Result.AddPair('state', TRttiEnumerationType.GetName<TServerState>(State));
 Result.AddPair('startup_time_utc', DateToISO8601(StartupTimeUTC));
 Result.AddPair('active_connections', ActiveConnections); // TJSONNumber no es necesario para enteros simples
 Result.AddPair('total_requests', TotalRequests);
 Result.AddPair('failed_requests', FailedRequests);
 Result.AddPair('bytes_sent', BytesSent);
 Result.AddPair('bytes_received', BytesReceived);
 Result.AddPair('avg_response_time_ms', AverageResponseTimeMs); // TJSONNumber no es necesario para flotantes simples
end;

{ TConnectionInfo }

function TConnectionInfo.ToJSON: TJSONObject;
begin
 Result := TJSONObject.Create;
 Result.AddPair('id', ID);
 Result.AddPair('remote_ip', RemoteIP);
 Result.AddPair('remote_port', RemotePort);
 Result.AddPair('protocol', Protocol);
 Result.AddPair('connect_time_utc', DateToISO8601(ConnectTimeUTC));
 Result.AddPair('last_activity_utc', DateToISO8601(LastActivityUTC));
 // Result.AddPair('bytes_sent_to_client', BytesSentToClient);
 // Result.AddPair('bytes_received_from_client', BytesReceivedFromClient);
end;

function TServerNotification.ToJSON: TJSONObject;
begin
 Result := TJSONObject.Create;
 try
   Result.AddPair('type', TRttiEnumerationType.GetName<TServerNotificationType>(NotifyType));
   Result.AddPair('message', Message);
   Result.AddPair('time_utc', DateToISO8601(TimeUTC));

   if Assigned(Data) then
   begin
     try
       Result.AddPair('data', Data.Clone as TJSONObject);
     except
       on E: Exception do
       begin
         LogMessage(Format('Error cloning notification data: %s', [E.Message]), logWarning);
         Result.AddPair('data', TJSONNull.Create);
       end;
     end;
   end
   else
     Result.AddPair('data', TJSONNull.Create);
 except
   on E: Exception do
   begin
     FreeAndNil(Result);
     raise;
   end;
 end;
end;

{ TServerNotification }

// Agregar al final de la implementation section:
function GetServerMonitoringConfig(out CheckIntervalMs, MaxFileDescriptors: Integer): Boolean;
var
  ConfigMgr: TConfigManager;
begin
  // Defaults
  CheckIntervalMs := 30000;
  MaxFileDescriptors := 256;
  Result := False;

  try
    ConfigMgr := TConfigManager.GetInstance;
    if Assigned(ConfigMgr) and Assigned(ConfigMgr.ConfigData) then
    begin
      CheckIntervalMs := TJSONHelper.GetInteger(ConfigMgr.ConfigData, 'server.monitoring.checkIntervalMs', CheckIntervalMs);
      MaxFileDescriptors := TJSONHelper.GetInteger(ConfigMgr.ConfigData, 'server.monitoring.maxFileDescriptors', MaxFileDescriptors);
      Result := True;
    end;
  except
    // Usar defaults si falla
  end;
end;


end.

