unit uLib.Server.Types;

interface

uses
  System.SysUtils, System.Classes, System.JSON, System.Rtti, System.DateUtils;

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
  // EConnectionError = class(EServerException); // Podría colisionar con EDBConnectionError.
                                               // Usar nombres más específicos si es necesario,
                                               // o cualificar completamente.

function CreateDefaultServerHTTPConfig: TServerHTTPConfig; // Renombrado

implementation

uses
   uLib.Utils;

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

{ TServerNotification }

function TServerNotification.ToJSON: TJSONObject;
begin
 Result := TJSONObject.Create;
 Result.AddPair('type', TRttiEnumerationType.GetName<TServerNotificationType>(NotifyType));
 Result.AddPair('message', Message);
 Result.AddPair('time_utc', DateToISO8601(TimeUTC));

 if Assigned(Data) then
   Result.AddPair('data', Data.Clone as TJSONObject) // Clonar para evitar problemas de propiedad
 else
   Result.AddPair('data', TJSONNull.Create); // Indicar explícitamente que no hay datos
end;

// Función helper para crear una configuración de servidor HTTP con valores por defecto
function CreateDefaultServerHTTPConfig: TServerHTTPConfig; // Renombrado
begin
  Result.Port := 8088;
  Result.MaxConnections := 0; // 0 para ilimitado en TIdTCPServer
  Result.ThreadPoolSize := 0; // 0 para default de Indy (TIdSchedulerThreadDefault - un thread por conexión)
  Result.ConnectionTimeout := 120000; // Indy: TIdTCPServer.TerminateWaitTime (ms) o IOHandler.ReadTimeout
  Result.KeepAliveEnabled := True;  // Indy: TIdHTTPServer.KeepAlive
  // Result.RequestTimeout := 30000; // No hay un equivalente directo simple en Indy para timeout de solicitud global
  Result.SSLEnabled := False;
  Result.SSLCertFile := 'cert.pem';
  Result.SSLKeyFile := 'key.pem';
  Result.SSLRootCertFile := '';
  Result.ServerName := 'Delphi HTTP Server/1.0';
  Result.BasePath := ''; // El que lo usa debe establecerlo (ej. TServerManager o TLinuxWebServer desde config global)
  Result.PIDFile := 'server.pid';
  Result.Daemonize := False;
  Result.ShutdownGracePeriodSeconds := 30;
end;

end.

