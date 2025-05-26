unit uGlobalDefinitions;

interface

uses
   System.Types,
   System.JSON,
   System.UITypes,
   System.Classes
;

const
   API_NAME = '/api';
   API_VERSION = '/v1';
   API_BASE = API_NAME + API_VERSION;

   AppVerMayor   = 1;
   AppVerMenor   = 0;
   AppVerRelease = 0;
   //---------------------------------------------------------------

   FILE_LOGS = 'logs.txt';
   FILE_DATABASE = 'storage.sdb';
   FILE_SETTINGS = 'settings.cnf';
   FILE_REPOSITORY = 'repository.conf';

   FOLDER_LOGS = 'logs';
   FOLDER_DATA = 'data';
   FOLDER_SETTINGS = 'settings';

   KEY_SETTINGS = 'zkXtZtHvaBlg3nUQ'; //----> 16 Digitos!
   IV = #2#123#23#87#9#120#26#89;     // 8 Digitos = Length(Key_Settings) / 2 ;
   {$IFDEF DEBUG}
    FILE_CRYPTED = False;
   {$ELSE}
    FILE_CRYPTED = true;
   {$ENDIF}
   //---------------------------------------------------------------
   //   DEVELOPER KEY API SAINTNET.COM
   //---------------------------------------------------------------
   MAIN_API_KEY = 'E7064C10-A32B-493B-8810-5D4C0C512873';
   DEFAULT_APP_ID = 3;
   URL_SAINTNET_API = 'https://api.esaint.net/';
   //URL_SAINTNET_LICENSE = 'https://esaint.net/slm/api/license/';
   //---------------------------------------------------------------
   HEADER_KEY_NAME = 'x-saintnet-key';
   HEADER_APP_ID   = 'x-saintnet-id';

   APP_COMPANY = 'Saintnet Latinoamerica, Inc.';
   APP_HOST = 'https://saintnet.com';
   //---------------------------------------------------------------
   //   API SAINTNET.COM
   //---------------------------------------------------------------

   JWT_SECRET_PASSWORD = 'KbZtHn9xw75duJLjgRBrAPemETQ8CGV4';   // 32 Digits/ JWT Token generation!

implementation

Uses
  System.SysUtils;

initialization

end.
