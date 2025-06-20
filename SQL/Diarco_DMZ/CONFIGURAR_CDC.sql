SELECT SERVERPROPERTY('Edition'), SERVERPROPERTY('ProductVersion');

SELECT name, is_cdc_enabled FROM sys.databases WHERE name = 'data-sync';


USE [data-sync];
EXEC sys.sp_cdc_enable_db;

	

SELECT @@SERVERNAME AS nombre_actual;

EXEC sp_dropserver 'DCO-DIARCOCI-T0';

EXEC sp_addserver 'DCO-DIARCOCI-T0', 'local';

SELECT @@SERVERNAME AS nombre_real;

EXEC sp_dropserver 'DCO-DIARCOCI-T0';
EXEC sp_addserver 'DCO-DIARCOCI-T0', 'local';

EXEC sp_helpserver;

USE [data-sync];

EXEC sys.sp_cdc_enable_table
    @source_schema = N'repl',
    @source_name   = N'T050_ARTICULOS',
    @role_name     = NULL,
    @supports_net_changes = 0;


SELECT SERVERPROPERTY('MachineName') AS HostFísico,
       SERVERPROPERTY('ServerName')   AS NombreInstancia,
       SERVERPROPERTY('InstanceName') AS NombreLógico;
