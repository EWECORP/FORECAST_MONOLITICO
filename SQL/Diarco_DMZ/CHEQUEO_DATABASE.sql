SELECT name, size, max_size, growth FROM sys.master_files WHERE database_id = DB_ID('tempdb');
USE tempdb;
EXEC sp_helpfile;
--- PROCESOS BÄSICOS
DBCC FREEPROCCACHE; -- Elimina el caché de los planes de ejecución
DBCC DROPCLEANBUFFERS; -- Libera los buffers del caché de datos
DBCC SHRINKDATABASE (tempdb, 10); -- Reducir el tamaño de la base de datos

--- REDUCIR FICHEROS en DISCO
DBCC SHRINKFILE (temp2, 1024);  -- Reducir el archivo temp2 a 1MB
DBCC SHRINKFILE (temp3, 1024);  -- Reducir el archivo temp2 a 1MB
DBCC SHRINKFILE (temp4, 1024);  -- Reducir el archivo temp2 a 1MB


EXEC sp_who2;   --- Monitorear los Procesos que estan corriendo

--- Identificar Proceso Problemático
SELECT blocking_session_id, wait_type, wait_time, wait_resource
FROM sys.dm_exec_requests
WHERE blocking_session_id <> 0;

--- Identificar 2
SELECT session_id, blocking_session_id, wait_type, wait_time, wait_resource, status, command, sql_handle
FROM sys.dm_exec_requests
WHERE session_id = 56;
-- Identificar 3
SELECT text 
FROM sys.dm_exec_requests AS r
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle)
WHERE r.session_id = 56;
-- Identificar 4
SELECT blocking_session_id, session_id, wait_type, wait_time, wait_resource
FROM sys.dm_exec_requests
WHERE blocking_session_id <> 0;

kill 63

--- TAMAÑO DE LAS TABLAS
USE [data-sync]
GO
SELECT 
    t.name AS Tabla, 
    SUM(a.total_pages) * 8 AS Tamaño_KB,
    SUM(a.total_pages) * 8 / 1024 AS Tamaño_MB
FROM sys.tables AS t
JOIN sys.indexes AS i ON t.object_id = i.object_id
JOIN sys.partitions AS p ON i.object_id = p.object_id AND i.index_id = p.index_id
JOIN sys.allocation_units AS a ON p.partition_id = a.container_id
GROUP BY t.name
ORDER BY Tamaño_MB DESC;

--TRUNCATE TABLE [repl].[T702_EST_VTAS_POR_ARTICULO_STG]
--TRUNCATE TABLE [repl].[T710_ESTADIS_OFERTA_FOLDER_STG]
--TRUNCATE TABLE 


