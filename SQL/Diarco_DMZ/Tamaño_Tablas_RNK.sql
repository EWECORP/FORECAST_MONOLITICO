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