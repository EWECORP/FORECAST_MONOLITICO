 SELECT 
    TABLE_NAME AS Tabla,
    COLUMN_NAME AS Campo,
	'Descripción' AS Descripcion,
    DATA_TYPE AS 'Tipo de Dato',
    CHARACTER_MAXIMUM_LENGTH AS Longitud,
    IS_NULLABLE AS 'Permite Nulos'
FROM 
    [DIARCOP001].[DiarcoP].INFORMATION_SCHEMA.COLUMNS
WHERE 
	TABLE_NAME LIKE 'T[0-9]%' 
    AND TABLE_SCHEMA = 'dbo'  -- Ajusta según el esquema que necesites
GO

-------- BASE DE DATOS KIKKER

 SELECT 
    TABLE_NAME AS Tabla,
    COLUMN_NAME AS Campo,
	'Descripción' AS Descripcion,
    DATA_TYPE AS 'Tipo de Dato',
    CHARACTER_MAXIMUM_LENGTH AS Longitud,
    IS_NULLABLE AS 'Permite Nulos'
FROM 
    INFORMATION_SCHEMA.COLUMNS
WHERE 
	TABLE_NAME LIKE 'M%' 
    AND TABLE_SCHEMA = 'dbo'  -- Ajusta según el esquema que necesites
GO
