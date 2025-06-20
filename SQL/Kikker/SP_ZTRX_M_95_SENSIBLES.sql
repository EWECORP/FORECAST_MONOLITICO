USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_ZTRX_M_95_SENSIBLES]    Script Date: 19/06/2025 16:01:08 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[SP_ZTRX_M_95_SENSIBLES]
AS
BEGIN
    SET NOCOUNT ON;

    -- Eliminar la tabla M_95_SENSIBLES si ya existe
    IF OBJECT_ID('dbo.M_95_SENSIBLES', 'U') IS NOT NULL
    BEGIN
        DROP TABLE dbo.M_95_SENSIBLES;
    END

    -- Crear la tabla M_95_SENSIBLES nuevamente
    CREATE TABLE M_95_SENSIBLES (
        COD_PRD VARCHAR(10),
		F_DATO DATETIME,  -- Fecha y hora de vigencia del dato
		F_PROC DATETIME  -- Fecha y hora de procesamiento
    );

    -- Insertar los datos en la tabla M_95_SENSIBLES con la fecha de procesamiento
    INSERT INTO M_95_SENSIBLES (COD_PRD, F_DATO, F_PROC)
    SELECT 
        CAST(C_ARTICULO AS VARCHAR(10)) AS COD_PRD,
		GETDATE() AS F_DATO, -- Fecha y hora actual en formato DATETIME
		GETDATE() AS F_PROC -- Fecha y hora actual en formato DATETIME
    FROM 
        [DIARCOP001].[DiarcoP].dbo.T050_ARTICULOS WITH (NOLOCK)
    WHERE 
        C_CLASIFICACION_COMPRA IN (1, 6) -- Filtrar artículos sensibles
        AND M_BAJA = 'N'; -- Filtrar solo productos que no están dados de baja
END;
GO


