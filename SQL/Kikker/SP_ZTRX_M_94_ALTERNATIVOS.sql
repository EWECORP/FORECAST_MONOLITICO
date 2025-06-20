USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_ZTRX_M_94_ALTERNATIVOS]    Script Date: 19/06/2025 16:00:50 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[SP_ZTRX_M_94_ALTERNATIVOS]
AS
BEGIN
    SET NOCOUNT ON;

    -- Eliminar la tabla M_94_ALTERNATIVOS si ya existe
    IF OBJECT_ID('dbo.M_94_ALTERNATIVOS', 'U') IS NOT NULL
    BEGIN
        DROP TABLE dbo.M_94_ALTERNATIVOS;
    END

    -- Crear la tabla M_94_ALTERNATIVOS nuevamente
    CREATE TABLE M_94_ALTERNATIVOS (
        COD_PRD VARCHAR(10),
        COD_PROD_ALTERNATIVO VARCHAR(10),
		F_DATO DATETIME,  -- Fecha y hora de vigencia del dato
		F_PROC DATETIME  -- Fecha y hora de procesamiento
    );

    -- Insertar los datos en la tabla M_94_ALTERNATIVOS con la fecha de procesamiento
    INSERT INTO M_94_ALTERNATIVOS (COD_PRD, COD_PROD_ALTERNATIVO, F_DATO, F_PROC)
    SELECT 
        CAST(C_ARTICULO AS VARCHAR(10)) AS COD_PRD,
        CAST(C_ARTICULO_ALTERNATIVO AS VARCHAR(10)) AS COD_PROD_ALTERNATIVO,
		GETDATE() AS F_DATO, -- Fecha y hora actual en formato DATETIME
		GETDATE() AS F_PROC -- Fecha y hora actual en formato DATETIME
    FROM 
        [DIARCOP001].[DiarcoP].dbo.T050_ARTICULOS WITH (NOLOCK)
    WHERE 
        M_BAJA = 'N' -- Filtrar solo productos que no están dados de baja
        AND C_ARTICULO_ALTERNATIVO <> 0; -- Excluir artículos sin alternativos

END;
GO


