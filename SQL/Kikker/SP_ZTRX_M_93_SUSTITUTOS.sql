USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_ZTRX_M_93_SUSTITUTOS]    Script Date: 19/06/2025 16:00:32 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[SP_ZTRX_M_93_SUSTITUTOS]
AS
BEGIN
    SET NOCOUNT ON;

    -- Eliminar la tabla M_93_SUSTITUTOS si ya existe
    IF OBJECT_ID('dbo.M_93_SUSTITUTOS', 'U') IS NOT NULL
    BEGIN
        DROP TABLE dbo.M_93_SUSTITUTOS;
    END

    -- Crear la tabla M_93_SUSTITUTOS nuevamente
    CREATE TABLE M_93_SUSTITUTOS (
        COD_PRD VARCHAR(10),
        COD_PROD_SUSTITUTO VARCHAR(10),
		F_DATO DATETIME,  -- Fecha y hora de vigencia del dato
		F_PROC DATETIME  -- Fecha y hora de procesamiento
    );

    -- Insertar los datos en la tabla M_93_SUSTITUTOS con la fecha de procesamiento
    INSERT INTO M_93_SUSTITUTOS (COD_PRD, COD_PROD_SUSTITUTO,F_DATO, F_PROC)
    SELECT 
        CAST(C_ARTICULO AS VARCHAR(10)) AS COD_PRD,
        CAST(C_ARTICULO_SUSTITUTO AS VARCHAR(10)) AS COD_PROD_SUSTITUTO,
		GETDATE() AS F_DATO, -- Fecha y hora actual en formato DATETIME
		GETDATE() AS F_PROC -- Fecha y hora actual en formato DATETIME
    FROM 
        [DIARCOP001].[DiarcoP].dbo.T050_ARTICULOS_SUSTITUTOS WITH (NOLOCK);

END;
GO


