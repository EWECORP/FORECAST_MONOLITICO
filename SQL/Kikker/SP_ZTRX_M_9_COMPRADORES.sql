USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_ZTRX_M_9_COMPRADORES]    Script Date: 19/06/2025 15:59:21 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [dbo].[SP_ZTRX_M_9_COMPRADORES]
AS
BEGIN
    SET NOCOUNT ON;

	-- Eliminar la tabla si ya existe
    IF OBJECT_ID('dbo.M_9_COMPRADORES', 'U') IS NOT NULL
    BEGIN
        DROP TABLE dbo.M_9_COMPRADORES;
    END

    -- Verificar si la tabla M_9_COMPRADORES existe, si no, crearla
    IF OBJECT_ID('dbo.M_9_COMPRADORES', 'U') IS NULL
    BEGIN
        CREATE TABLE M_9_COMPRADORES (
            COD_COMPRADOR VARCHAR(10),
            N_COMPRADOR VARCHAR(100),
			F_DATO DATETIME,  -- Fecha y hora de vigencia del dato
			F_PROC DATETIME  -- Fecha y hora de procesamiento
        );
    END

    -- Insertar el valor 'SIN COMPRADOR'
    INSERT INTO M_9_COMPRADORES (COD_COMPRADOR, N_COMPRADOR,F_DATO, F_PROC)
    SELECT '0', 'SIN COMPRADOR', GETDATE() , GETDATE();

    -- Insertar los datos desde la tabla T117_COMPRADORES
    INSERT INTO M_9_COMPRADORES (COD_COMPRADOR, N_COMPRADOR,F_DATO, F_PROC)
    SELECT 
        CONVERT(VARCHAR, C_COMPRADOR) AS COD_COMPRADOR,
        DBO.[NORMALIZA_STRING](N_COMPRADOR) AS N_COMPRADOR,
        GETDATE(), GETDATE()  -- Fecha de procesamiento en formato YYYY-MM-DD
    FROM 
        [DIARCOP001].[DiarcoP].dbo.T117_COMPRADORES
    WHERE 
        M_BAJA = 'N'; -- Filtrar solo los compradores activos

END;
GO


