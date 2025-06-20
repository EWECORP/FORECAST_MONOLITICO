USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_ZTRX_M_3_2_FAMILIA_ARTICULO]    Script Date: 19/06/2025 15:57:31 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE   PROCEDURE [dbo].[SP_ZTRX_M_3_2_FAMILIA_ARTICULO]
AS 
BEGIN
    SET NOCOUNT ON;
	-- Eliminar la tabla si ya existe
    IF OBJECT_ID('dbo.M_3_2_FAMILIA_ARTICULO', 'U') IS NOT NULL
    BEGIN
        DROP TABLE dbo.M_3_2_FAMILIA_ARTICULO;
    END

    -- Verificar si la tabla M_3_2_FAMILIA_ARTICULO existe, si no, crearla
    IF OBJECT_ID('dbo.M_3_2_FAMILIA_ARTICULO', 'U') IS NULL
    BEGIN
        CREATE TABLE M_3_2_FAMILIA_ARTICULO (
            COD_FAMILIA VARCHAR(10),
            COD_ARTICULO VARCHAR(10),
			F_DATO DATETIME,  -- Fecha y hora de vigencia del dato
			F_PROC DATETIME  -- Fecha y hora de procesamiento
        );
    END

    -- Insertar los datos en la tabla M_3_2_FAMILIA_ARTICULO con la fecha de procesamiento
    INSERT INTO M_3_2_FAMILIA_ARTICULO (COD_FAMILIA, COD_ARTICULO,F_DATO, F_PROC)
    SELECT DISTINCT
        CAST(C_CLASIFICACION_COMPRA AS VARCHAR(10)) AS COD_FAMILIA,
        CAST(C_ARTICULO AS VARCHAR(10)) AS COD_ARTICULO,
		GETDATE() AS F_DATO, -- Fecha y hora actual en formato DATETIME
		GETDATE() AS F_PROC -- Fecha y hora actual en formato DATETIME
    FROM [DIARCOP001].[DIARCOP].dbo.T050_ARTICULOS WITH (NOLOCK)
    WHERE C_ARTICULO NOT IN (
        SELECT C_ARTICULO FROM [DIARCOP001].[DIARCOP].dbo.T050_ARTICULOS_DIFERENCIAS_DE_PRECIOS WITH (NOLOCK)
    )
    AND M_BAJA = 'N';

END;
GO


