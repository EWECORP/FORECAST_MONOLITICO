USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_ZTRX_M_92_DEPOSITOS]    Script Date: 19/06/2025 15:59:58 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[SP_ZTRX_M_92_DEPOSITOS]
AS
BEGIN
    SET NOCOUNT ON;
		
    -- Eliminar la tabla si ya existe
    IF OBJECT_ID('dbo.M_92_DEPOSITOS', 'U') IS NOT NULL
    BEGIN
        DROP TABLE dbo.M_92_DEPOSITOS;
    END

    -- Verificar si la tabla M_92_DEPOSITOS existe, si no, crearla
    IF OBJECT_ID('dbo.M_92_DEPOSITOS', 'U') IS NULL
    BEGIN
        CREATE TABLE M_92_DEPOSITOS (
            ID VARCHAR(10),
            DC_NOMBRE VARCHAR(100),
			F_DATO DATETIME,  -- Fecha y hora de vigencia del dato
			F_PROC DATETIME  -- Fecha y hora de procesamiento
        );
    END

    -- Insertar los datos en la tabla M_92_DEPOSITOS con la fecha de procesamiento
    INSERT INTO M_92_DEPOSITOS (ID, DC_NOMBRE, F_DATO,  F_PROC)
    SELECT 
        CASE 
            WHEN C_SUCU_EMPR = 41 THEN '41CD'
            WHEN C_SUCU_EMPR = 82 THEN '82CD'
            ELSE DBO.[NORMALIZA_STRING](C_SUCU_EMPR) 
        END AS ID,
        DBO.[NORMALIZA_STRING](N_SUCURSAL) AS DC_NOMBRE,
		GETDATE() AS F_DATO, -- Fecha y hora actual en formato DATETIME
		GETDATE() AS F_PROC -- Fecha y hora actual en formato DATETIMEecha de procesamiento en formato YYYY-MM-DD
    FROM 
        [DIARCOP001].[DIARCOP].dbo.T100_EMPRESA_SUC WITH (NOLOCK)
    WHERE 
        C_SUCU_EMPR IN (41, 82); -- Filtrar solo para las tiendas 41 y 82

END;
GO


