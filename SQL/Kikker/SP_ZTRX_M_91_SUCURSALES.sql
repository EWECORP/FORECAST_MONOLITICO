USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_ZTRX_M_91_SUCURSALES]    Script Date: 19/06/2025 15:59:39 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE [dbo].[SP_ZTRX_M_91_SUCURSALES]
AS
BEGIN
    SET NOCOUNT ON;
	   
	-- Eliminar la tabla si ya existe
    IF OBJECT_ID('dbo.M_91_SUCURSALES', 'U') IS NOT NULL
    BEGIN
        DROP TABLE dbo.M_91_SUCURSALES;
    END

    -- Verificar si la tabla M_91_SUCURSALES existe, si no, crearla
    IF OBJECT_ID('dbo.M_91_SUCURSALES', 'U') IS NULL
    BEGIN
        CREATE TABLE M_91_SUCURSALES (
            ID_TIENDA VARCHAR(10),
            SUC_NOMBRE VARCHAR(100),
            F_DATO DATETIME,  -- Fecha y hora de vigencia del dato
			F_PROC DATETIME  -- Fecha y hora de procesamiento
        );
    END

    -- Insertar los datos en la tabla M_91_SUCURSALES con la fecha de procesamiento
    INSERT INTO M_91_SUCURSALES (ID_TIENDA, SUC_NOMBRE, F_DATO, F_PROC)
    SELECT 
        CASE 
            WHEN C_SUCU_EMPR = 41 THEN '41CD'
            WHEN C_SUCU_EMPR = 82 THEN '82CD'
            ELSE DBO.[NORMALIZA_STRING](C_SUCU_EMPR) 
        END AS ID_TIENDA,
        DBO.[NORMALIZA_STRING](N_SUCURSAL) AS SUC_NOMBRE,
        GETDATE() AS F_DATO, -- Fecha y hora actual en formato DATETIME
		GETDATE() AS F_PROC -- Fecha y hora actual en formato DATETIME
    FROM 
        [DIARCOP001].[DIARCOP].dbo.T100_EMPRESA_SUC WITH (NOLOCK)
    WHERE 
        C_SUCU_EMPR NOT IN (6, 8, 14, 17, 39, 40, 300, 80, 81, 83, 84, 88) -- Excluir tiendas no necesarias
    AND 
        M_SUCU_VIRTUAL = 'N' -- Excluir tiendas virtuales
    AND 
        C_SUCU_EMPR NOT IN (SELECT C_SUCU_EMPR FROM [DIARCOP001].[DiarcoP].dbo.T900_SUCURSALES_EXCLUIDAS_GERENCIA_DB) -- Excluir sucursales cerradas por gerencia

END;
GO


