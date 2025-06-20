USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_ZTRX_T_5_MOVIMIENTOS_DIARIOS_DIF_X_DIAS]    Script Date: 19/06/2025 16:02:59 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE   PROCEDURE [dbo].[SP_ZTRX_T_5_MOVIMIENTOS_DIARIOS_DIF_X_DIAS]
AS
BEGIN
    SET NOCOUNT ON;

	-- Definir las fechas de consulta
    DECLARE @fecha AS DATETIME = GETDATE() - 3;
    DECLARE @FECHA_HASTA AS DATETIME = GETDATE() - 2;

    -- Eliminar la tabla T_5_MOVIMIENTOS_DIARIOS_DIF_X_DIAS si ya existe
    IF OBJECT_ID('dbo.T_5_MOVIMIENTOS_DIARIOS_DIF_X_DIAS', 'U') IS NOT NULL
    BEGIN
        DROP TABLE dbo.T_5_MOVIMIENTOS_DIARIOS_DIF_X_DIAS;
    END

    -- Crear la tabla T_5_MOVIMIENTOS_DIARIOS_DIF_X_DIAS
    CREATE TABLE T_5_MOVIMIENTOS_DIARIOS_DIF_X_DIAS (
        C_ARTICULO VARCHAR(10),
        C_SUCURSAL VARCHAR(10),
        F_MOVIMIENTO DATE,
        COD_MOVIMIENTO VARCHAR(10),
        CANTIDAD DECIMAL(18,2),
        VALOR_UNIT DECIMAL(18,2),
        PIS BIT,
        F_DATO DATETIME,  -- Fecha y hora de vigencia del dato
        F_PROC DATETIME   -- Fecha y hora de procesamiento
    );
	   
    -- Insertar movimientos de stock
    INSERT INTO T_5_MOVIMIENTOS_DIARIOS_DIF_X_DIAS (
        C_ARTICULO, C_SUCURSAL, F_MOVIMIENTO, COD_MOVIMIENTO, CANTIDAD, VALOR_UNIT, PIS, F_DATO, F_PROC
    )
    SELECT 
        CONVERT(VARCHAR, C_ARTICULO) AS C_ARTICULO,
        CASE M.C_SUCU_EMPR 
            WHEN 41 THEN '41CD' 
            ELSE DBO.[NORMALIZA_STRING](M.C_SUCU_EMPR) 
        END AS C_SUCURSAL,
        CONVERT(DATE, F_MOV) AS F_MOVIMIENTO,
        CASE 
            WHEN c_tabla = 44 THEN CAST(c_tabla AS VARCHAR(5)) 
            WHEN c_tabla = 4 AND c_mov = 75 THEN CAST(c_tabla AS VARCHAR(5)) + CAST(c_mov AS VARCHAR(5)) 
            WHEN c_tabla = 23 THEN CAST(c_tabla AS VARCHAR(5)) 
            WHEN c_tabla = 24 THEN CAST(c_tabla AS VARCHAR(5)) 
            WHEN c_tabla = 106 THEN CAST(c_tabla AS VARCHAR(5)) 
            WHEN c_tabla = 4 AND c_mov = 77 THEN CAST(c_tabla AS VARCHAR(5)) + CAST(c_mov AS VARCHAR(5)) 
            WHEN c_tabla = 43 AND c_mov = 74 THEN CAST(c_tabla AS VARCHAR(5)) + CAST(c_mov AS VARCHAR(5)) 
            WHEN c_tabla = 0 AND c_mov = 10 THEN CAST(c_tabla AS VARCHAR(5)) + CAST(c_mov AS VARCHAR(5)) 
            WHEN c_tabla = 43 AND c_mov = 73 THEN CAST(c_tabla AS VARCHAR(5)) + CAST(c_mov AS VARCHAR(5))
        END AS COD_MOVIMIENTO,
        CONVERT(DECIMAL(18,2), Q_BULTOS_KGS_MOV * Q_FACTOR_PZAS_MOV) AS CANTIDAD,
        CONVERT(DECIMAL(18,2), I_COSTO_ESTADISTICO) AS VALOR_UNIT,
        0 AS PIS,  -- Por defecto en 0
        GETDATE() AS F_DATO,  -- Fecha y hora de vigencia del dato
        GETDATE() AS F_PROC   -- Fecha y hora de procesamiento
    FROM 
        [DCO-DBCORE-P02].[DiarcoEst].[dbo].[T870_HIST_MOVIMIENTOS_STOCK] M
    INNER JOIN 
        [DIARCOP001].[DIARCOP].DBO.T100_EMPRESA_SUC SUC 
        ON SUC.C_SUCU_EMPR = M.C_SUCU_EMPR
    WHERE 
        F_MOV >= @fecha 
        AND F_MOV < @FECHA_HASTA
        AND c_tabla IN (44, 23, 24, 106, 4, 0, 43)
        AND SUC.C_SUCU_EMPR NOT IN (6, 8, 14, 17, 39, 40, 300, 80, 81, 83, 84, 88)
        AND SUC.M_SUCU_VIRTUAL = 'N'
        AND SUC.C_SUCU_EMPR NOT IN (
            SELECT C_SUCU_EMPR FROM [DIARCOP001].[DiarcoP].[dbo].[T900_SUCURSALES_EXCLUIDAS_GERENCIA_DB]
        );

    -- Insertar movimientos de ventas
    INSERT INTO T_5_MOVIMIENTOS_DIARIOS_DIF_X_DIAS (
        C_ARTICULO, C_SUCURSAL, F_MOVIMIENTO, COD_MOVIMIENTO, CANTIDAD, VALOR_UNIT, PIS, F_DATO, F_PROC
    )
    SELECT 
        CONVERT(VARCHAR, C_ARTICULO) AS C_ARTICULO,
        CASE M.C_SUCU_EMPR 
            WHEN 41 THEN '41CD' 
            ELSE DBO.[NORMALIZA_STRING](M.C_SUCU_EMPR) 
        END AS C_SUCURSAL,
        CONVERT(DATE, F_VENTA) AS F_MOVIMIENTO,
        '99' AS COD_MOVIMIENTO, -- Tipo Ventas
        CONVERT(DECIMAL(18,2), Q_UNIDADES_VENDIDAS) AS CANTIDAD,
        CONVERT(DECIMAL(18,2), I_PRECIO_VENTA) AS VALOR_UNIT,
        0 AS PIS,  -- Por defecto en 0
        GETDATE() AS F_DATO,  -- Fecha y hora de vigencia del dato
        GETDATE() AS F_PROC   -- Fecha y hora de procesamiento
    FROM 
        [DCO-DBCORE-P02].[DiarcoEst].[dbo].T702_EST_VTAS_POR_ARTICULO M
    INNER JOIN 
        [DIARCOP001].[DIARCOP].DBO.T100_EMPRESA_SUC SUC 
        ON SUC.C_SUCU_EMPR = M.C_SUCU_EMPR
    WHERE 
        CONVERT(DATE, F_VENTA) >= @fecha 
        AND CONVERT(DATE, F_VENTA) < @FECHA_HASTA
        AND SUC.C_SUCU_EMPR NOT IN (6, 8, 14, 17, 39, 40, 300, 80, 81, 83, 84, 88)
        AND SUC.M_SUCU_VIRTUAL = 'N'
        AND SUC.C_SUCU_EMPR NOT IN (
            SELECT C_SUCU_EMPR FROM [DIARCOP001].[DiarcoP].[dbo].[T900_SUCURSALES_EXCLUIDAS_GERENCIA_DB]
        );

    -- Insertar movimientos de ventas de Diarco Barrio
    INSERT INTO T_5_MOVIMIENTOS_DIARIOS_DIF_X_DIAS (
        C_ARTICULO, C_SUCURSAL, F_MOVIMIENTO, COD_MOVIMIENTO, CANTIDAD, VALOR_UNIT, PIS, F_DATO, F_PROC
    )
    SELECT 
        CONVERT(VARCHAR, C_ARTICULO) AS C_ARTICULO,
        CASE M.C_SUCU_EMPR 
            WHEN 41 THEN '41CD' 
            ELSE DBO.[NORMALIZA_STRING](M.C_SUCU_EMPR) 
        END AS C_SUCURSAL,
        CONVERT(DATE, F_VENTA) AS F_MOVIMIENTO,
        '99' AS COD_MOVIMIENTO, -- Tipo Ventas
        CONVERT(DECIMAL(18,2), Q_UNIDADES_VENDIDAS) AS CANTIDAD,
        CONVERT(DECIMAL(18,2), I_PRECIO_VENTA) AS VALOR_UNIT,
        0 AS PIS,  -- Por defecto en 0
        GETDATE() AS F_DATO,  -- Fecha y hora de vigencia del dato
        GETDATE() AS F_PROC   -- Fecha y hora de procesamiento
FROM 
        [DCO-DBCORE-P02].[DiarcoEst].[dbo].T702_EST_VTAS_POR_ARTICULO_DBARRIO M
    INNER JOIN 
        [DIARCOP001].[DIARCOP].DBO.T100_EMPRESA_SUC SUC 
        ON SUC.C_SUCU_EMPR = M.C_SUCU_EMPR
    WHERE 
        CONVERT(DATE, F_VENTA) >= @fecha 
        AND CONVERT(DATE, F_VENTA) < @FECHA_HASTA
        AND SUC.C_SUCU_EMPR NOT IN (6, 8, 14, 17, 39, 40, 300, 80, 81, 83, 84, 88)
        AND SUC.M_SUCU_VIRTUAL = 'N'
        AND SUC.C_SUCU_EMPR NOT IN (
            SELECT C_SUCU_EMPR FROM [DIARCOP001].[DiarcoP].[dbo].[T900_SUCURSALES_EXCLUIDAS_GERENCIA_DB]
        );

END;

GO


