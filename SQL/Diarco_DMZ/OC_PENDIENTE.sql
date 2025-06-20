//** LEAD TIME  x PROVEEDOR x SUCURSAL   ***//
SELECT TOP 100 * FROM [DIARCOP001].[DiarcoP].[dbo].[T020_PROVEEDOR_DIAS_ENTREGA_DETA]
SELECT TOP 100 * FROM [DIARCOP001].[DiarcoP].[dbo].[T052_ARTICULOS_PROVEEDOR]
SELECT TOP 100 * FROM [DIARCOP001].[DiarcoP].
SELECT TOP 100 * FROM [DIARCOP001].[DiarcoP].
SELECT TOP 100 * FROM [DIARCOP001].[DiarcoP].

SELECT  TOP 100 [C_OC]
      ,[U_PREFIJO_OC]
      ,[U_SUFIJO_OC]      
      ,[U_DIAS_LIMITE_ENTREGA]
	  , DATEADD(DAY, [U_DIAS_LIMITE_ENTREGA], [F_ENTREGA]) as FECHA_LIMITE
	  , DATEDIFF (DAY, DATEADD(DAY, [U_DIAS_LIMITE_ENTREGA], [F_ENTREGA]), GETDATE()) as DEMORA
      ,[C_PROVEEDOR]
      ,[C_SUCU_COMPRA]
      ,[C_SUCU_DESTINO]
      ,[C_SUCU_DESTINO_ALT]
      ,[C_SITUAC]
      ,[F_SITUAC]
      ,[F_ALTA_SIST]
      ,[F_EMISION]
      ,[F_ENTREGA]    
       ,[C_USUARIO_OPERADOR]    
  -- SELECT TOP 1000 *     
  FROM [DIARCOP001].[DiarcoP].[dbo].[T080_OC_CABE]  
  WHERE [C_SITUAC] = 1
  AND C_PROVEEDOR = 20
  AND DATEADD(DAY, [U_DIAS_LIMITE_ENTREGA], [F_ENTREGA]) < GETDATE()
   
GO


SELECT 
    cab.C_PROVEEDOR,
    --cab.C_OC,
    --cab.U_PREFIJO_OC,
    cab.U_SUFIJO_OC as NRO_OC,
    --cab.F_EMISION,
	cab.C_SITUAC,
    cab.F_ENTREGA as FECHA_EMISION,
    cab.U_DIAS_LIMITE_ENTREGA as PLAZO,
    DATEADD(DAY, cab.U_DIAS_LIMITE_ENTREGA, cab.F_ENTREGA) AS FECHA_LIMITE,
	DATEDIFF (DAY, DATEADD(DAY, [U_DIAS_LIMITE_ENTREGA], [F_ENTREGA]), GETDATE()) as DEMORA,
	cab.F_COMP_ING_MERC as FECHA_CUMPLIMIENTO,
    det.M_CUMPLIDA_PARCIAL,
    YEAR(cab.F_ENTREGA) AS ANIO_ENTREGA,
    MONTH(cab.F_ENTREGA) AS MES_ENTREGA
FROM 
    [DIARCOP001].[DiarcoP].[dbo].[T080_OC_CABE] cab
INNER JOIN 
    [DIARCOP001].[DiarcoP].[dbo].[T081_OC_DETA] det
ON 
    cab.C_OC = det.C_OC 
    AND cab.U_PREFIJO_OC = det.U_PREFIJO_OC
    AND cab.U_SUFIJO_OC = det.U_SUFIJO_OC
WHERE 
    YEAR(cab.F_ENTREGA) = 2025
    AND cab.C_SITUAC = 2 --  órdenes 1= Pendientes y 2= Cerradas
    AND cab.C_PROVEEDOR = 20

--- VERSIÓN OTIF
SELECT  
    cab.C_PROVEEDOR,
    cab.U_SUFIJO_OC AS NRO_OC,
    cab.F_ENTREGA AS FECHA_EMISION,
    cab.U_DIAS_LIMITE_ENTREGA AS PLAZO,
    DATEADD(DAY, cab.U_DIAS_LIMITE_ENTREGA, cab.F_ENTREGA) AS FECHA_LIMITE,
	cab.C_SITUAC,
    cab.F_COMP_ING_MERC AS FECHA_CUMPLIMIENTO,
    det.M_CUMPLIDA_PARCIAL,
    YEAR(cab.F_ENTREGA) AS ANIO_ENTREGA,
    MONTH(cab.F_ENTREGA) AS MES_ENTREGA,
	CASE 
        WHEN cab.F_COMP_ING_MERC IS NOT NULL  
                AND cab.F_COMP_ING_MERC > '1900-01-01' THEN 1 
        ELSE 0 
    END AS OC_RECIBIDAS,  ---- Órdenes que se Recibieron

	-- Cálculo del cumplimiento OC
	CASE 
        WHEN cab.F_COMP_ING_MERC IS NOT NULL 
             AND cab.F_COMP_ING_MERC > '1900-01-01' THEN 1 
		ELSE 0 
    END AS OC_CUMPLIDA,

    -- Cálculo del cumplimiento OTIF, ignorando fechas inválidas
    CASE 
        WHEN cab.F_COMP_ING_MERC IS NOT NULL 
             AND cab.F_COMP_ING_MERC > '1900-01-01'
             AND cab.F_COMP_ING_MERC <= DATEADD(DAY, cab.U_DIAS_LIMITE_ENTREGA, cab.F_ENTREGA) 
             AND det.M_CUMPLIDA_PARCIAL = 'N' THEN 1 
        ELSE 0 
    END AS OTIF

FROM  
    [DIARCOP001].[DiarcoP].[dbo].[T080_OC_CABE] cab
INNER JOIN  
    [DIARCOP001].[DiarcoP].[dbo].[T081_OC_DETA] det
ON  
    cab.C_OC = det.C_OC  
    AND cab.U_PREFIJO_OC = det.U_PREFIJO_OC 
    AND cab.U_SUFIJO_OC = det.U_SUFIJO_OC
WHERE  
    YEAR(cab.F_ENTREGA) = 2025 
	AND DATEADD(DAY, cab.U_DIAS_LIMITE_ENTREGA, cab.F_ENTREGA) < GETDATE()
    AND cab.C_SITUAC <= 2  -- órdenes 1= Pendientes y 2= Cerradas  
    AND cab.C_PROVEEDOR = 20


//****** OTIF FINAL 
¡Por supuesto! Para obtener un resumen del indicador OTIF, puedes calcular el porcentaje de cumplimiento agrupado por mes y año. Esto se logra dividiendo la cantidad de órdenes cumplidas (OTIF = 1) entre el total de órdenes de ese período.
Aquí te dejo la consulta modificada para calcular el porcentaje de OTIF mensual:
*****//

SELECT  
    YEAR(cab.F_ENTREGA) AS ANIO_ENTREGA,
    MONTH(cab.F_ENTREGA) AS MES_ENTREGA,
    COUNT(*) AS TOTAL_OC,  -- Total de órdenes de compra
	SUM(
        CASE 
            WHEN cab.F_COMP_ING_MERC IS NOT NULL  
                 AND cab.F_COMP_ING_MERC > '1900-01-01' THEN 1 
            ELSE 0 
        END
    ) AS OC_RECIBIDAS,  ---- Órdenes que se Recibieron

    SUM(
        CASE 
            WHEN cab.F_COMP_ING_MERC IS NOT NULL  
                 AND cab.F_COMP_ING_MERC > '1900-01-01' 
                 AND cab.F_COMP_ING_MERC <= DATEADD(DAY, cab.U_DIAS_LIMITE_ENTREGA, cab.F_ENTREGA) 
                 -- AND det.M_CUMPLIDA_PARCIAL = 'N' 
				 THEN 1 
            ELSE 0 
        END
    ) AS OC_CUMPLIDAS,  -- Órdenes OTIF
    CAST( 
        SUM(
            CASE 
                WHEN cab.F_COMP_ING_MERC IS NOT NULL  
                     AND cab.F_COMP_ING_MERC > '1900-01-01' 
                     AND cab.F_COMP_ING_MERC <= DATEADD(DAY, cab.U_DIAS_LIMITE_ENTREGA, cab.F_ENTREGA) 
                     -- AND det.M_CUMPLIDA_PARCIAL = 'N' 
					 THEN 1 
                ELSE 0 
            END
        ) * 100.0 / COUNT(*) AS DECIMAL(5,2)
    ) AS PORCENTAJE_OTIF -- Porcentaje de cumplimiento OTIF

FROM  
    [DIARCOP001].[DiarcoP].[dbo].[T080_OC_CABE] cab
--INNER JOIN  
--    [DIARCOP001].[DiarcoP].[dbo].[T081_OC_DETA] det
--ON  
--    cab.C_OC = det.C_OC  
--    AND cab.U_PREFIJO_OC = det.U_PREFIJO_OC 
--    AND cab.U_SUFIJO_OC = det.U_SUFIJO_OC
WHERE  
    YEAR(cab.F_ENTREGA) = 2025  
    AND cab.C_SITUAC <= 2  -- órdenes 1= Pendientes y 2= Cerradas  
    AND cab.C_PROVEEDOR = 20
GROUP BY  
    YEAR(cab.F_ENTREGA), MONTH(cab.F_ENTREGA)
ORDER BY  
    ANIO_ENTREGA, MES_ENTREGA;

//***** 
necesitamos trabajar a nivel de cabezal de las órdenes de compra y calcular el OTIF a nivel de la OC completa. Aquí te dejo una solución adaptada:
- Primero, evalúa el OTIF a nivel de los ítems en el detalle.
- Después, agrupa los resultados a nivel de la orden de compra (OC).
- Finalmente, realiza el resumen general a nivel mensual.
*******//

WITH OTIF_DETALLE AS (
    -- Evaluamos cumplimiento a nivel de los ítems
    SELECT  
        cab.C_OC,
        cab.U_PREFIJO_OC,
        cab.U_SUFIJO_OC,
        cab.C_PROVEEDOR,
        cab.C_SITUAC,
        cab.F_ENTREGA AS FECHA_EMISION,
        cab.U_DIAS_LIMITE_ENTREGA AS PLAZO,
        DATEADD(DAY, cab.U_DIAS_LIMITE_ENTREGA, cab.F_ENTREGA) AS FECHA_LIMITE,
        cab.F_COMP_ING_MERC AS FECHA_CUMPLIMIENTO,
        det.M_CUMPLIDA_PARCIAL,
        YEAR(cab.F_ENTREGA) AS ANIO_ENTREGA,
        MONTH(cab.F_ENTREGA) AS MES_ENTREGA,

		CASE 
            WHEN cab.F_COMP_ING_MERC IS NOT NULL  
                 AND cab.F_COMP_ING_MERC > '1900-01-01' THEN 1 
            ELSE 0 
        END AS OC_RECIBIDAS, 

        -- Evaluamos OTIF a nivel de ítem
        CASE 
            WHEN cab.F_COMP_ING_MERC IS NOT NULL  
                 AND cab.F_COMP_ING_MERC > '1900-01-01' 
                 AND cab.F_COMP_ING_MERC <= DATEADD(DAY, cab.U_DIAS_LIMITE_ENTREGA, cab.F_ENTREGA) 
                 AND det.M_CUMPLIDA_PARCIAL = 'N' THEN 1 
            ELSE 0 
        END AS OTIF_ITEM

    FROM  
        [DIARCOP001].[DiarcoP].[dbo].[T080_OC_CABE] cab
    INNER JOIN  
        [DIARCOP001].[DiarcoP].[dbo].[T081_OC_DETA] det
    ON  
        cab.C_OC = det.C_OC  
        AND cab.U_PREFIJO_OC = det.U_PREFIJO_OC 
        AND cab.U_SUFIJO_OC = det.U_SUFIJO_OC
    WHERE  
        YEAR(cab.F_ENTREGA) = 2025
		AND MONTH(cab.F_ENTREGA) = 3
        AND cab.C_SITUAC <= 2  -- órdenes 1= Pendientes y 2= Cerradas  
        AND cab.C_PROVEEDOR < 141
),
OTIF_ORDEN AS (
    -- Consolidamos a nivel de OC
    SELECT
        C_OC,
        U_PREFIJO_OC,
        U_SUFIJO_OC,
        C_PROVEEDOR,
        ANIO_ENTREGA,
        MES_ENTREGA,
        -- Una orden es OTIF si todos sus ítems cumplen OTIF
        MIN(OTIF_ITEM) AS OTIF_OC
    FROM OTIF_DETALLE
    GROUP BY 
        C_OC, U_PREFIJO_OC, U_SUFIJO_OC, C_PROVEEDOR, ANIO_ENTREGA, MES_ENTREGA
)
-- Resumen final con porcentaje OTIF mensual
SELECT
    ANIO_ENTREGA,
    MES_ENTREGA,
	C_PROVEEDOR,
    COUNT(*) AS TOTAL_OC,  -- Total de órdenes de compra
    SUM(OTIF_OC) AS OC_CUMPLIDAS,  -- Total de órdenes OTIF
    CAST(SUM(OTIF_OC) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS PORCENTAJE_OTIF
FROM OTIF_ORDEN
GROUP BY ANIO_ENTREGA, MES_ENTREGA,C_PROVEEDOR
ORDER BY ANIO_ENTREGA, MES_ENTREGA;


-------------- NUEVA VERSIÓN EWE TUNEADA
//***  OJO PARAMETROS

SELECT COUNT(*)
FROM [DIARCOP001].[DiarcoP].[dbo].[T080_OC_CABE]
WHERE [F_EMISION] BETWEEN '2025-03-01' AND '2025-03-31';

  EN MARZO 2025 Hubo 14.500 OC

  ******//

WITH OTIF_DETALLE AS (
    -- Evaluamos cumplimiento a nivel de los ítems
    SELECT  
        cab.C_OC,
        cab.U_PREFIJO_OC,
        cab.U_SUFIJO_OC,
        cab.C_PROVEEDOR,
        cab.C_SITUAC,
        cab.F_ENTREGA AS FECHA_EMISION,
        prv.Q_DIAS_PREPARACION AS LEAD_TIME,
		DATEADD(DAY, prv.Q_DIAS_PREPARACION, cab.F_ENTREGA) AS FECHA_ESPERADA,
		cab.U_DIAS_LIMITE_ENTREGA AS PLAZO,
        DATEADD(DAY, cab.U_DIAS_LIMITE_ENTREGA, cab.F_ENTREGA) AS FECHA_LIMITE,
        cab.F_COMP_ING_MERC AS FECHA_CUMPLIMIENTO,
        --det.M_CUMPLIDA_PARCIAL,
        YEAR(cab.F_ENTREGA) AS ANIO_ENTREGA,
        MONTH(cab.F_ENTREGA) AS MES_ENTREGA,

        -- Evaluamos OTIF a nivel de ítem
        CASE 
            WHEN cab.F_COMP_ING_MERC IS NOT NULL  
                 AND cab.F_COMP_ING_MERC > '1900-01-01' 
                 AND cab.F_COMP_ING_MERC <= DATEADD(DAY, cab.U_DIAS_LIMITE_ENTREGA, cab.F_ENTREGA) 
                -- AND det.M_CUMPLIDA_PARCIAL = 'N' 
				THEN 1 
            ELSE 0 
        END AS OTIF_ITEM

    FROM  
        [DIARCOP001].[DiarcoP].[dbo].[T080_OC_CABE] cab
    LEFT JOIN  
        [DIARCOP001].[DiarcoP].[dbo].[T081_OC_DETA] det
    ON  
        cab.C_OC = det.C_OC  
        AND cab.U_PREFIJO_OC = det.U_PREFIJO_OC 
        AND cab.U_SUFIJO_OC = det.U_SUFIJO_OC
	FULL JOIN  
        [DIARCOP001].[DiarcoP].[dbo].[T020_PROVEEDOR_DIAS_ENTREGA_DETA] prv
    ON  
        cab.C_PROVEEDOR = prv.C_PROVEEDOR
        AND cab.C_SUCU_COMPRA = prv.C_SUCU_EMPR 

    WHERE  
        YEAR(cab.F_ENTREGA) = 2025
		--AND MONTH(cab.F_ENTREGA) = 3
        AND cab.C_SITUAC <= 2  -- órdenes 1= Pendientes y 2= Cerradas  
        AND cab.C_PROVEEDOR = 20
),
OTIF_ORDEN AS (
    -- Consolidamos a nivel de OC
    SELECT
        C_OC,
        U_PREFIJO_OC,
        U_SUFIJO_OC,
        C_PROVEEDOR,
        ANIO_ENTREGA,
        MES_ENTREGA,
        -- Una orden es OTIF si todos sus ítems cumplen OTIF
        MIN(OTIF_ITEM) AS OTIF_OC,
        -- Marcamos la OC como recibida si cumple las condiciones de F_COMP_ING_MERC
        CASE
            WHEN MIN(CASE 
                        WHEN FECHA_LIMITE IS NOT NULL AND FECHA_LIMITE > '1900-01-01' 
                        THEN 1 ELSE 0 END) = 1 THEN 1 
            ELSE 0
        END AS OC_RECIBIDA
    FROM OTIF_DETALLE
    GROUP BY 
        C_OC, U_PREFIJO_OC, U_SUFIJO_OC, C_PROVEEDOR, ANIO_ENTREGA, MES_ENTREGA
)
-- Resumen final con porcentaje OTIF mensual y OC_RECIBIDAS
SELECT
    ANIO_ENTREGA,
    MES_ENTREGA,
    COUNT(*) AS TOTAL_OC,  -- Total de órdenes de compra
    SUM(OTIF_OC) AS OC_CUMPLIDAS,  -- Total de órdenes OTIF
    CAST(SUM(OTIF_OC) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS PORCENTAJE_OTIF, -- Porcentaje OTIF
    SUM(OC_RECIBIDA) AS OC_RECIBIDAS  -- Total de órdenes recibidas
FROM OTIF_ORDEN
GROUP BY ANIO_ENTREGA, MES_ENTREGA
ORDER BY ANIO_ENTREGA, MES_ENTREGA;