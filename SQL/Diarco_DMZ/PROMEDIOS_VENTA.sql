/*** CALCULAR PROMEDIOS DIARIOS y MENSUALES de VENTA ***/

WITH VentasAgrupadas AS (
    SELECT 
        V.[C_ARTICULO],
        V.[C_SUCU_EMPR],
        CONVERT(DATE, V.[F_VENTA]) AS FechaVenta, -- Convertimos a DATE para agrupar por día
        MONTH(V.[F_VENTA]) AS Mes,
        YEAR(V.[F_VENTA]) AS Año,
        SUM(V.[Q_UNIDADES_VENDIDAS]) AS TotalUnidadesDiarias
    FROM [DiarcoEst].[dbo].[T702_EST_VTAS_POR_ARTICULO] V
    WHERE V.C_ARTICULO BETWEEN 50 AND 100  
      AND V.F_VENTA >= '20240101'
    GROUP BY V.[C_ARTICULO], V.[C_SUCU_EMPR], CONVERT(DATE, V.[F_VENTA]), MONTH(V.[F_VENTA]), YEAR(V.[F_VENTA])
),
Promedios AS (
    SELECT 
        C_ARTICULO,
        C_SUCU_EMPR,
        Mes,
        Año,
        AVG(TotalUnidadesDiarias) AS PromedioDiario,
        SUM(TotalUnidadesDiarias) AS TotalUnidadesMensuales
    FROM VentasAgrupadas
    GROUP BY C_ARTICULO, C_SUCU_EMPR, Mes, Año
)
SELECT 
    P.C_ARTICULO,
    P.C_SUCU_EMPR,
    P.Año,
    P.Mes,
    P.PromedioDiario,
    P.TotalUnidadesMensuales,
    P.TotalUnidadesMensuales / COUNT(*) AS PromedioMensual
FROM Promedios P
GROUP BY P.C_ARTICULO, P.C_SUCU_EMPR, P.Año, P.Mes, P.PromedioDiario, P.TotalUnidadesMensuales;

