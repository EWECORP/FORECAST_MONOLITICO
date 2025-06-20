/*** ALGORITMOS PARA DEMANDA PYTHON  (UTILIZA: DiarcoEst SERVER=192.168.0.250 ***/

SELECT TOP 1000 V.[F_VENTA] as Fecha
      ,V.[C_ARTICULO] as Codigo_Articulo
      ,V.[C_SUCU_EMPR] as Sucursal
      ,V.[I_PRECIO_VENTA] as Precio
      ,V.[I_PRECIO_COSTO] as Costo
 --     ,V.[I_VENDIDO] as Total 
      ,V.[Q_UNIDADES_VENDIDAS] as Unidades
   
      ,V.[C_FAMILIA] as Familia
      ,A.[C_RUBRO] as Rubro
      ,A.[C_SUBRUBRO_1] as SubRubro
 --     ,A.[C_SUBRUBRO_2]
      ,A.[N_ARTICULO] as Nombre_Articulo
	  ,A.[C_CLASIFICACION_COMPRA] as Clasificacion
 
  FROM [DCO-DBCORE-P02].[DiarcoEst].[dbo].[T702_EST_VTAS_POR_ARTICULO] V
  LEFT JOIN [DCO-DBCORE-P02].[DiarcoEst].[dbo].[T050_ARTICULOS] A 
	ON V.C_ARTICULO = A.C_ARTICULO
WHERE V.[C_SUCU_EMPR] BETWEEN  5 AND 5  AND
V.F_VENTA >='20240101'

/***
SELECT TOP 100 *  FROM [DCO-DBCORE-P02].[DiarcoEst].[dbo].[T050_ARTICULOS]
SELECT TOP 100 *  FROM [DCO-DBCORE-P02].[DiarcoEst].[dbo].[T051_ARTICULOS_SUCURSAL]


***/