SELECT A.[C_PROVEEDOR_PRIMARIO] as Codigo_Proveedor
    ,S.[C_ARTICULO] as Codigo_Articulo
    ,S.[C_SUCU_EMPR] as Codigo_Sucursal
    ,S.[I_PRECIO_VTA] as Precio_Venta
    ,S.[I_COSTO_ESTADISTICO] as Precio_Costo
    ,S.[Q_FACTOR_VTA_SUCU] as Factor_Venta
    ,ST.Q_UNID_ARTICULO + ST.Q_PESO_ARTICULO AS Stock_Unidades-- Stock Cierre Dia Anterior
    ,(R.[Q_VENTA_30_DIAS] + R.[Q_VENTA_15_DIAS]) * S.[Q_FACTOR_VTA_SUCU] AS Venta_Unidades_30_Dias -- OJO convertida desde BULTOS DIARCO
            
    ,(ST.Q_UNID_ARTICULO + ST.Q_PESO_ARTICULO)* S.[I_COSTO_ESTADISTICO] AS Stock_Valorizado  -- Stock Cierre Dia Anterior
    ,(R.[Q_VENTA_30_DIAS] + R.[Q_VENTA_15_DIAS]) * S.[Q_FACTOR_VTA_SUCU] * S.[I_COSTO_ESTADISTICO] AS Venta_Valorizada
	    
    ,S.[F_ULTIMA_VTA]
    ,S.[Q_VTA_ULTIMOS_15DIAS] * S.[Q_FACTOR_VTA_SUCU] AS VENTA_UNIDADES_1Q -- OJO esto está en BULTOS DIARCO
    ,S.[Q_VTA_ULTIMOS_30DIAS] * S.[Q_FACTOR_VTA_SUCU] AS VENTA_UNIDADES_2Q -- OJO esto está en BULTOS DIARCO
        
FROM [DIARCOP001].[DiarcoP].[dbo].[T051_ARTICULOS_SUCURSAL] S
INNER JOIN [DIARCOP001].[DiarcoP].[dbo].[T050_ARTICULOS] A
    ON A.[C_ARTICULO] = S.[C_ARTICULO]
LEFT JOIN [DIARCOP001].[DiarcoP].[dbo].[T060_STOCK] ST
    ON ST.C_ARTICULO = S.[C_ARTICULO] 
    AND ST.C_SUCU_EMPR = S.[C_SUCU_EMPR]
LEFT JOIN [DIARCOP001].[DiarcoP].[dbo].[T710_ESTADIS_REPOSICION] R
    ON R.[C_ARTICULO] = S.[C_ARTICULO]
    AND R.[C_SUCU_EMPR] = S.[C_SUCU_EMPR]

WHERE S.[M_HABILITADO_SUCU] = 'S' -- Permitido Reponer
    AND A.M_BAJA = 'N'  -- Activo en Maestro Artículos
    AND A.[C_PROVEEDOR_PRIMARIO] IN (20, 25, 62, 98, 189, 327, 1465, 8449)  -- Solo LISTA DE PROVEEDORES
        
ORDER BY S.[C_ARTICULO],S.[C_SUCU_EMPR];
        