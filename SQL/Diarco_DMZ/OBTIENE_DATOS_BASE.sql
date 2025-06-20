--- OBTENER_MAESTRO_PRODUCTOS
SELECT       A.[C_PROVEEDOR_PRIMARIO]
            ,S.[C_ARTICULO]
            ,S.[C_SUCU_EMPR]
            ,S.[I_PRECIO_VTA]
            ,S.[I_COSTO_ESTADISTICO]
            ,S.[Q_FACTOR_VTA_SUCU]
            ,S.[Q_BULTOS_PENDIENTE_OC]-- OJO esto está en BULTOS DIARCO
            ,S.[Q_PESO_PENDIENTE_OC]
            ,S.[Q_UNID_PESO_PEND_RECEP_TRANSF]
            ,ST.Q_UNID_ARTICULO AS Q_STOCK_UNIDADES-- Stock Cierre Dia Anterior
            ,ST.Q_PESO_ARTICULO AS Q_STOCK_PESO
            ,S.[M_OFERTA_SUCU]
            ,S.[M_HABILITADO_SUCU]
            ,S.[M_FOLDER]
            ,A.M_BAJA  --- Puede no ser necesaria al hacer inner
            --,S.[Q_UNID_PESO_VTA_MES_ACTUAL]
            ,S.[F_ULTIMA_VTA]
            ,S.[Q_VTA_ULTIMOS_15DIAS]-- OJO esto está en BULTOS DIARCO
            ,S.[Q_VTA_ULTIMOS_30DIAS]-- OJO esto está en BULTOS DIARCO
            ,S.[Q_TRANSF_PEND]-- OJO esto está en BULTOS DIARCO
            ,S.[Q_TRANSF_EN_PREP]-- OJO esto está en BULTOS DIARCO
            ,A.[N_ARTICULO]
            ,A.[C_FAMILIA]
            ,A.[C_RUBRO]
            ,A.[C_CLASIFICACION_COMPRA] -- ojo nombre erroneo en la contratabla
            ,(R.[Q_VENTA_30_DIAS] + R.[Q_VENTA_15_DIAS]) AS Q_VENTA_ACUM_30 -- OJO esto está en BULTOS DIARCO
            ,R.[Q_DIAS_CON_STOCK] -- Cantidad de dias para promediar venta diaria
            ,R.[Q_REPONER] -- OJO esto está en BULTOS DIARCO
            ,R.[Q_REPONER_INCLUIDO_SOBRE_STOCK]-- OJO esto está en BULTOS DIARCO (Venta Promedio * Comprar Para + Lead Time - STOCK - PEND, OC)
                --- Ojo la venta promerio excluye  las oferta para no alterar el promedio
            ,R.[Q_VENTA_DIARIA_NORMAL]-- OJO esto está en BULTOS DIARCO
            ,R.[Q_DIAS_STOCK]
            ,R.[Q_DIAS_SOBRE_STOCK]
            ,R.[Q_DIAS_ENTREGA_PROVEEDOR]
			,AP.[Q_FACTOR_PROVEEDOR]
			,AP.[U_PISO_PALETIZADO]
			,AP.[U_ALTURA_PALETIZADO]
			,CCP.[I_LISTA_CALCULADO]
                
        FROM [DIARCOP001].[DiarcoP].[dbo].[T051_ARTICULOS_SUCURSAL] S
        INNER JOIN [DIARCOP001].[DiarcoP].[dbo].[T050_ARTICULOS] A
            ON A.[C_ARTICULO] = S.[C_ARTICULO]
        LEFT JOIN [DIARCOP001].[DiarcoP].[dbo].[T060_STOCK] ST
            ON ST.C_ARTICULO = S.[C_ARTICULO] 
            AND ST.C_SUCU_EMPR = S.[C_SUCU_EMPR]
        --LEFT JOIN [DIARCOP001].[DiarcoP].[dbo].[T055_ARTICULOS_PARAM_STOCK] P
        --    ON P.[C_SUCU_EMPR] = S.[C_SUCU_EMPR]
        --    AND P.[C_FAMILIA] = A.[C_FAMILIA]
        --    AND P.[C_RUBRO] = A.[C_RUBRO]
        --    AND P.[C_CLAISIFICACION_COMPRA] = A.[C_CLASIFICACION_COMPRA]  -- ojo nombre erroneo
        LEFT JOIN [DIARCOP001].[DiarcoP].[dbo].[T052_ARTICULOS_PROVEEDOR] AP
			ON A.[C_PROVEEDOR_PRIMARIO] = AP.[C_PROVEEDOR]
				AND S.[C_ARTICULO] = AP.[C_ARTICULO]
		LEFT JOIN [DIARCOP001].[DiarcoP].[dbo].[T055_ARTICULOS_CONDCOMPRA_COSTOS] CCP
			ON A.[C_PROVEEDOR_PRIMARIO] = CCP.[C_PROVEEDOR]
				AND S.[C_ARTICULO] = CCP.[C_ARTICULO]
				AND S.[C_SUCU_EMPR] = CCP.[C_SUCU_EMPR]

		LEFT JOIN [DIARCOP001].[DiarcoP].[dbo].[T710_ESTADIS_REPOSICION] R
            ON R.[C_ARTICULO] = S.[C_ARTICULO]
            AND R.[C_SUCU_EMPR] = S.[C_SUCU_EMPR]

        WHERE S.[M_HABILITADO_SUCU] = 'S' -- Permitido Reponer
            AND A.M_BAJA = 'N'  -- Activo en Maestro Artículos
            AND A.[C_PROVEEDOR_PRIMARIO] = 20  -- Solo del Proveedor
        
        ORDER BY S.[C_ARTICULO],S.[C_SUCU_EMPR];
 



--- OBTENER_DATOS_STOCK
		SELECT A.[C_PROVEEDOR_PRIMARIO] as Codigo_Proveedor
            ,S.[C_ARTICULO] as Codigo_Articulo
            ,S.[C_SUCU_EMPR] as Codigo_Sucursal
            ,S.[I_PRECIO_VTA] as Precio_Venta
            ,S.[I_COSTO_ESTADISTICO] as Precio_Costo
            ,S.[Q_FACTOR_VTA_SUCU] as Factor_Venta
            ,ST.Q_UNID_ARTICULO + ST.Q_PESO_ARTICULO AS Stock_Unidades-- Stock Cierre Dia Anterior
            ,(R.[Q_VENTA_30_DIAS] + R.[Q_VENTA_15_DIAS]) * S.[Q_FACTOR_VTA_SUCU] AS Venta_Unidades_30_Dias -- OJO convertida desde BULTOS DIARCO
                    
            ,(ST.Q_UNID_ARTICULO + ST.Q_PESO_ARTICULO)* S.[I_COSTO_ESTADISTICO] AS Stock_Valorizado-- Stock Cierre Dia Anterior
            ,(R.[Q_VENTA_30_DIAS] + R.[Q_VENTA_15_DIAS]) * S.[Q_FACTOR_VTA_SUCU] * S.[I_COSTO_ESTADISTICO] AS Venta_Valorizada

            ,ROUND(((ST.Q_UNID_ARTICULO + ST.Q_PESO_ARTICULO)* S.[I_COSTO_ESTADISTICO]) / 	
                ((R.[Q_VENTA_30_DIAS] + R.[Q_VENTA_15_DIAS]+0.0001) * S.[Q_FACTOR_VTA_SUCU] * S.[I_COSTO_ESTADISTICO] ),0) * 30
                AS Dias_Stock
                    
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
            AND A.[C_PROVEEDOR_PRIMARIO] = 20 -- Solo del Proveedor
                        
        ORDER BY S.[C_ARTICULO],S.[C_SUCU_EMPR];

