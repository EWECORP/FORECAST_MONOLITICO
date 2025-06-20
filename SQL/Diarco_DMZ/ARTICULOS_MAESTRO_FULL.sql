 SELECT TOP 100 A.[C_PROVEEDOR_PRIMARIO]
            ,S.[C_ARTICULO]
            ,S.[C_SUCU_EMPR]
			--,R.[C_SUCU_EMPR]
			--,R.[C_ARTICULO]
			,S.[I_PRECIO_VTA]
            ,S.[I_COSTO_ESTADISTICO]
            ,S.[Q_FACTOR_VENTA_ESP]
            ,S.[Q_FACTOR_VTA_SUCU]
            ,S.[M_OFERTA_SUCU]
            ,S.[M_HABILITADO_SUCU]
            ,A.M_BAJA
            ,S.[Q_VTA_DIA_ANT]
            ,S.[Q_VTA_ACUM]
            ,S.[Q_ULT_ING_STOCK]
            ,S.[Q_STOCK_A_ULT_ING]
            ,S.[Q_15DIASVTA_A_ULT_ING_STOCK]
            ,S.[Q_30DIASVTA_A_ULT_ING_STOCK]
            ,S.[Q_BULTOS_PENDIENTE_OC]
            ,S.[Q_PESO_PENDIENTE_OC]
            ,S.[Q_UNID_PESO_PEND_RECEP_TRANSF]
            ,S.[Q_UNID_PESO_VTA_MES_ACTUAL]
            ,S.[F_ULTIMA_VTA]
            ,S.[Q_VTA_ULTIMOS_15DIAS]
            ,S.[Q_VTA_ULTIMOS_30DIAS]
            ,S.[Q_TRANSF_PEND]
            ,S.[Q_TRANSF_EN_PREP]
            ,S.[M_FOLDER]
            ,S.[M_ALTA_RENTABILIDAD]
            ,S.[Lugar_Abastecimiento]
            ,S.[M_COSTO_LOGISTICO]
            ,A.[N_ARTICULO]
            ,A.[C_FAMILIA]
            ,A.[C_RUBRO]

			  ,R.[Q_VENTA_30_DIAS]
			  ,R.[Q_VENTA_15_DIAS]
			  ,R.[Q_VENTA_DOMINGO]
			  ,R.[Q_VENTA_ESPECIAL_30_DIAS]
			  ,R.[Q_VENTA_ESPECIAL_15_DIAS]
			  ,R.[Q_DIAS_CON_STOCK]
			  ,R.[Q_REPONER]
			  ,R.[Q_REPONER_INCLUIDO_SOBRE_STOCK]

			  ,R.[Q_VENTA_DIARIA_NORMAL]
			  ,R.[Q_DIAS_STOCK]
			  ,R.[Q_DIAS_SOBRE_STOCK]
			  ,R.[Q_DIAS_ENTREGA_PROVEEDOR]
        
        FROM [DIARCOP001].[DiarcoP].[dbo].[T051_ARTICULOS_SUCURSAL] S
        LEFT JOIN [DIARCOP001].[DiarcoP].[dbo].[T050_ARTICULOS] A
            ON A.[C_ARTICULO] = S.[C_ARTICULO]
        LEFT JOIN [DIARCOP001].[DiarcoP].[dbo].[T055_ARTICULOS_PARAM_STOCK] P
            ON P.[C_SUCU_EMPR] = S.[C_SUCU_EMPR]
            AND P.[C_FAMILIA] =A.[C_FAMILIA]
		LEFT JOIN [DIARCOP001].[DiarcoP].[dbo].[T710_ESTADIS_REPOSICION] R
			ON R.[C_ARTICULO] = S.[C_ARTICULO]
			AND R.[C_SUCU_EMPR] = S.[C_SUCU_EMPR]


        WHERE S.[M_HABILITADO_SUCU] = 'S' -- Permitido Reponer
            AND A.M_BAJA = 'N'  -- Activo en Maestro Artículos
            AND A.[C_PROVEEDOR_PRIMARIO] = 20 -- Solo del Proveedor

		ORDER BY S.[C_ARTICULO],S.[C_SUCU_EMPR]
        ;