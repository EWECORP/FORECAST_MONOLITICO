  /*** VER OC PENDIENES CABECERA y DETALLE JUNTOS   ***/
  /***  Tipos de Precios 
	1	Estaod OC Pendiente                               	Pendiente 
	2	Estado OC Cerrada                                 	Cumplida  
	3	Estado OC Anulada                                 	Anulada   
  ***/

SELECT TOP 1000 
		C.[C_OC]
      ,C.[U_PREFIJO_OC]
      ,C.[U_SUFIJO_OC]
      ,C.[U_PREFIJO_LOTE]
      ,C.[U_LOTE]
      ,C.[M_OC_MADRE]
      ,C.[M_OC_PARA_TRANSFERENCIA]
      ,C.[M_OC_PAGOANT]
      ,C.[U_DIAS_LIMITE_ENTREGA]
      ,C.[C_PROVEEDOR]
      ,C.[C_SUCU_COMPRA]
      ,C.[C_SUCU_DESTINO]
      ,C.[C_SUCU_DESTINO_ALT]
      ,C.[C_SITUAC]
      ,C.[F_SITUAC]
      ,C.[F_ALTA_SIST]
      ,C.[F_EMISION]
      ,C.[F_ENTREGA]
      ,C.[I_NETO_OC]
      ,C.[I_IVA_OC]
      ,C.[I_IMP_INTERNO_OC]
      ,C.[I_TOTAL_OC]
      ,C.[C_COMPRADOR]
      ,C.[C_USUARIO_OPERADOR]
      ,C.[C_TERMINAL_OPERADOR]
      ,C.[C_USUARIO_CUMPLIO]
      ,C.[C_TIPO]
      ,C.[C_PLAZO_ENTREGA1]

      ,C.[C_PLAZO_ENTREGA6]
      ,C.[D_COND_PAGO]
      ,C.[D_OBSERVACION]
      ,C.[F_COMP_ING_MERC]
      ,C.[C_COMP_ING_MERC]
      ,C.[U_PREFIJO_COMP_ING_MERC]
      ,C.[U_SUFIJO_COMP_ING_MERC]
      ,C.[D_OBSERVACION_ING_MERC]
      ,C.[C_TIPO_ENTREGA_MERCADERIA]
      ,C.[C_USUARIO_MODIFICO]
      ,C.[C_TERMINAL_MODIFICO]
      ,C.[F_MODIFICO]
      ,C.[M_OC_ELECTRONICA]
      ,C.[C_SITUAC_OC_ELECTRONICA]
      ,C.[F_SITUAC_OC_ELECTRONICA]
      ,C.[M_ENVIADO]
      ,C.[M_ESP]
      ,C.[C_TIPO_PROVEEDOR_EDI]

	  --- Detalle ---

	  ,D.[C_ARTICULO]
      ,D.[Q_BULTOS_SUGERIDOS]
      ,D.[Q_BULTOS_PROV_PED]
      ,D.[Q_FACTOR_PROV_PED]
      ,D.[Q_BULTOS_PROV_BONIF]
      ,D.[Q_BULTOS_EMPR_PED]
      ,D.[Q_FACTOR_EMPR_PED]
      ,D.[Q_PESO_UNIT_ART]
      ,D.[Q_PESO_TOTAL_PED]
      ,D.[Q_PESO_TOTAL_BONIF]
      ,D.[C_IVA_EN_CALCULO]
      ,D.[K_COEF_IVA]
      ,D.[K_IMP_INTERNO]
      ,D.[I_COSTO_BASE]
      ,D.[I_PRECIO_COMPRA]
      ,D.[I_PRECIO_PARTE]
      ,D.[I_PRECIO_LISTA]
      ,D.[I_IMP_INTERNO]
      ,D.[I_ENVASES]
      ,D.[I_TOTAL_IMP_INTERNO]
      ,D.[I_TOTAL_ITEM]
      ,D.[Q_UNID_CUMPLIDAS]
      ,D.[Q_PESO_CUMPLIDO]
      ,D.[M_CUMPLIDA_PARCIAL]
      ,D.[C_USUARIO_CUMPLIO_PARCIAL]
      ,D.[F_CUMPLIDA_PARCIAL]
      ,D.[U_PISO_PALETIZADO_OC]
      ,D.[U_ALTURA_PALETIZADO_OC]
  FROM [DIARCOP001-BK].[DiarcoP].[dbo].[T080_OC_CABE] C
  FULL OUTER JOIN [DIARCOP001-BK].[DiarcoP].[dbo].[T081_OC_DETA] D
  ON C.[C_OC] = D.[C_OC]
	AND C.[U_PREFIJO_OC] = D.[U_PREFIJO_OC]
	AND C.[U_SUFIJO_OC] = D.[U_SUFIJO_OC]


  WHERE [C_SITUAC] = 1
	AND [F_EMISION] > '20241101'
  ORDER BY [F_EMISION] 
GO


