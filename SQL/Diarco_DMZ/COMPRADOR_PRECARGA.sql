/*****
Final del Proceso cargar campos definidos
****/

SELECT [C_PROVEEDOR]
      ,[C_ARTICULO]
      ,[C_SUCU_EMPR]
      ,[Q_BULTOS_KILOS_DIARCO]
      ,[F_ALTA_SIST]
	  ,[C_USUARIO_MODIF]
      ,[C_COMPRADOR]
	  ,[C_COMPRA_KIKKER]
      --,[F_GENERO_OC]
     -- ,[C_USUARIO_BLOQUEO]
     -- ,[M_PROCESADO]
     -- ,[F_PROCESADO]
    --  ,[U_PREFIJO_OC]
	--,[U_SUFIJO_OC]
	     --,[C_USUARIO_GENERO_OC]
     --,[C_TERMINAL_GENERO_OC]


  FROM [DIARCOP001].[DiarcoP].[dbo].[T080_OC_PRECARGA_KIKKER]
GO
-- Precio de Lista
-- Precio de Compra
-- Numeración Lista de Precios.
-- Fecha Vigencia Lista

OJO Artículo Pesables:  (Promedio Kg por Pieza / Caja)
OJO Bultos Diarco tiene que ser múltiplos del Bulto Proveedor.

Hernan: Precio Lista Calculado --->  (VIGENTES) (TABLA 116) COD_DESC_COMPRAS
Tipo de Descuento 105 --> Descuento Factura
			      108 ---> Descuento Pronto Pago
				  102 ---> Nota Credito

SELECT Top 100 [C_PROVEEDOR]      ,[C_ARTICULO]      ,[C_SUCU_EMPR]      ,[I_LISTA_CALCULADO]
      ,[I_COSTO_BASE]      ,[K_IMP_INTERNOS]      ,[I_COSTO_ENVASE]      ,[I_COSTO_OTROS]
      ,[I_COSTO_EFECTIVO]      ,[C_COSTO_IVA]      ,[F_COSTO_ULTIMO_CBIO_PRECIO_BASE]
      ,[C_USUARIO_ULTIMO_CBIO_PRECIO_BASE]
      ,[C_DTO1_COMP]      ,[K_DTO1_COMP]      ,[C_DTO2_COMP]      ,[K_DTO2_COMP]
      ,[C_DTO3_COMP]      ,[K_DTO3_COMP]      ,[C_DTO4_COMP]      ,[K_DTO4_COMP]
      ,[C_DTO5_COMP]      ,[K_DTO5_COMP]      ,[C_DTO6_COMP]      ,[K_DTO6_COMP]
      ,[C_DTO7_COMP]      ,[K_DTO7_COMP]      ,[C_DTO8_COMP]      ,[K_DTO8_COMP]
      ,[C_DTO9_COMP]      ,[K_DTO9_COMP]      ,[C_DTO10_COMP]     ,[K_DTO10_COMP]
	  ,[C_DOC_ULT_ING]    ,[U_PREFIJO_DOC_ULT_ING]      ,[U_SUFIJO_DOC_ULT_ING]
      ,[F_DOC_ULT_ING]    ,[Q_ULT_ING]      ,[Q_PESO_ULT_ING]      ,[C_OC_ULT_COMP]
      ,[U_PREFIJO_OC_ULT_COMP]      ,[U_SUFIJO_OC_ULT_COMP]      ,[I_PRECIO_OC_ULT_COMP]
      ,[I_PRECIO_PARTE_OC_ULT_COMP]      ,[F_EMISION_OC_ULT_COMP]      ,[Q_DIAS_STOCK]
      ,[C_DTO1_ACCION]      ,[K_DTO1_ACCION]      ,[Q_DIAS_SOBRE_STOCK]
  FROM [DIARCOP001].[DiarcoP].[dbo].[T055_ARTICULOS_CONDCOMPRA_COSTOS]
WHERE [C_ARTICULO] = 71173
      AND [Q_DIAS_SOBRE_STOCK] > 0


SELECT TOP 100 * FROM [DIARCOP001].[DiarcoP].[dbo].[T710_ESTADIS_REPOSICION]
