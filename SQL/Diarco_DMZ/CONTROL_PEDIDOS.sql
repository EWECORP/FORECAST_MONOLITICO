SELECT TOP (1000) A.[C_ARTICULO]

	  ,S.[C_SUCU_EMPR]    
      ,S.[M_OFERTA_SUCU]
      ,S.[M_HABILITADO_SUCU]
      ,A.[C_FAMILIA]
    
      ,A.[C_PROVEEDOR_PRIMARIO]
      ,A.[F_ALTA]
      ,A.[N_ARTICULO]
      ,A.[N_ARTICULO_FACT]
    
      ,A.[M_VENDE_POR_PESO]

      ,A.[M_PROMOCION]
      ,A.[M_IMPORTADO]
      ,A.[M_A_DAR_DE_BAJA]
 
      ,A.[M_BAJA]
      ,A.[F_BAJA]
 
      ,A.[C_COMPRADOR]

      ,A.[OK_COMPRA]
      ,A.[M_TOP]
      ,A.[M_VARIEDAD_EXTRA]
      ,A.[M_SIN_VENTA]
      ,A.[M_RESTO_TOP]
      ,A.[C_CLASIFICACION_COMPRA]
 
      ,A.[F_MODIF]
      ,A.[C_USUARIO_MODIF]
  
  FROM [data-sync].[repl].[T050_ARTICULOS] A
  FULL JOIN [data-sync].[repl].[T051_ARTICULOS_SUCURSAL] S
  
  ON A.C_ARTICULO = S.C_ARTICULO
  WHERE A.C_PROVEEDOR_PRIMARIO = 4565
       AND A.M_A_DAR_DE_BAJA ='N'
	   AND A.M_BAJA ='N'
	   AND S.M_HABILITADO_SUCU ='S'

	order by S.C_SUCU_EMPR


