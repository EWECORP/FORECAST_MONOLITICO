SELECT [C_DOC]
      ,[U_DOC_PREFIJO]
      ,[U_DOC_SUFIJO]
      ,[C_DOC_LETRA]

      ,[C_SUCU_ORIG_ALTA]
      ,[F_ALTA_SIST]
      ,[C_SITUAC]
      ,[F_SITUAC]
    
      ,[C_DOC_ORIG_POR]
      ,[C_DOC_LETRA_ORIG_POR]
      ,[U_DOC_PREFIJO_ORIG_POR]
      ,[U_DOC_SUFIJO_ORIG_POR]
      ,[C_DOC_APLIC]
      ,[U_DOC_PREFIJO_APLIC]
      ,[U_DOC_SUFIJO_APLIC]
      ,[C_USUARIO_APLIC]
   
      ,[I_TOTAL]
      ,[I_TOTAL_ORIGINAL]
      ,[I_NETO_1]
      ,[I_NETO_1_ORIGINAL]
      ,[C_CLIENTE]
      ,[N_CLIENTE]
      ,[C_CUIT_CLIENTE]
      ,[N_DIRECCION_CLIENTE]
   
      ,[F_IMPRE]

      ,[D_OBSERVACION]
  
  FROM [DIARCOP001].[DiarcoP].[dbo].[T231_COMPROBANTES_CABE_HIST]
WHERE [C_PROVEEDOR] <> 0


