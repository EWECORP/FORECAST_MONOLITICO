USE [data-sync]
GO

/****** Object:  Table [dbo].[T_RECUPERO_PROVEEDORES]    Script Date: 28/01/2025 8:54:46 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[T_RECUPERO_PROVEEDORES]') AND type in (N'U'))
DROP TABLE [dbo].[T_RECUPERO_PROVEEDORES]
GO

--- Toma de la Tabla de Comprobantes que GENERA DIARCO --> Notas de Débito o Facturas a los Proveedores.
SELECT [C_CLIENTE]
      ,[F_ALTA_SIST]
	  ,[N_CLIENTE]
    
      ,[C_DOC]
      ,[U_DOC_PREFIJO]
      ,[U_DOC_SUFIJO]
      ,[C_DOC_LETRA]
	  ,[C_SUCU_ORIG_ALTA]
	  ,[C_USUARIO_APLIC]
   
      ,[I_TOTAL]
      ,[I_TOTAL_ORIGINAL]
      ,[I_NETO_1]
      ,[I_NETO_1_ORIGINAL]   
      ,[F_IMPRE]
	  ,[D_OBSERVACION]
	  ,'de T231_COMPROBANTES_CABE' AS ORIGEN
	  ,1 AS FLAG_1
	  ,0 AS FLAG_2
	  ,0 AS FLAG_3
  
INTO T_RECUPERO_PROVEEDORES
 -- SELECT *
  FROM [DIARCOP001].[DiarcoP].[dbo].[T231_COMPROBANTES_CABE_HIST]
WHERE [C_PROVEEDOR] <> 0