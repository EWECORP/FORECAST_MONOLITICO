USE [data-sync]
GO

TRUNCATE TABLE [dbo].[T114_RUBROS]
GO


INSERT INTO [dbo].[T114_RUBROS]
           ([C_RUBRO]
           ,[D_RUBRO]
           ,[C_RUBRO_PADRE]
           ,[C_RUBRO_NIVEL]
           ,[F_ALTA]
           ,[C_USUARIO_ALTA]
           ,[C_TERMINAL_ALTA]
           ,[F_BAJA]
           ,[C_USUARIO_BAJA]
           ,[C_TERMINAL_BAJA]
           ,[M_BAJA]
           ,[M_EXCLUIDA_EN_VALORIZ])

 SELECT [C_RUBRO]
      ,[D_RUBRO]
      ,[C_RUBRO_PADRE]
      ,[C_RUBRO_NIVEL]
      ,[F_ALTA]
      ,[C_USUARIO_ALTA]
      ,[C_TERMINAL_ALTA]
      ,[F_BAJA]
      ,[C_USUARIO_BAJA]
      ,[C_TERMINAL_BAJA]
      ,[M_BAJA]
      ,[M_EXCLUIDA_EN_VALORIZ]
  FROM [DIARCOP001].[DiarcoP].[dbo].[T114_RUBROS]
GO


