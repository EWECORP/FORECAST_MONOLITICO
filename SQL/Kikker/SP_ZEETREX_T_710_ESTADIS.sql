USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_ZEETREX_T_710_ESTADIS]    Script Date: 19/06/2025 15:54:58 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE [dbo].[SP_ZEETREX_T_710_ESTADIS]
AS 
/*
Genera HEADER con los campos
Transforma en STRING todos los campos
Trae Detalle de Ventas de Artículo ultimos 30 dias
*/
begin 
SET NOCOUNT ON;

DECLARE @dia_ant varchar(10) = FORMAT(GETDATE()-1,'yyyy-MM-dd') 

	SELECT 'C_SUCU_EMPR','C_ARTICULO','FECHA','Q_DIA1','Q_DIA2','Q_DIA3','Q_DIA4','Q_DIA5','Q_DIA6','Q_DIA7','Q_DIA8','Q_DIA9','Q_DIA10','Q_DIA11','Q_DIA12','Q_DIA13','Q_DIA14',
	'Q_DIA15','Q_DIA16','Q_DIA17','Q_DIA18','Q_DIA19','Q_DIA20','Q_DIA21','Q_DIA22','Q_DIA23','Q_DIA24','Q_DIA25','Q_DIA26','Q_DIA27','Q_DIA28','Q_DIA29','Q_DIA30','Q_DIA31'
	UNION ALL
	
	select CONVERT(varchar,[C_SUCU_EMPR])
      ,CONVERT(varchar,[C_ARTICULO])
	  ,@dia_ant
      ,CONVERT(varchar,[Q_DIA1])
      ,CONVERT(varchar,[Q_DIA2])
      ,CONVERT(varchar,[Q_DIA3])
      ,CONVERT(varchar,[Q_DIA4])
      ,CONVERT(varchar,[Q_DIA5])
      ,CONVERT(varchar,[Q_DIA6])
      ,CONVERT(varchar,[Q_DIA7])
      ,CONVERT(varchar,[Q_DIA8])
      ,CONVERT(varchar,[Q_DIA9])
      ,CONVERT(varchar,[Q_DIA10])
      ,CONVERT(varchar,[Q_DIA11])
      ,CONVERT(varchar,[Q_DIA12])
      ,CONVERT(varchar,[Q_DIA13])
      ,CONVERT(varchar,[Q_DIA14])
      ,CONVERT(varchar,[Q_DIA15])
      ,CONVERT(varchar,[Q_DIA16])
      ,CONVERT(varchar,[Q_DIA17])
      ,CONVERT(varchar,[Q_DIA18])
      ,CONVERT(varchar,[Q_DIA19])
      ,CONVERT(varchar,[Q_DIA20])
      ,CONVERT(varchar,[Q_DIA21])
      ,CONVERT(varchar,[Q_DIA22])
      ,CONVERT(varchar,[Q_DIA23])
      ,CONVERT(varchar,[Q_DIA24])
      ,CONVERT(varchar,[Q_DIA25])
      ,CONVERT(varchar,[Q_DIA26])
      ,CONVERT(varchar,[Q_DIA27])
      ,CONVERT(varchar,[Q_DIA28])
      ,CONVERT(varchar,[Q_DIA29])
      ,CONVERT(varchar,[Q_DIA30])
      ,CONVERT(varchar,[Q_DIA31])
  FROM [DIARCOP001].[DiarcoP].[dbo].[T710_ESTADIS]
end;
	
GO


