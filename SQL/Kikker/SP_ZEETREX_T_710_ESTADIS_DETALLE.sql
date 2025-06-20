USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_ZEETREX_T_710_ESTADIS_DETALLE]    Script Date: 19/06/2025 15:55:18 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[SP_ZEETREX_T_710_ESTADIS_DETALLE]
AS 
/*
Genera HEADER con los campos
Transforma en STRING todos los campos
Trae la estadísticas de ventas diaria por sucursal (LOCAL/PLU/FECHA) 1,5 M de registros
*/
BEGIN 
	SET NOCOUNT ON;

	DECLARE @Fecha_Ant DATETIME;
	SET @Fecha_Ant = CAST(DATEADD(DAY, -1, GETDATE()) AS DATE);

	SELECT 'F_DIA','C_SUCU_EMPR','C_ARTICULO','M_DOMINGO','M_FOLDER','M_OFERTA','Q_VENTA','Q_STOCK','M_SEPA'
	UNION ALL
	
	SELECT CONVERT(varchar,[F_DIA],121) 
      ,CONVERT(varchar,[C_SUCU_EMPR])
      ,CONVERT(varchar,[C_ARTICULO])
      ,CONVERT(varchar,[M_DOMINGO])
      ,CONVERT(varchar,[M_FOLDER])
      ,CONVERT(varchar,[M_OFERTA])
      ,CONVERT(varchar,[Q_VENTA])
      ,CONVERT(varchar,[Q_STOCK])
      ,CONVERT(varchar,[M_SEPA])
	  FROM [DIARCOP001].[DiarcoP].[dbo].[T710_ESTADIS_DETALLE]
	  WHERE [F_DIA] = @Fecha_Ant
END;
		
GO


