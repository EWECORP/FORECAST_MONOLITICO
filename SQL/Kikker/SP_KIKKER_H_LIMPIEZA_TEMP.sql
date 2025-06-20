USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_KIKKER_H_LIMPIEZA_TEMP]    Script Date: 19/06/2025 15:36:49 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[SP_KIKKER_H_LIMPIEZA_TEMP] 
AS
BEGIN
	SET NOCOUNT ON;
	
IF OBJECT_ID('tempdb..##stock_historicos') IS NOT NULL  
        BEGIN
         drop table ##stock_historicos
        END;
IF OBJECT_ID('tempdb..##precios_historicos') IS NOT NULL  
        BEGIN
         drop table  ##precios_historicos
        END;
IF OBJECT_ID('tempdb..##costos_historicos') IS NOT NULL   
        BEGIN
         drop table  ##costos_historicos
        END;
IF OBJECT_ID('tempdb..##vigencia_promos') IS NOT NULL   
        BEGIN
         drop table ##vigencia_promos
		END;
IF OBJECT_ID('tempdb..##surtido') IS NOT NULL   
        BEGIN
         drop table ##surtido
		END;

END
GO


