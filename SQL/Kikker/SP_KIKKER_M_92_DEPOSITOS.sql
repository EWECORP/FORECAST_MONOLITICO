USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_KIKKER_M_92_DEPOSITOS]    Script Date: 19/06/2025 15:42:58 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[SP_KIKKER_M_92_DEPOSITOS]
AS 
/*
USE DIARCOP
ID
DC_NOMBRE
Trae todas las tiendas abiertas y cerradas. No mantienen el maestro de tiendas
*/
begin 
SET NOCOUNT ON;
	select 'ID','DC_NOMBRE' union all
	select 	case DBO.[NORMALIZA_STRING](C_SUCU_EMPR) when 41 then '41CD' else DBO.[NORMALIZA_STRING](C_SUCU_EMPR) end,
		  dbo.[NORMALIZA_STRING](N_SUCURSAL) 
	from [DIARCOP001].[DIARCOP].dbo.T100_EMPRESA_SUC 
	where C_SUCU_EMPR IN (41,82)
end;
	
	
GO


