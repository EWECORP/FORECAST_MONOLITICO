USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_KIKKER_M_91_LOJAS]    Script Date: 19/06/2025 15:42:34 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE [dbo].[SP_KIKKER_M_91_LOJAS]
AS 
/*
USE DIARCOP
ID_TIENDA
NOMBRE
Trae todas las tiendas abiertas y cerradas. No mantienen el maestro de tiendas
Nota: Kikker nos pide agregar las tiendas 41 y 82 13/09
*/


begin 
SET NOCOUNT ON;
	SELECT  'ID','SUC_NOMBRE'
	UNION ALL

	SELECT  case C_SUCU_EMPR when 41 then '41CD' else DBO.[NORMALIZA_STRING](C_SUCU_EMPR) end,
    DBO.[NORMALIZA_STRING](N_SUCURSAL)
	FROM     [DIARCOP001].[DIARCOP].DBO.T100_EMPRESA_SUC 
	WHERE	    C_SUCU_EMPR NOT IN (6,8,14,17,39,40,300, 80, 81, 83, 84, 88)
	AND      M_SUCU_VIRTUAL = 'N'
	AND      C_SUCU_EMPR NOT IN (SELECT C_SUCU_EMPR FROM [DIARCOP001].[DiarcoP].[dbo].[T900_SUCURSALES_EXCLUIDAS_GERENCIA_DB]) --- esto excluye los barrios cerrados
--	ORDER BY 1
end;
	
	
GO


