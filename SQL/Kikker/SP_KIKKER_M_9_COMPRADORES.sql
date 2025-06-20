USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_KIKKER_M_9_COMPRADORES]    Script Date: 19/06/2025 15:39:35 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE procedure [dbo].[SP_KIKKER_M_9_COMPRADORES]
AS
BEGIN
/*
USE DIARCOP
COD_COMPRADOR: Código Comprador
Nome_Comprador:Nombre Comprador
*/

SET NOCOUNT ON;
select 'COD_COMPRADOR','NOME_COMPRADOR'
union all
SELECT '0','SIN COMPRADOR'
union all
SELECT CONVERT(VARCHAR,c_comprador), DBO.[NORMALIZA_STRING]( n_comprador) 
FROM [DIARCOP001].[DiarcoP].dbo.T117_COMPRADORES WHERE M_BAJA = 'N';
END;
GO


