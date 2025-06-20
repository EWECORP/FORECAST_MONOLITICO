USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_KIKKER_M_94_ALTERNATIVOS]    Script Date: 19/06/2025 15:43:38 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[SP_KIKKER_M_94_ALTERNATIVOS]
AS 
/*
USE DIARCOP
ID
DC_NOMBRE
Trae todas las tiendas abiertas y cerradas. No mantienen el maestro de tiendas
*/
begin 
SET NOCOUNT ON;
	select 'COD_PRD','COD_PROD_ALTERNATIVOS' union all
	select cast(C_ARTICULO as varchar(10)),
	cast(C_ARTICULO_ALTERNATIVO as varchar(10)) 
	from  [DIARCOP001].[DiarcoP].[dbo].T050_ARTICULOS 
	where M_BAJA='N' and C_ARTICULO_ALTERNATIVO <>0;
end;
GO


