USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_KIKKER_M_93_SUSTITUTOS]    Script Date: 19/06/2025 15:43:19 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[SP_KIKKER_M_93_SUSTITUTOS]
AS 
/*
USE DIARCOP
ID
DC_NOMBRE
Trae todas las tiendas abiertas y cerradas. No mantienen el maestro de tiendas
*/
begin 
SET NOCOUNT ON;
	select 'COD_PRD','COD_PROD_SUSTITUTO' union all
	select cast(C_ARTICULO as varchar(10)),
	cast(C_ARTICULO_SUSTITUTO as varchar(10)) 
	from  [DIARCOP001].[DiarcoP].[dbo].T050_ARTICULOS_SUSTITUTOS

end;
	
	
GO


