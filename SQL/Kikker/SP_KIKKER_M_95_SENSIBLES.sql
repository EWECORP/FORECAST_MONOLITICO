USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_KIKKER_M_95_SENSIBLES]    Script Date: 19/06/2025 15:46:21 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[SP_KIKKER_M_95_SENSIBLES]
AS 
/*
USE DIARCOP
ID
DC_NOMBRE
Trae todas las tiendas abiertas y cerradas. No mantienen el maestro de tiendas
*/
begin 
SET NOCOUNT ON;
	select 'COD_PRD' union all
	select cast(C_ARTICULO as varchar(10)) from  [DIARCOP001].[DiarcoP].[dbo].T050_ARTICULOS where C_CLASIFICACION_COMPRA in (1,6) and M_BAJA='N'
end;
	

/*SENSIBLES:
select * from [DIARCOP001].[DiarcoP].[dbo].T001_tabla_Codigo where c_tabla=119
*/
GO


