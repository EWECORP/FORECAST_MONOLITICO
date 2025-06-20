USE [data-sync]
GO

DECLARE	@return_value int
DECLARE @parametro datetime = N'2024-12-31'

EXEC	@return_value = [dbo].[SP_CNX_T_OC_CABECERA]
		@FechaDesde = @parametro
SELECT	'Return Value' = @return_value

EXEC	@return_value = [dbo].[SP_CNX_T_OC_DETALLE]
		@FechaDesde = @parametro
SELECT	'Return Value' = @return_value

EXEC	@return_value =  [dbo].[SP_CNX_T_OC_DETALLE_EXT]
		@FechaDesde = @parametro
SELECT	'Return Value' = @return_value

EXEC	@return_value = [dbo].[SP_CNX_T_RECUPERO_PROVEEDORES]
		@FechaDesde = @parametro
SELECT	'Return Value' = @return_value

EXEC	@return_value = [dbo].[SP_CNX_T_COMPETENCIA_DETALLE]
		@FechaDesde = @parametro
SELECT	'Return Value' = @return_value

GO