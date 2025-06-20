USE [data-sync]
GO

DECLARE	@return_value int

EXEC	@return_value = [dbo].[SP_CNX_T_OC_DETALLE_EXT]
		@FechaDesde = N'20241031'

SELECT	'Return Value' = @return_value

GO
