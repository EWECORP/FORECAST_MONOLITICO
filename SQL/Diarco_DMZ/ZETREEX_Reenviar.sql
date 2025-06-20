USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_ZEETREX_REENVIAR]    ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		EDUARDO ETTLIN
-- Create date: 2024-09-23
-- Description:	Reenviar Archivos Faltantes
-- =============================================
CREATE   PROCEDURE [dbo].[SP_ZEETREX_REENVIAR]  @dia varchar(8)=null,@interfaz varchar(50)
BEGIN
	-- Variables globales
	DECLARE	
			@return_value int, 
			@contador_exec int = 0,
			@return_export int,
			@dia varchar(8) = FORMAT(GETDATE(),'yyyyMMdd'),
			@dia_ant varchar(8) = FORMAT(GETDATE()-1,'yyyyMMdd') ,
			@dia_ant_dif varchar(8) = FORMAT(GETDATE()-3,'yyyyMMdd') ,
			@comando varchar(4000),
			@contador_interfaces_no_generadas INT = 0,
			@interfaces_no_generadas  varchar(4000) = '',
			@msg varchar(4000)

	-- Variables del cursor
	DECLARE 
			@nombre_interfaz varchar(128),
			@sp_nombre varchar(128),
			@prefix varchar(50),
			@suffix varchar(50),
			@custom_basename varchar(50)

	SET NOCOUNT ON;
 
	--ejemplos:
	--set @interfaz = 'mercadologico'
	--set @sp_nombre = 'SP_KIKKER_M_1_MERCADOLOGICO'
	--set @dia = FORMAT(GETDATE(),'yyyyMMdd');
	--set @prefix = @dia

	-- VALORES
	VALUES
		('Estadistica','SP_ZEETREX_T_710_ESTADIS',NULL,NULL,NULL),
		('Estadistica_Detalle','SP_ZEETREX_T_710_ESTADIS_DETALLE',NULL,NULL,NULL),
		('Estadistica_Reposicion','SP_ZEETREX_T_710_ESTADIS_REPOSICION',NULL,NULL,NULL)


	set @nombre_interfaz='Estadistica_Reposicion'
	set @sp_nombre='SP_ZEETREX_T_710_ESTADIS_REPOSICION'
	set @dia = FORMAT(GETDATE()-1,'yyyyMMdd')


	declare @comando varchar(4000);

	set @comando='cd E:\AutoKikker\ && E:\AutoKikker\kikker_exporter.exe exporter -i '+ @interfaz + ' -t StoredProcedure -e local -o '+ @sp_nombre+ ' --log-path E:\AutoKikker\'+@dia+'\log -l E:\AutoKikker\'+@dia+ '\interfaces --no-compress --default-config-yml E:\AutoKikker\config.yml --config-key E:\AutoKikker\.config.key';

	if (@prefix is not null)
	begin 
		set @comando = @comando + ' --prefix ' + @prefix
	end;

	exec @return_export = xp_cmdshell @comando;

END
GO


