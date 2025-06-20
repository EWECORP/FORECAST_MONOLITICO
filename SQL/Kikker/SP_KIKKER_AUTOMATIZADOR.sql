USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_KIKKER_AUTOMATIZADOR]    Script Date: 19/06/2025 15:31:28 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[SP_KIKKER_AUTOMATIZADOR] @dia varchar(8)=null,@interfaz varchar(50), @sp_nombre varchar(50), @prefix varchar(50) = null, @sufix varchar(50) = null, @custom_basename varchar(128) = null, @return_export int OUTPUT
AS
BEGIN
	SET NOCOUNT ON;

 
--ejemplos:
--set @interfaz = 'mercadologico'
--set @sp_nombre = 'SP_KIKKER_M_1_MERCADOLOGICO'
--set @dia = FORMAT(GETDATE(),'yyyyMMdd');

--set @prefix = @dia

declare @comando varchar(4000);

set @comando='cd E:\AutoKikker\ && E:\AutoKikker\kikker_exporter.exe exporter -i '+ @interfaz + ' -t StoredProcedure -e local -o '+ @sp_nombre+ ' --log-path E:\AutoKikker\'+@dia+'\log -l E:\AutoKikker\'+@dia+ '\interfaces --no-compress --default-config-yml E:\AutoKikker\config.yml --config-key E:\AutoKikker\.config.key';

if (@prefix is not null)
begin 
	set @comando = @comando + ' --prefix ' + @prefix
end;

exec @return_export = xp_cmdshell @comando;

END
GO


