USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[ZEETREX_ENVIO_FTP]    Script Date: 12/09/2024 17:37:51 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


--ZXEETREX PROCESO ENVIO por SFTP'
CREATE   PROCEDURE [dbo].[ZEETREX_ENVIO_FTP]  
	@F_PROCESO AS DATE
	
AS
BEGIN
	SET NOCOUNT ON;

	-- Declaración de variables
	declare @NombreArchivo varchar(500)
	declare @Comando VARCHAR(4000)
	declare @origen varchar(500)
	declare @destino varchar(500)
	declare @Result INT
	declare @ftpCommand VARCHAR(4000)
	
	-- Composición de rutas
	set @origen = 'E:\AutoKikker\' + convert(varchar(8), @F_PROCESO, 112) + '\interfaces'
	SET @destino = 'D:\ZEETREX\INTEGRACION-' + convert(varchar(8), @F_PROCESO, 112) + '.ZIP'

	
	-- Comando para enviar el archivo por SFTP utilizando pscp
	SET @ftpCommand = '"C:\Program Files\PuTTY\pscp.exe" -sftp -pw diarco2024 D:\ZEETREX\INTEGRACION-' + @destino + ' usr_diarco@140.99.164.229:/archivos/usr_diarco/'

	-- Ejecución del comando FTP
	EXEC @Result = master..xp_cmdshell @ftpCommand

	-- pscp.exe" -sftp -pw diarco2024 D:\ZEETREX\INTEGRACION-20240912.ZIP  usr_diarco@140.99.164.229:/archivos/usr_diarco/'

	-- Verificación de éxito o error en el envío
	IF @Result <> 0
	BEGIN
		RAISERROR('Error al enviar el archivo por SFTP.', 16, 1);
		RETURN;
	END
	
END
GO


