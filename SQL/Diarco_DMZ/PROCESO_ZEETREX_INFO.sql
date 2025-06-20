USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[PROCESO_ZEETREX_INFO]    Script Date: 11/09/2024 13:11:18 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


--PROCESO_INFO '09/09/2024'
CREATE PROCEDURE [dbo].[PROCESO_ZEETREX_INFO]  
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
	SET @Comando = '"C:\Program Files\7-Zip\7z.EXE" a -tzip ' + @destino + ' ' + @origen + '\*.* -mx9'
	
	-- Validación de existencia de directorio
	IF NOT EXISTS (SELECT * FROM sys.master_files WHERE physical_name LIKE @origen + '\%')
	BEGIN
		RAISERROR('El directorio de origen no existe.', 16, 1);
		RETURN;
	END
	
	-- Ejecución del comando de compresión
	EXEC @Result = master..xp_cmdshell @Comando
	
	-- Manejo de error en la ejecución del comando
	IF @Result <> 0
	BEGIN
		RAISERROR('Error al ejecutar el comando de compresión.', 16, 1);
		RETURN;
	END

	-- Comando para enviar el archivo por SFTP utilizando pscp
	SET @ftpCommand = '"C:\Program Files\PuTTY\pscp.exe" -sftp -pw diarco2024 ' + @destino + ' usr_diarco@140.99.164.229:/archivos/usr_diarco/'

	-- Ejecución del comando FTP
	EXEC @Result = master..xp_cmdshell @ftpCommand

	-- Verificación de éxito o error en el envío
	IF @Result <> 0
	BEGIN
		RAISERROR('Error al enviar el archivo por SFTP.', 16, 1);
		RETURN;
	END
	
END
GO


