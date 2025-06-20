USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[ZEETREX_PROCESO_INFO]    Script Date: 19/06/2025 16:04:41 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


--ZXEETREX PROCESO_INFO '09/09/2024 - COMPRESIÓN y ENVIO por SFTP'
CREATE   PROCEDURE [dbo].[ZEETREX_PROCESO_INFO]  
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
	
	-- Ejecución del comando de compresión
	EXEC @Result = master..xp_cmdshell @Comando
	
	-- Manejo de error en la ejecución del comando
	IF @Result <> 0
	BEGIN
		RAISERROR('Error al ejecutar el comando de compresión.', 16, 1);
		RETURN;
	END
	
END
GO


