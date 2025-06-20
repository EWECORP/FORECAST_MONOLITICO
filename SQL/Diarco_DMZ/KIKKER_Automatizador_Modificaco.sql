USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_KIKKER_AUTOMATIZADOR_SECUENCIA]    Script Date: 10/09/2024 17:55:25 ******/
DROP PROCEDURE [dbo].[SP_KIKKER_AUTOMATIZADOR_SECUENCIA]
GO

/****** Object:  StoredProcedure [dbo].[SP_KIKKER_AUTOMATIZADOR_SECUENCIA]    Script Date: 10/09/2024 17:55:25 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE   PROCEDURE [dbo].[SP_KIKKER_AUTOMATIZADOR_SECUENCIA]
AS
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

-- Variables de cursor

DECLARE 
        @nombre_interfaz varchar(128),
        @sp_nombre varchar(128),
        @prefix varchar(50),
        @suffix varchar(50),
        @custom_basename varchar(50)

-- Generación de Interfaces

CREATE TABLE #TMPInterfaces(
    nombre_interfaz varchar(128),
    sp_nombre varchar(50),
    prefix varchar(50),
    suffix varchar(50),
    custom_basename varchar(50)
)

INSERT INTO #TMPInterfaces (nombre_interfaz,sp_nombre,prefix,suffix,custom_basename)
VALUES('mercadologico','SP_KIKKER_M_1_MERCADOLOGICO',NULL,NULL,NULL),
('produtos','SP_KIKKER_M_3_PRODUCTOS',NULL,NULL,NULL),
('fornecedor','SP_KIKKER_M_10_FORNECEDORES',NULL,NULL,NULL),
('tipo_movimento','SP_KIKKER_M_2_TIPO_MOVIMIENTO',NULL,NULL,NULL),
('familias','SP_KIKKER_M_3_1_FAMILIA',NULL,NULL,NULL),
('familia_produto','SP_KIKKER_M_3_2_FAMILIA_PRODUCTO',NULL,NULL,NULL),
('compradores','SP_KIKKER_M_9_COMPRADORES',NULL,NULL,NULL),
('lojas','SP_KIKKER_M_91_LOJAS',NULL,NULL,NULL),
('depositos','SP_KIKKER_M_92_DEPOSITOS',NULL,NULL,NULL),
('sustitutos','SP_KIKKER_M_93_SUSTITUTOS',NULL,NULL,NULL),
('alternativos','SP_KIKKER_M_94_ALTERNATIVOS',NULL,NULL,NULL),
('sensibles','SP_KIKKER_M_95_SENSIBLES',NULL,NULL,NULL),
('Stock_Seguridad','SP_KIKKER_M_96_STOCK_SEGURIDAD',NULL,NULL,NULL),
('promocoes','SP_KIKKER_T_11_1_PROMOCIONES',NULL,NULL,NULL),
('promocoes_produto','SP_KIKKER_T_11_2_PROMOCIONES_PRODUCTO',NULL,NULL,NULL),
('tabela_preco','SP_KIKKER_T_11_PRECIOS',NULL,NULL,NULL),
('produtos_loja','SP_KIKKER_T_4_PRODUCTOS_LOJA',NULL,NULL,NULL),
('movimento','SP_KIKKER_T_5_MOVIMIENTOS_DIARIOS',@dia_ant,NULL,NULL),
('movimento','SP_KIKKER_T_5_MOVIMIENTOS_DIARIOS_DIF_X_DIAS',@dia_ant_dif,NULL,NULL),
('pedidos_pendentes','SP_KIKKER_T_7_PEDIDOS_PENDIENTES',NULL,NULL,NULL),
('estoque','SP_KIKKER_T_8_STOCK',@dia_ant,NULL,NULL),
('Precios_Productos_Tienda','SP_KIKKER_T_87_PRECIOS_PRODUCTOS_TIENDA',NULL,NULL,NULL),
('Estadistica','SP_KIKKER_T_710_ESTADIS',NULL,NULL,NULL),
('Estadistica_Detalle','SP_KIKKER_T_710_ESTADIS_DETALLE',@dia_ant,NULL,NULL),
('Estadistica_Reposicion','SP_KIKKER_T_710_ESTADIS_REPOSICION',NULL,NULL,NULL)

DECLARE interfaz_cursor CURSOR FOR
SELECT 
        nombre_interfaz,
        sp_nombre,
        prefix,
        suffix,
        custom_basename
    FROM #TMPInterfaces

OPEN interfaz_cursor
FETCH NEXT FROM interfaz_cursor 
INTO @nombre_interfaz, @sp_nombre, @prefix, @suffix, @custom_basename

WHILE @@FETCH_STATUS = 0  
    BEGIN

    /*
    Modo de uso:

    SP_KIKKER_AUTOMATIZADOR 
                            @dia varchar(8)=null,
                            @interfaz varchar(50), 
                            @sp_nombre varchar(50), 
                            @prefix varchar(50) = null, 
                            @sufix varchar(50) = null, 
                            @custom_basename varchar(128) = null,
                            @return_export int OUTPUT
    */

    EXEC dbo.SP_KIKKER_AUTOMATIZADOR 
            @dia = @dia, 
            @interfaz = @nombre_interfaz, 
            @sp_nombre = @sp_nombre, 
            @prefix = @prefix, 
            @sufix = @suffix, 
            @custom_basename = @custom_basename, 
            @return_export = @return_value OUTPUT
    SET @contador_exec = @contador_exec + @return_value

    IF @return_value <> 0
        BEGIN
            SET @contador_interfaces_no_generadas = @contador_interfaces_no_generadas + 1
            SET @interfaces_no_generadas = @interfaces_no_generadas + ', "' + @nombre_interfaz + '"'
        END

  --  PRINT(@contador_exec) -- DEBUG

    FETCH NEXT FROM interfaz_cursor 
    INTO @nombre_interfaz, @sp_nombre, @prefix, @suffix, @custom_basename
END

CLOSE interfaz_cursor  
DEALLOCATE interfaz_cursor 

-- Compresión y envio a Kikker

--IF @contador_exec = 0
--    BEGIN
--		SET @comando ='cd E:\AutoKikker\ && E:\AutoKikker\kikker_exporter.exe sender -i Interfaces_Diarias_'+ @dia + ' -d E:\AutoKikker\'+ @dia + '\interfaces --sftp-directory /home/diarco/kikker_processar_ontem --custom-basename '+ @dia_ant +' --log-path E:\AutoKikker\'+@dia+'\log  --default-config-yml E:\AutoKikker\config.yml --config-key E:\AutoKikker\.config.key --ssh-host-keys-file E:\AutoKikker\ssh\known_hosts --compress-files'
--        EXEC @contador_exec = xp_cmdshell @comando
--			IF @contador_exec <> 0
--			 BEGIN
--				SET @msg= 'No se ejecutó correctamente el sender. Chequear logs en el servidor para más detalles'
--				RAISERROR(@msg, 11, 1)
--			 END
--    END
--ELSE
--    BEGIN
--		SET @msg= 'No se ejecutó el envio de las interfaces de Kikker. Cantidad de interfaces no generadas: ' + cast(@contador_interfaces_no_generadas as varchar) + '. Listado de interfaces no generadas: ' + @interfaces_no_generadas
--        RAISERROR(@msg, 11, 1)
--    END
END

GO


