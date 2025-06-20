USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_KIKKER_AUTOMATIZADOR_IMPORTADOR_KIKKER_TO_SGM]    Script Date: 19/06/2025 15:31:50 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[SP_KIKKER_AUTOMATIZADOR_IMPORTADOR_KIKKER_TO_SGM]
AS
/*
- Author: soporte@openbusiness.ar
- Description: Automatizador de importador de sugerencias de compras de Kikker al sistema interno de Diarco SGM
- Changes:
	- 15/02/2023 - Cambio de bloque de transacciones por conector al destino (DIARCOP001)
	- 06/07/2023 - Incorporación de Comprador desde Kikker a SGM (DIARCOP001)
*/

BEGIN
	SET NOCOUNT ON;
	
	-- Obtener de variables globales y ejecución de importador

   	DECLARE 
         	@comando varchar(4000),
         	@dia varchar(8) = FORMAT(GETDATE(),'yyyyMMdd'),
         	@kikker_exe_dir varchar(400) = 'E:\AutoKikker\',
		 	@c_compra_kikker int,
		 	@products int,
			@process_ts datetime = GETDATE();
	DECLARE @c_compra_kikker_tbl TABLE(
	[C_COMPRA_KIKKER] INT NOT NULL
	);
	
	DECLARE @order_code_kikker_tbl TABLE(
		[C_PROVEEDOR] [varchar](max) NULL,
		[C_ARTICULO] [varchar](max) NULL,
		[C_SUCU_EMPR] [varchar](max) NULL,
		[Q_BULTOS_KILOS_DIARCO] [float] NULL,
		[F_ALTA_SIST] [datetime] NULL,
		[C_USUARIO_GENERO_OC] [varchar](max) NULL,
		[C_TERMINAL_GENERO_OC] [varchar](max) NULL,
		[F_GENERO_OC] [varchar](max) NULL,
		[C_USUARIO_BLOQUEO] [varchar](max) NULL,
		[M_PROCESADO] [varchar](max) NULL,
		[F_PROCESADO] [date] NULL,
		[U_PREFIJO_OC] [bigint] NULL,
		[U_SUFIJO_OC] [bigint] NULL,
		[C_COMPRA_KIKKER] [varchar](max) NULL,
		[C_USUARIO_MODIF] [varchar](max) NULL,
		[C_COMPRADOR] [varchar](max) NULL,
		[HASH_R] [varbinary](20) NOT NULL
	);
	
	
   SET @comando='cd '+ @kikker_exe_dir + ' && '+ @kikker_exe_dir + 'kikker_exporter.exe importer -t "SQL Command" -f '+@kikker_exe_dir+'sql_file.sql --source-objectname delivery_order_items_to_erps --target-tablename delivery_order_items_to_erps --log-path '+ @kikker_exe_dir + @dia +'\log --default-config-yml '+ @kikker_exe_dir +'.config_importer.yml --config-key '+ @kikker_exe_dir +'.config_importer.key';

   EXEC xp_cmdshell @comando;

   -- Detectar novedades en tabla output para SGM

   	DECLARE order_code_cursor CURSOR FOR 
	SELECT DISTINCT order_package_code
		FROM [dbo].[delivery_order_items_to_erps]

	INSERT INTO @order_code_kikker_tbl([C_PROVEEDOR],[C_ARTICULO],[C_SUCU_EMPR],[Q_BULTOS_KILOS_DIARCO],[F_ALTA_SIST],[C_USUARIO_GENERO_OC],[C_TERMINAL_GENERO_OC],[F_GENERO_OC],[C_USUARIO_BLOQUEO],[M_PROCESADO],[F_PROCESADO],[U_PREFIJO_OC],[U_SUFIJO_OC],[C_COMPRA_KIKKER],[C_USUARIO_MODIF],[C_COMPRADOR],[HASH_R])
	SELECT
							[C_PROVEEDOR],
							[C_ARTICULO],
							[C_SUCU_EMPR],
							[Q_BULTOS_KILOS_DIARCO],
							[F_ALTA_SIST],
							[C_USUARIO_GENERO_OC],
							[C_TERMINAL_GENERO_OC],
							[F_GENERO_OC],
							[C_USUARIO_BLOQUEO],
							[M_PROCESADO],
							[F_PROCESADO],
							[U_PREFIJO_OC],
							[U_SUFIJO_OC],
							[C_COMPRA_KIKKER],
							[C_USUARIO_MODIF],
							[C_COMPRADOR],
							[HASH_R]
						FROM (
								SELECT 
				        			    *,
				        			    HASHBYTES('SHA1',
                        			                  CONCAT(
									                            COALESCE(CAST([C_PROVEEDOR] AS VARCHAR)+'|',''),
									                            COALESCE(CAST([C_ARTICULO] AS VARCHAR)+'|',''),
									                            COALESCE(CAST([C_SUCU_EMPR] AS VARCHAR)+'|',''),
									                            COALESCE(CAST([Q_BULTOS_KILOS_DIARCO] AS VARCHAR)+'|',''),
									                            COALESCE(CAST([F_ALTA_SIST] AS VARCHAR)+'|',''),
									                            COALESCE(CAST([C_USUARIO_GENERO_OC] AS VARCHAR)+'|',''),
									                            COALESCE(CAST([C_TERMINAL_GENERO_OC] AS VARCHAR)+'|',''),
									                            COALESCE(CAST([C_USUARIO_BLOQUEO] AS VARCHAR)+'|',''),
									                            COALESCE(CAST([M_PROCESADO] AS VARCHAR)+'|',''),
									                            COALESCE(CAST([F_PROCESADO] AS VARCHAR)+'|',''),
                        			            				COALESCE(CAST([U_PREFIJO_OC] AS VARCHAR)+'|',''),
									                            COALESCE(CAST([C_COMPRA_KIKKER] AS VARCHAR)+'|',''),
									                            COALESCE(CAST([C_USUARIO_MODIF] AS VARCHAR)+'|',''),
																COALESCE(CAST([C_COMPRADOR] AS VARCHAR),'')
					    			                        	)
                        			            ) [HASH_R]
	                    			FROM (
                        			      SELECT 
		                			                 SUPPLIER_CODE "C_PROVEEDOR",
		                			                 PRODUCT_CODE "C_ARTICULO",
		                			                 STORE_CODE "C_SUCU_EMPR",
		                			                 CASE WHEN art.M_VENDE_POR_PESO = 'S' THEN ROUND((ORDER_IN_SALES_UNIT/QUANTITY_IN_PURCHASE_PACKAGE),0) else ORDER_IN_SALES_UNIT end "Q_BULTOS_KILOS_DIARCO", -- 15/08/2023 TRATAMIENTO DE PESABLES
		                			                 CREATED_AT "F_ALTA_SIST",
		                			                 '' "C_USUARIO_GENERO_OC",
		                			                 '' "C_TERMINAL_GENERO_OC",
		                			                 '' "F_GENERO_OC",
		                			                 '' "C_USUARIO_BLOQUEO",
		                			                 'N' "M_PROCESADO",
		                			                 CAST('01/01/1900' AS DATE) "F_PROCESADO",
		                			                 0 "U_PREFIJO_OC",
		                			                 0 "U_SUFIJO_OC",
		                			                 ORDER_PACKAGE_CODE "C_COMPRA_KIKKER",
		                			                 CASE
			            			                     WHEN CHANGED_BY_LOGIN <> ''
				        			                         THEN CHANGED_BY_LOGIN
			            			                     WHEN CHANGED_PRICE_BY_LOGIN <> ''
				      			                         THEN CHANGED_PRICE_BY_LOGIN
			            			                     ELSE
				        			                         ''
		                			                 END "C_USUARIO_MODIF",
													 ERP_BUYER_CODE "C_COMPRADOR"
	                    			              from [dbo].[delivery_order_items_to_erps]
												  inner join [DIARCOP001].[DiarcoP].dbo.t050_articulos art on art.C_ARTICULo=PRODUCT_CODE
                        			         ) t
								) group_by
						GROUP BY 
							[C_PROVEEDOR],
							[C_ARTICULO],
							[C_SUCU_EMPR],
							[Q_BULTOS_KILOS_DIARCO],
							[F_ALTA_SIST],
							[C_USUARIO_GENERO_OC],
							[C_TERMINAL_GENERO_OC],
							[F_GENERO_OC],
							[C_USUARIO_BLOQUEO],
							[M_PROCESADO],
							[F_PROCESADO],
							[U_PREFIJO_OC],
							[U_SUFIJO_OC],
							[C_COMPRA_KIKKER],
							[C_USUARIO_MODIF],
							[C_COMPRADOR],
							[HASH_R]

	OPEN order_code_cursor
	FETCH NEXT FROM order_code_cursor INTO @c_compra_kikker

	WHILE @@FETCH_STATUS = 0
	BEGIN
		BEGIN TRY
			BEGIN TRANSACTION
    				MERGE [dbo].[T080_OC_PRECARGA_KIKKER] AS Target
					USING (
							SELECT 	
								[C_PROVEEDOR],
								[C_ARTICULO],
								[C_SUCU_EMPR],
								[Q_BULTOS_KILOS_DIARCO],
								[F_ALTA_SIST],
								[C_USUARIO_GENERO_OC],
								[C_TERMINAL_GENERO_OC],
								[F_GENERO_OC],
								[C_USUARIO_BLOQUEO],
								[M_PROCESADO],
								[F_PROCESADO],
								[U_PREFIJO_OC],
								[U_SUFIJO_OC],
								[C_COMPRA_KIKKER],
								[C_USUARIO_MODIF],
								[C_COMPRADOR],
								[HASH_R]
							FROM @order_code_kikker_tbl
						WHERE [C_COMPRA_KIKKER] = @c_compra_kikker) AS Source
						ON Source.[HASH_R] = Target.[HASH_R]
					WHEN NOT MATCHED BY Target THEN
						INSERT ([C_PROVEEDOR],
										[C_ARTICULO],
										[C_SUCU_EMPR],
										[Q_BULTOS_KILOS_DIARCO],
										[F_ALTA_SIST],
										[C_USUARIO_GENERO_OC],
										[C_TERMINAL_GENERO_OC],
										[F_GENERO_OC],
										[C_USUARIO_BLOQUEO],
										[M_PROCESADO],
										[F_PROCESADO],
										[U_PREFIJO_OC],
										[U_SUFIJO_OC],
										[C_COMPRA_KIKKER],
										[C_USUARIO_MODIF],
										[C_COMPRADOR],
										[HASH_R])
						VALUES(Source.[C_PROVEEDOR],
										Source.[C_ARTICULO],
										Source.[C_SUCU_EMPR],
										Source.[Q_BULTOS_KILOS_DIARCO],
										Source.[F_ALTA_SIST],
										Source.[C_USUARIO_GENERO_OC],
										Source.[C_TERMINAL_GENERO_OC],
										Source.[F_GENERO_OC],
										Source.[C_USUARIO_BLOQUEO],
										Source.[M_PROCESADO],
										Source.[F_PROCESADO],
										Source.[U_PREFIJO_OC],
										Source.[U_SUFIJO_OC],
										Source.[C_COMPRA_KIKKER],
										Source.[C_USUARIO_MODIF],
										Source.[C_COMPRADOR],
										Source.[HASH_R]);
			COMMIT
		END TRY
		BEGIN CATCH
			ROLLBACK
			BEGIN
			/* Nuevo Desarrollo de Estados hacia Kikker*/
			SET @comando='cd '+ @kikker_exe_dir + ' && '+ @kikker_exe_dir + 'status_importer.exe --error_number ' + cast(ERROR_NUMBER() as varchar(10)) + ' --error_severity '+ cast(ERROR_SEVERITY() as varchar(10)) + ' --order_package_code '  + cast(@c_compra_kikker as varchar(10))
			--select @comando;
			EXEC xp_cmdshell @comando;
			
			DELETE FROM [dbo].[T080_OC_PRECARGA_KIKKER] WHERE C_COMPRA_KIKKER=@c_compra_kikker;
			INSERT INTO [dbo].[KIKKER_ERRORS]
			SELECT
					'MERGE',
					@c_compra_kikker,
					@@ROWCOUNT,
					ERROR_LINE(),
					ERROR_MESSAGE(),
					ERROR_NUMBER(),
					ERROR_SEVERITY(),
					ERROR_STATE(),
					@process_ts;
			END
		END CATCH
		FETCH NEXT FROM order_code_cursor INTO @c_compra_kikker 
	END

	CLOSE order_code_cursor
	DEALLOCATE order_code_cursor

   	-- Construcción de tabla temporal
					
	INSERT INTO @c_compra_kikker_tbl
	SELECT DISTINCT kikker.[C_COMPRA_KIKKER]
		FROM (
				SELECT 
						DISTINCT [C_COMPRA_KIKKER],
						[C_PROVEEDOR],
						[C_ARTICULO],
      					CAST(CASE 
									[C_SUCU_EMPR] 
										WHEN '41CD' 
											THEN 41 
									ELSE 
										[C_SUCU_EMPR] 
								END AS INT) [C_SUCU_EMPR]
				FROM [dbo].[T080_OC_PRECARGA_KIKKER]
				WHERE [RECEIVED_DATE] IS NULL
			) kikker
		LEFT JOIN [DIARCOP001].[DIARCOP].[DBO].[T080_OC_PRECARGA_KIKKER] sgm
			ON kikker.[C_PROVEEDOR] = sgm.[C_PROVEEDOR]
				AND kikker.[C_SUCU_EMPR] = sgm.[C_SUCU_EMPR]
				AND kikker.[C_ARTICULO] = sgm.[C_ARTICULO]
		WHERE sgm.[C_COMPRA_KIKKER] IS NULL
	ORDER BY [C_COMPRA_KIKKER] ASC
					
	-- Importación a SGM

	DECLARE c_compra_kikker_cursor CURSOR FOR 
	SELECT [C_COMPRA_KIKKER]
		FROM @c_compra_kikker_tbl

	OPEN c_compra_kikker_cursor
	FETCH NEXT FROM c_compra_kikker_cursor INTO @c_compra_kikker

	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @products = (SELECT 
					COUNT(*)
					FROM
					(
						SELECT 
							[C_PROVEEDOR],
							CAST(CASE 
									[C_SUCU_EMPR] 
										WHEN '41CD' 
											THEN 41 
										ELSE 
											[C_SUCU_EMPR] 
									END AS INT) [C_SUCU_EMPR],
							[C_ARTICULO],
							CAST([C_COMPRA_KIKKER] AS INT) [C_COMPRA_KIKKER]
						FROM [dbo].[T080_OC_PRECARGA_KIKKER]
							WHERE [C_COMPRA_KIKKER] = @c_compra_kikker
								AND [C_COMPRA_KIKKER] IN (
															SELECT 
																	[C_COMPRA_KIKKER] 
																FROM @c_compra_kikker_tbl
															)
						) ktbl_curr_oc
					JOIN
						(
							SELECT 
									[C_PROVEEDOR],
									CAST(CASE 
											[C_SUCU_EMPR] 
												WHEN '41CD' 
													THEN 41 
												ELSE 
													[C_SUCU_EMPR] 
											END AS INT) [C_SUCU_EMPR],
									[C_ARTICULO],
									CAST([C_COMPRA_KIKKER] AS INT) [C_COMPRA_KIKKER]
								FROM [dbo].[T080_OC_PRECARGA_KIKKER]
									WHERE [C_COMPRA_KIKKER] <> @c_compra_kikker
										AND [C_COMPRA_KIKKER] IN (
																	SELECT 
																			[C_COMPRA_KIKKER] 
																		FROM @c_compra_kikker_tbl
																	)
							UNION ALL
							SELECT 
									[C_PROVEEDOR],
									[C_SUCU_EMPR],
									[C_ARTICULO],
									CAST([C_COMPRA_KIKKER] AS INT) [C_COMPRA_KIKKER]
								FROM [DIARCOP001].[DIARCOP].[DBO].[T080_OC_PRECARGA_KIKKER]
									WHERE [C_COMPRA_KIKKER] <> @c_compra_kikker
							) ktbl_oc_pend
						ON ktbl_curr_oc.[C_PROVEEDOR] = ktbl_oc_pend.[C_PROVEEDOR]
							AND ktbl_curr_oc.[C_SUCU_EMPR] = ktbl_oc_pend.[C_SUCU_EMPR]
							AND ktbl_curr_oc.[C_ARTICULO] = ktbl_oc_pend.[C_ARTICULO]
							AND ktbl_oc_pend.[C_COMPRA_KIKKER] < ktbl_curr_oc.[C_COMPRA_KIKKER])
			IF @products > 0
			BEGIN
				GOTO C_COMPRA_KIKKER_NEXT
			END

		BEGIN TRY
							
			-- BEGIN TRANSACTION (15/02/2023)
			-- Inserción en tabla de SGM
				
			INSERT INTO [DIARCOP001].[DIARCOP].[DBO].[T080_OC_PRECARGA_KIKKER]([C_PROVEEDOR],[C_ARTICULO],[C_SUCU_EMPR],[Q_BULTOS_KILOS_DIARCO],[F_ALTA_SIST],[C_USUARIO_GENERO_OC],[C_TERMINAL_GENERO_OC],[F_GENERO_OC],[C_USUARIO_BLOQUEO],[M_PROCESADO],[F_PROCESADO],[U_PREFIJO_OC],[U_SUFIJO_OC],[C_COMPRA_KIKKER],[C_USUARIO_MODIF],[C_COMPRADOR])
				SELECT 
						CAST(ktbl.[C_PROVEEDOR] AS INT) [C_PROVEEDOR]
      					,CAST(ktbl.[C_ARTICULO] AS INT) [C_ARTICULO]
      					,CAST(CASE 
									ktbl.[C_SUCU_EMPR] 
										WHEN '41CD' 
											THEN 41 
									ELSE 
										ktbl.[C_SUCU_EMPR] 
								END AS INT) [C_SUCU_EMPR]
      					,CASE WHEN ART.M_VENDE_POR_PESO = 'S' THEN ktbl.[Q_BULTOS_KILOS_DIARCO] else ktbl.[Q_BULTOS_KILOS_DIARCO]/COALESCE(art_suc.[Q_FACTOR_VTA_SUCU],1) end [Q_BULTOS_KILOS_DIARCO] --SE DESCOMENTA 08/08/2023 MAXI ARGIRO PARA QUE VIAJE LA DIVISIÓN POR EL FACTOR DE VENTA.
     					-- ,ktbl.[F_ALTA_SIST] Versión con Fecha de entrega
	  					,GETDATE() [F_ALTA_SIST]
      					,ktbl.[C_USUARIO_GENERO_OC]
      					,ktbl.[C_TERMINAL_GENERO_OC]
      					,ktbl.[F_GENERO_OC]
      					,ktbl.[C_USUARIO_BLOQUEO]
      					,ktbl.[M_PROCESADO]
      					,ktbl.[F_PROCESADO]
      					,ktbl.[U_PREFIJO_OC]
      					,ktbl.[U_SUFIJO_OC]
      					,ktbl.[C_COMPRA_KIKKER] 
      					,CASE
						 WHEN CHARINDEX('@', ktbl.[C_USUARIO_MODIF]) > 0
							THEN SUBSTRING(ktbl.[C_USUARIO_MODIF], 1, CHARINDEX('@', ktbl.[C_USUARIO_MODIF])-1)
							ELSE ' '
						 END ,
						COMP.[C_COMPRADOR] --Maxi Argiro 01/08/2023 agregado del comprador último
						--ktbl.[C_COMPRADOR]
  						FROM [dbo].[T080_OC_PRECARGA_KIKKER] ktbl
						LEFT JOIN [DIARCOP001].[DIARCOP].[DBO].[T051_ARTICULOS_SUCURSAL] art_suc
    						ON ktbl.[C_ARTICULO] = art_suc.[C_ARTICULO]
        						AND CAST(CASE ktbl.[C_SUCU_EMPR] 
												WHEN '41CD' 
													THEN 41 
												ELSE 
													ktbl.[C_SUCU_EMPR] 
										END AS INT) = art_suc.[C_SUCU_EMPR] 
						LEFT JOIN [DIARCOP001].[DiarcoP].dbo.T117_COMPRADORES COMP  --Maxi Argiro 01/08/2023 agregado del comprador último
							ON COMP.[C_USUARIO] = CASE WHEN CHARINDEX('@', ktbl.[C_COMPRADOR]) > 0 THEN SUBSTRING(ktbl.[C_COMPRADOR], 1, CHARINDEX('@', ktbl.[C_COMPRADOR])-1) ELSE ' ' END 
						inner join [DIARCOP001].[DiarcoP].dbo.t050_articulos art on art.[C_ARTICULO]=ktbl.[C_ARTICULO]
							WHERE ktbl.[C_COMPRA_KIKKER] = @c_compra_kikker
							
				-- Actualización de tabla temporal

				UPDATE [dbo].[T080_OC_PRECARGA_KIKKER]
					SET RECEIVED_DATE = GETDATE()
				WHERE [C_COMPRA_KIKKER] = @c_compra_kikker

				/* Nuevo Desarrollo de Estados hacia Kikker*/
			SET @comando='cd '+ @kikker_exe_dir + ' && '+ @kikker_exe_dir + 'status_importer.exe --error_number ' + '0' + ' --error_severity '+ '0' + ' --order_package_code '  +cast(@c_compra_kikker as varchar(10))
			--select @comando;
			EXEC xp_cmdshell @comando;
			
			--COMMIT (15/02/2023)

		END TRY
		BEGIN CATCH
			--ROLLBACK (15/02/2023)
			/* Nuevo Desarrollo de Estados hacia Kikker*/
			SET @comando='cd '+ @kikker_exe_dir + ' && '+ @kikker_exe_dir + 'status_importer.exe --error_number ' + cast(ERROR_NUMBER() as varchar(10)) + ' --error_severity '+ cast(ERROR_SEVERITY() as varchar(10)) + ' --order_package_code '  +cast(@c_compra_kikker as varchar(10))
			--select @comando;
			EXEC xp_cmdshell @comando;
			

			DELETE FROM [dbo].[T080_OC_PRECARGA_KIKKER] WHERE C_COMPRA_KIKKER=@c_compra_kikker;
			INSERT INTO [dbo].[KIKKER_ERRORS]
			SELECT
					'Importacion a SGM',
					@c_compra_kikker,
					@@ROWCOUNT,
					ERROR_LINE(),
					ERROR_MESSAGE(),
					ERROR_NUMBER(),
					ERROR_SEVERITY(),
					ERROR_STATE(),
					@process_ts;
		END CATCH
		C_COMPRA_KIKKER_NEXT:
		FETCH NEXT FROM c_compra_kikker_cursor INTO @c_compra_kikker 
	END

	CLOSE c_compra_kikker_cursor
	DEALLOCATE c_compra_kikker_cursor
	
	IF (SELECT 
					COUNT(1) 
				FROM [dbo].[KIKKER_ERRORS] 
				WHERE [CREATE_DATE] = @process_ts) > 0
	BEGIN
			RAISERROR ('Falla en inserción de sugerencia de compra de Kikker. Revisar la tabla de errores',
               16,
               1
               );
	END
END;

GO


