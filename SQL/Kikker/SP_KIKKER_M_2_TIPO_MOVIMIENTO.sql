USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_KIKKER_M_2_TIPO_MOVIMIENTO]    Script Date: 19/06/2025 15:38:05 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[SP_KIKKER_M_2_TIPO_MOVIMIENTO]
AS 
/*
USE DIARCOP
CODIGO_TIPO_MOVIMENTO - Código do ERP para o Tipo de Movimento
DESCRICAO_TIPO_MOVIMENTO - Descrição para o Tipo de Movimento
TIPO_OPERACAO - Tipo da operação que esse movimento representa, codificada da seguinte
forma:
"0 - Ignorar movimiento
1 - Venta
2 - Recibido en la tienda
3 - Devolución al Proveedor
4 - Ajuste de Inventario
5 - Desperdicio o Merma
6 -  Consumo Interno                
7 - Transferencia"
*/
begin 
SET NOCOUNT ON;


	SELECT 'CODIGO_TIPO_MOVIMENTO','DESCRICAO_TIPO_MOVIMENTO','TIPO_OPERACAO','SINAL' union all
	SELECT  distinct
	Case		when (c_tabla= 44) then cast(c_tabla as varchar(5)) /*3 - Por devolución*/
				when (c_tabla=4 and C_CODIGO_TABLA=75) then cast(c_tabla as varchar(5))+ cast(C_CODIGO_TABLA  as varchar(5))   /*3 - Por devolución*/
		        when c_tabla=23  then cast(c_tabla  as varchar(5)) 			 /*4 - Ajuste de Inventario Positivo c_mov único 71*/
		        when c_tabla=24  then cast(c_tabla  as varchar(5)) 			 /*4 - Ajuste de Inventario Negativo c_mov único 71*/
		        when (c_tabla=106)  then cast(c_tabla  as varchar(5))				 /*5 - Desperdicio o Merma*/
				when (c_tabla=4 and C_CODIGO_TABLA=77) then cast(c_tabla as varchar(5))+ cast(C_CODIGO_TABLA  as varchar(5)) 			 /*5 - Desperdicio o Merma*/
				when (c_tabla=4 and C_CODIGO_TABLA=74) then  cast(c_tabla as varchar(5))+ cast(C_CODIGO_TABLA  as varchar(5))					 /*7 + Transferencia ingreso*/ 
				when (c_tabla=0 and C_CODIGO_TABLA=10) then cast(c_tabla as varchar(5))+ cast(C_CODIGO_TABLA  as varchar(5)) 					 /*2 - Recibido en la tienda*/ 
				when (c_tabla=4 and C_CODIGO_TABLA=73) then cast(c_tabla as varchar(5))+ cast(C_CODIGO_TABLA  as varchar(5))	end	,/*7 - Transferencia salida*/ 
	Case		when (c_tabla= 44) then 'Devolución al Proveedor'  /*3 - Por devolución*/
				when (c_tabla=4 and C_CODIGO_TABLA=75) then 'Devolución al Proveedor'  /*3 - Por devolución*/
		        when c_tabla=23  then 'Ajuste de Inventario'		 /*4 - Ajuste de Inventario Positivo c_mov único 71*/
		        when c_tabla=24  then 'Ajuste de Inventario'				 /*4 - Ajuste de Inventario Negativo c_mov único 71*/
		        when (c_tabla=106)  then 'Desperdicio o Merma'				 /*5 - Desperdicio o Merma*/
				when (c_tabla=4 and C_CODIGO_TABLA=77) then 'Desperdicio o Merma'				 /*5 - Desperdicio o Merma*/
				when (c_tabla=4 and C_CODIGO_TABLA=74) then 'Transferencia'						 /*7 + Transferencia ingreso*/ 
				when (c_tabla=0 and C_CODIGO_TABLA=10) then 'Recibido en la tienda'						 /*2 - Recibido en la tienda*/ 
				when (c_tabla=4 and C_CODIGO_TABLA=73) then 'Transferencia' 		end,				 /*7 - Transferencia salida*/ 
	Case		when (c_tabla= 44) then  '3'  /*3 - Por devolución*/
				when (c_tabla=4 and C_CODIGO_TABLA=75) then '3'  /*3 - Por devolución*/
		        when c_tabla=23  then '4'			 /*4 - Ajuste de Inventario Positivo c_mov único 71*/
		        when c_tabla=24  then '4'				 /*4 - Ajuste de Inventario Negativo c_mov único 71*/
		        when (c_tabla=106)  then '5'				 /*5 - Desperdicio o Merma*/
				when (c_tabla=4 and C_CODIGO_TABLA=77) then '5'				 /*5 - Desperdicio o Merma*/
				when (c_tabla=4 and C_CODIGO_TABLA=74) then '7'					 /*7 + Transferencia ingreso*/ 
				when (c_tabla=0 and C_CODIGO_TABLA=10) then '2'						 /*2 - Recibido en la tienda*/ 
				when (c_tabla=4 and C_CODIGO_TABLA=73) then '7' 		end,				 /*7 - Transferencia salida*/ 
	Case		when (c_tabla= 44) then '+'  /*3 - Por devolución*/
				when (c_tabla=4 and C_CODIGO_TABLA=75) then '+'  /*3 - Por devolución*/
		        when c_tabla=23  then '+'			 /*4 - Ajuste de Inventario Positivo c_mov único 71*/
		        when c_tabla=24  then '-'				 /*4 - Ajuste de Inventario Negativo c_mov único 71*/
		        when (c_tabla=106)  then '+'				 /*5 - Desperdicio o Merma*/
				when (c_tabla=4 and C_CODIGO_TABLA=77) then '+'				 /*5 - Desperdicio o Merma*/
				when (c_tabla=4 and C_CODIGO_TABLA=74) then  '+'						 /*7 + Transferencia ingreso*/ 
				when (c_tabla=0 and C_CODIGO_TABLA=10) then '+'						 /*2 - Recibido en la tienda*/ 
				when (c_tabla=4 and C_CODIGO_TABLA=73) then '+' 		end				 /*7 - Transferencia salida*/ 
	   from [DIARCOP001].[DiarcoP].dbo.t001_tabla_codigo 
		   where c_tabla in (44,23,24,106,4,0)
		   union all 
	SELECT '99','Venta','1','+'		 /*1 - Por Venta*/ 
	union all 
	SELECT '010','Recibido en la tienda','2','+'						 /*2 - Recibido en la tienda*/ 
	union all
	SELECT '4373','Transferencia','7','+'
	union all
	select '4374','Transferencia','7','+'
end;


/*
SELECT  C_TABLA, C_CODIGO_TABLA, D_CODIGO
FROM	  [DIARCOP001].[DiarcoP].dbo.t001_tabla_codigo
WHERE	  C_TABLA = 4
AND	  C_CODIGO_TABLA IN (10,71,73,74,75,77)

UNION ALL

SELECT  C_TABLA, C_CODIGO_TABLA, D_CODIGO
FROM	  [DIARCOP001].[DiarcoP].dbo.t001_tabla_codigo
WHERE	  C_TABLA IN (23,24,106) 
*/
GO


