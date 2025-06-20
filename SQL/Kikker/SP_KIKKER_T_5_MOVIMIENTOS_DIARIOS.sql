USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_KIKKER_T_5_MOVIMIENTOS_DIARIOS]    Script Date: 19/06/2025 15:51:39 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE procedure [dbo].[SP_KIKKER_T_5_MOVIMIENTOS_DIARIOS]
AS
BEGIN
/*COD_PRODUTO	Código del Producto
COD_LOJA	Código Tienda
DATA_MOVIMENTO	Fecha del Movimiento (AAAA-MM-DD)
CODIGO_MOVIMENTO	Código Tipo de Movimiento 
QTDE	Cantidad
VALOR_UNIT	Valor unitário del movimiento
VALOR_ICMS_UNIT	Valor ICMS unitário
PIS 	Tributo PIS COFINS? (1 - true ou 0 - false)*/

SET NOCOUNT ON;

--IMPORTANTE: FALTA RELEVAR DE DONDE SALEN LAS RECEPCIONES de TIENDA



--MODIFICACIONES POR HERNAN LOPEZ
---- HERNAN LOPEZ 03/08/2022

	 
--declare @fecha as date = getdate()-1;---- HERNAN LOPEZ 03/08/2022
declare @fecha as dateTIME = getdate()-1; ---- HERNAN LOPEZ 03/08/2022
DECLARE @FECHA_HASTA AS DATETIME = GETDATE() ---- HERNAN LOPEZ 03/08/2022

select 'COD_PRODUTO','COD_LOJA','DATA_MOVIMENTO','CODIGO_MOVIMENTO','QTDE','VALOR_UNIT','VALOR_ICMS_UNIT','PIS' union all
select 
convert(varchar,C_ARTICULO),
case M.C_SUCU_EMPR when 41 then '41CD' else DBO.[NORMALIZA_STRING](M.C_SUCU_EMPR) end,
--replace(convert(varchar,F_MOV,111),'/','-'), ---- HERNAN LOPEZ 03/08/2022
convert(varchar,F_MOV,23), ---- HERNAN LOPEZ 03/08/2022
	Case		when (c_tabla= 44) then cast(c_tabla as varchar(5))   /*3 - Por devolución*/
				when (c_tabla=4 and c_mov=75) then cast(c_tabla as varchar(5))+ cast(c_mov  as varchar(5))   /*3 - Por devolución*/
		        when c_tabla=23  then cast(c_tabla  as varchar(5)) 				 /*4 - Ajuste de Inventario Positivo c_mov único 71*/
		        when c_tabla=24  then cast(c_tabla  as varchar(5)) 					 /*4 - Ajuste de Inventario Negativo c_mov único 71*/
		        when (c_tabla=106)  then cast(c_tabla  as varchar(5))				 /*5 - Desperdicio o Merma*/
				when (c_tabla=4 and c_mov=77) then cast(c_tabla as varchar(5))+ cast(c_mov  as varchar(5))				 /*5 - Desperdicio o Merma*/
				when (c_tabla=43 and c_mov=74) then  cast(c_tabla as varchar(5))+ cast(c_mov  as varchar(5)) 						 /*7 + Transferencia ingreso*/ 
				when (c_tabla=0 and c_mov=10) then cast(c_tabla as varchar(5))+ cast(c_mov  as varchar(5)) 						 /*2 - Recibido en la tienda*/ 
				when (c_tabla=43 and c_mov=73) then cast(c_tabla as varchar(5))+ cast(c_mov  as varchar(5)) 		end	,			 /*7 - Transferencia salida*/ 
--DBO.[NORMALIZA_STRING](Q_BULTOS_KGS_MOV*Q_FACTOR_PZAS_MOV), ---- HERNAN LOPEZ 03/08/2022
--DBO.[NORMALIZA_STRING](I_COSTO_ESTADISTICO), ---- HERNAN LOPEZ 03/08/2022
convert(varchar,(Q_BULTOS_KGS_MOV*Q_FACTOR_PZAS_MOV)), ---- HERNAN LOPEZ 03/08/2022
convert(varchar,I_COSTO_ESTADISTICO), ---- HERNAN LOPEZ 03/08/2022
'',
'0'
from [DCO-DBCORE-P02].[DiarcoEst].[dbo].[T870_HIST_MOVIMIENTOS_STOCK] M
inner join [DIARCOP001].[DIARCOP].DBO.T100_EMPRESA_SUC SUC on SUC.C_SUCU_EMPR=M.C_SUCU_EMPR
--where replace(convert(varchar,F_MOV,111),'/','-')= replace(convert(varchar,@fecha,111),'/','-') ---- HERNAN LOPEZ 03/08/2022
where F_MOV >= @FECHA AND F_MOV < @FECHA_HASTA ---- HERNAN LOPEZ 03/08/2022
and c_tabla in (44,23,24,106,4,0,43) 
and SUC.C_SUCU_EMPR NOT IN (6,8,14,17,39,40,300, 80, 81, 83, 84, 88)
AND     SUC.M_SUCU_VIRTUAL = 'N'
AND      SUC.C_SUCU_EMPR NOT IN (SELECT C_SUCU_EMPR FROM [DIARCOP001].[DiarcoP].[dbo].[T900_SUCURSALES_EXCLUIDAS_GERENCIA_DB])
union all
select 
convert(varchar,C_ARTICULO),
case M.C_SUCU_EMPR when 41 then '41CD' else DBO.[NORMALIZA_STRING](M.C_SUCU_EMPR) end,
--replace(convert(varchar,F_VENTA,111),'/','-'), ---- HERNAN LOPEZ 03/08/2022
convert(varchar,F_VENTA,23), ---- HERNAN LOPEZ 03/08/2022
'99',--Tipo Ventas.
--DBO.[NORMALIZA_STRING](Q_UNIDADES_VENDIDAS), ---- HERNAN LOPEZ 03/08/2022
--DBO.[NORMALIZA_STRING](I_PRECIO_COSTO), ---- HERNAN LOPEZ 03/08/2022
convert(varchar,Q_UNIDADES_VENDIDAS), ---- HERNAN LOPEZ 03/08/2022
convert(varchar,I_PRECIO_VENTA), ---- HERNAN LOPEZ 03/08/2022 -I_PRECIO_VENTA
'',
'0'
from [DCO-DBCORE-P02].[DiarcoEst].[dbo].T702_EST_VTAS_POR_ARTICULO M
inner join [DIARCOP001].[DIARCOP].DBO.T100_EMPRESA_SUC SUC on SUC.C_SUCU_EMPR=M.C_SUCU_EMPR
where 
 SUC.C_SUCU_EMPR NOT IN (6,8,14,17,39,40,300, 80, 81, 83, 84, 88)
AND     SUC.M_SUCU_VIRTUAL = 'N'
AND      SUC.C_SUCU_EMPR NOT IN (SELECT C_SUCU_EMPR FROM [DIARCOP001].[DiarcoP].[dbo].[T900_SUCURSALES_EXCLUIDAS_GERENCIA_DB])
AND CONVERT(VARCHAR,F_VENTA,23) >= CONVERT(VARCHAR,@fecha,23) AND CONVERT(VARCHAR,F_VENTA,23) < CONVERT(VARCHAR,@FECHA_HASTA,23) ---- HERNAN LOPEZ 03/08/2022
union all
select 
convert(varchar,C_ARTICULO),
case M.C_SUCU_EMPR when 41 then '41CD' else DBO.[NORMALIZA_STRING](M.C_SUCU_EMPR) end,
--replace(convert(varchar,F_VENTA,111),'/','-'), ---- HERNAN LOPEZ 03/08/2022
convert(varchar,F_VENTA,23), ---- HERNAN LOPEZ 03/08/2022
'99',--Tipo Ventas.
--DBO.[NORMALIZA_STRING](Q_UNIDADES_VENDIDAS), ---- HERNAN LOPEZ 03/08/2022
--DBO.[NORMALIZA_STRING](I_PRECIO_COSTO), ---- HERNAN LOPEZ 03/08/2022
convert(varchar,Q_UNIDADES_VENDIDAS), ---- HERNAN LOPEZ 03/08/2022
convert(varchar,I_PRECIO_VENTA), ---- HERNAN LOPEZ 03/08/2022 -I_PRECIO_VENTA
'',
'0'
from [DCO-DBCORE-P02].[DiarcoEst].[dbo].T702_EST_VTAS_POR_ARTICULO_DBARRIO M
inner join [DIARCOP001].[DIARCOP].DBO.T100_EMPRESA_SUC SUC on SUC.C_SUCU_EMPR=M.C_SUCU_EMPR
--where replace(convert(varchar,F_VENTA,111),'/','-')=replace(convert(varchar,@fecha,111),'/','-') ---- HERNAN LOPEZ 03/08/2022
where CONVERT(VARCHAR,F_VENTA,23) >= CONVERT(VARCHAR,@fecha,23) AND CONVERT(VARCHAR,F_VENTA,23) < CONVERT(VARCHAR,@FECHA_HASTA,23) ---- HERNAN LOPEZ 03/08/2022
and SUC.C_SUCU_EMPR NOT IN (6,8,14,17,39,40,300, 80, 81, 83, 84, 88)
AND     SUC.M_SUCU_VIRTUAL = 'N'
AND      SUC.C_SUCU_EMPR NOT IN (SELECT C_SUCU_EMPR FROM [DIARCOP001].[DiarcoP].[dbo].[T900_SUCURSALES_EXCLUIDAS_GERENCIA_DB])
end;
GO


