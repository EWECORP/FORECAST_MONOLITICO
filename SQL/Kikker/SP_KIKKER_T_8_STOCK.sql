USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_KIKKER_T_8_STOCK]    Script Date: 19/06/2025 15:29:51 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[SP_KIKKER_T_8_STOCK] as -- @sucu as integer = null, @fecha as date =getdate AS
/*
COD_PRODUTO - Código do produto
COD_LOJA - Código da loja
DATA_ESTOQUE - Data do estoque (formato AAAA-MM-DD)
ESTOQUE_ATUAL - Estoque atual (unidades de venda)
--PENDENCIA_VENDA - Pendência de venda (unidades de venda) --
PEDIDO_PENDENTE - Pedidos pendentes (quantidade em unidades de venda) --pedidodes de venta de la sucur, para el articulo , sum
CUSTO_UNIT_ULT_ENTRADA - Custo unitário da Última entrada i_costo_estadistico
PRECO_UNIT_VENDA - Preço unitário de venda
PROMOCAO - Venda em promoção - (1 - promoção ou 0 não promoção)
LOTE	Nro Lote para control
VALIDADE_LOTE	Lote para control
*/
BEGIN

set nocount on;

declare @fecha as date =getdate()-1;

/*calculo de las unidades pendientes de entrega por sucursal y articulo*/
select 
OC_CABE.C_SUCU_COMPRA as SUCU_COMPRA,
OC_DETA.C_ARTICULO as C_ARTICULO,
sum((OC_DETA.Q_BULTOS_PROV_PED*OC_DETA.Q_FACTOR_PROV_PED)-OC_DETA.Q_UNID_CUMPLIDAS) as Pendientes
into #pedidos_pendientes
from  [DIARCOP001].[DIARCOP].DBO.T080_OC_CABE OC_CABE 
inner join [DIARCOP001].[DIARCOP].DBO.T081_OC_DETA OC_DETA  on OC_DETA.C_OC = OC_CABE.C_OC 
															AND OC_DETA.U_PREFIJO_OC = OC_CABE.U_PREFIJO_OC 
															AND OC_DETA.U_SUFIJO_OC = OC_CABE.U_SUFIJO_OC 
WHERE ((OC_DETA.Q_BULTOS_PROV_PED*OC_DETA.Q_FACTOR_PROV_PED)  - OC_DETA.Q_UNID_CUMPLIDAS) <> 0	AND  OC_CABE.C_SITUAC = 1
and OC_CABE.C_SUCU_COMPRA<>300
group by 
OC_CABE.C_SUCU_COMPRA,
OC_DETA.C_ARTICULO

/*traemos las promos por vencimiento*/
select 
C_SUCU_EMPR as C_SUCU_EMPR,C_ARTICULO as C_ARTICULO,1 as flag
into #promo_venci
from [DIARCOP001].[DIARCOP].DBO.T230_facturador_negocios_especiales_por_cantidad
where REPLACE(CONVERT(VARCHAR,@fecha,111),'/','-') between F_DESDE and F_HASTA and q_unidades_kilos_saldo>0
union all
select 
C_SUCU_EMPR as C_SUCU_EMPR,C_ARTICULO as C_ARTICULO,1 as flag
from [DIARCO-BARRIO].[DIARCOBARRIO].DBO.T230_facturador_negocios_especiales_por_cantidad 
where REPLACE(CONVERT(VARCHAR,@fecha,111),'/','-') between F_DESDE and F_HASTA and q_unidades_kilos_saldo>0


SELECT 'COD_PRODUTO','COD_LOJA','DATA_ESTOQUE','ESTOQUE_ATUAL','PENDENCIA_VENDA','PEDIDO_PENDENTE','CUSTO_UNIT_ULT_ENTRADA','PRECO_UNIT_VENDA','PROMOCAO','LOTE','VALIDADE_LOTE','ESTOQUE_RESERVA','PROMOCAO_VALIDADE'

UNION ALL
select 
convert(varchar,STK.C_ARTICULO),
case STK.C_SUCU_EMPR when 41 then '41CD' else DBO.[NORMALIZA_STRING](STK.C_SUCU_EMPR) end,
REPLACE(CONVERT(VARCHAR,@fecha,111),'/','-'),
CASE WHEN ART.M_VENDE_POR_PESO='N' then DBO.[NORMALIZA_STRING](STK.Q_UNID_ARTICULO) when ART.M_VENDE_POR_PESO='S' then DBO.[NORMALIZA_STRING](STK.Q_PESO_ARTICULO) end,
isnull(convert(varchar,DBO.[NORMALIZA_STRING](PP.Pendientes)),'0'),
'',
DBO.[NORMALIZA_STRING](I_LISTA_CALCULADO),
DBO.[NORMALIZA_STRING](ART_SUC.I_PRECIO_VTA),
case ART_SUC.M_OFERTA_SUCU when 'N' then '0' else '1' end,
'',
'',
'',
case promo_venci.flag when 1 then '1' else '0' end
from [DIARCOP001].[DIARCOP].DBO.t060_STOCK STK
left join #pedidos_pendientes PP on  PP.C_ARTICULO=STK.C_ARTICULO and PP.SUCU_COMPRA=STK.C_SUCU_EMPR
left join #promo_venci PROMO_VENCI on PROMO_VENCI.C_SUCU_EMPR=STK.C_SUCU_EMPR and PROMO_VENCI.C_ARTICULO=STK.C_ARTICULO
left join [DIARCOP001].[DIARCOP].DBO.T055_ARTICULOS_CONDCOMPRA_COSTOS COSTO on COSTO.C_ARTICULO=STK.C_ARTICULO and COSTO.C_SUCU_EMPR=STK.C_SUCU_EMPR 
left join [DIARCOP001].[DIARCOP].DBO.T051_ARTICULOS_SUCURSAL ART_SUC ON ART_SUC.C_ARTICULO=STK.C_ARTICULO and ART_SUC.C_SUCU_EMPR=STK.C_SUCU_EMPR 
inner join [DIARCOP001].[DIARCOP].DBO.T050_ARTICULOS ART on ART.C_ARTICULO=STK.C_ARTICULO  and  ART.C_PROVEEDOR_PRIMARIO=COSTO.C_PROVEEDOR
INNER JOIN [DIARCOP001].[DIARCOP].DBO.T100_EMPRESA_SUC SUC on STK.C_SUCU_EMPR=SUC.C_SUCU_EMPR
where 
SUC.C_SUCU_EMPR NOT IN (6,8,14,17,39,40,300, 80, 81, 83, 84, 88)
AND SUC.M_SUCU_VIRTUAL = 'N'
AND SUC.C_SUCU_EMPR NOT IN (SELECT C_SUCU_EMPR FROM [DIARCOP001].[DiarcoP].[dbo].[T900_SUCURSALES_EXCLUIDAS_GERENCIA_DB]);

drop table #pedidos_pendientes,#promo_venci;
END;
GO


