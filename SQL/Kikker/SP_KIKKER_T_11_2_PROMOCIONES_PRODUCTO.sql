USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_KIKKER_T_11_2_PROMOCIONES_PRODUCTO]    Script Date: 19/06/2025 15:48:20 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[SP_KIKKER_T_11_2_PROMOCIONES_PRODUCTO]
AS 
 
 /*CODIGO_PROMOCAO; COD_PRODUTO; COD_LOJA; VALOR_PROMOCIONAL; NOME_PRECO; DATA_INICIO;DATA_FINAL*/

BEGIN
	SET NOCOUNT ON;

declare @fecha as date =getdate()-1;

select 'CODIGO_PROMOCAO','COD_PRODUTO','COD_LOJA','VALOR_PROMOCIONAL','NOME_PRECO','DATA_INICIO','DATA_FINAL' union all
select
convert(varchar,PROMO.C_ART),
convert(varchar,PROMO.C_ART),
case PROMO.C_SUCU_EMPR when 41 then '41CD' else DBO.[NORMALIZA_STRING](PROMO.C_SUCU_EMPR) end,
convert(varchar,PROMO.PRECIO),
convert(varchar,PROMO.C_NOMBRE),
min(PROMO.F_DESDE),
max(PROMO.F_HASTA)
from 
(
select
distinct
cast(ART.C_ARTICULO as varchar(10)) as C_ART,
dbo.[NORMALIZA_STRING](ART.N_ARTICULO)  as C_NOMBRE,
ART_SUC.C_SUCU_EMPR as C_SUCU_EMPR,
convert(varchar, F_VIGENCIA_DESDE, 23) as F_DESDE,
convert(varchar, F_VIGENCIA_HASTA, 23) as F_HASTA,
cast(ART_SUC.I_PRECIO_VTA as varchar(10)) as PRECIO
from DIARCOP001.DIARCOP.DBO.T900_PRECIOS_VIGENCIA  PV WITH(NOLOCK)
inner join [DIARCOP001].[DiarcoP].dbo.t050_articulos ART on PV.C_ARTICULO=ART.C_ARTICULO
inner join [DIARCOP001].[DIARCOP].dbo.[T051_ARTICULOS_SUCURSAL] ART_SUC on ART_SUC.C_ARTICULO=ART.C_ARTICULO and ART_SUC.C_SUCU_EMPR=PV.C_SUCU_EMPR
where M_VIGENTE='S' and C_TIPO_PRECIO=2
union all 
select
distinct
cast(ART.C_ARTICULO as varchar(10)) as C_ART,
dbo.[NORMALIZA_STRING](ART.N_ARTICULO)  as C_NOMBRE,
ART_SUC.C_SUCU_EMPR as C_SUCU_EMPR,
convert(varchar, F_VIGENCIA_DESDE, 23) as F_DESDE,
convert(varchar, F_VIGENCIA_HASTA, 23) as F_HASTA,
cast(ART_SUC.I_PRECIO_VTA as varchar(10))  as PRECIO
from [DIARCO-BARRIO].DIARCOBARRIO.DBO.T900_PRECIOS_VIGENCIA PV WITH(NOLOCK)
inner join [DIARCOP001].[DiarcoP].dbo.t050_articulos ART on PV.C_ARTICULO=ART.C_ARTICULO
inner join [DIARCOP001].[DIARCOP].dbo.[T051_ARTICULOS_SUCURSAL] ART_SUC on ART_SUC.C_ARTICULO=ART.C_ARTICULO and ART_SUC.C_SUCU_EMPR=PV.C_SUCU_EMPR
where M_VIGENTE='S' and C_TIPO_PRECIO=2 
union all
select 
cast( promo_venci.C_ARTICULO as varchar(10)) as C_ART,
dbo.[NORMALIZA_STRING](ART.N_ARTICULO)  as C_NOMBRE,
 promo_venci.C_SUCU_EMPR as C_SUCU_EMPR,
convert(varchar, F_DESDE, 23) as F_DESDE,
convert(varchar, F_HASTA, 23) as F_HASTA,
cast(promo_venci.I_PRECIO_VTA as varchar(10))  as PRECIO
from [DIARCOP001].[DIARCOP].DBO.T230_facturador_negocios_especiales_por_cantidad promo_venci
inner join [DIARCOP001].[DiarcoP].dbo.t050_articulos ART on promo_venci.C_ARTICULO=ART.C_ARTICULO
where REPLACE(CONVERT(VARCHAR,@fecha,111),'/','-') between F_DESDE and F_HASTA and q_unidades_kilos_saldo>0
union all 
select 
cast( promo_venci.C_ARTICULO as varchar(10)) as C_ART,
dbo.[NORMALIZA_STRING](ART.N_ARTICULO)  as C_NOMBRE,
 promo_venci.C_SUCU_EMPR as C_SUCU_EMPR,
convert(varchar, F_DESDE, 23) as F_DESDE,
convert(varchar, F_HASTA, 23) as F_HASTA,
cast(promo_venci.I_PRECIO_VTA as varchar(10))  as PRECIO
from [DIARCO-BARRIO].[DIARCOBARRIO].DBO.T230_facturador_negocios_especiales_por_cantidad promo_venci
inner join [DIARCOP001].[DiarcoP].dbo.t050_articulos ART on promo_venci.C_ARTICULO=ART.C_ARTICULO
where REPLACE(CONVERT(VARCHAR,@fecha,111),'/','-') between F_DESDE and F_HASTA and q_unidades_kilos_saldo>0
) as PROMO
group by PROMO.C_ART,PROMO.C_NOMBRE,PROMO.C_SUCU_EMPR,PROMO.PRECIO

END;
GO


