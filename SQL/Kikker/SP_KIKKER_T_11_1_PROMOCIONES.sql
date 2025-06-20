USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_KIKKER_T_11_1_PROMOCIONES]    Script Date: 19/06/2025 15:47:20 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[SP_KIKKER_T_11_1_PROMOCIONES]
AS 

/*CODIGO_PROMOCAO, NOME, TIPO, TIPO_VALIDADE*/

SET NOCOUNT ON;


BEGIN
declare @fecha as date =getdate()-1;

Select 'CODIGO_PROMOCAO','NOME','TIPO','TIPO_VALIDADE' union all
select 
distinct  
cast(ART_SUC.C_ARTICULO as varchar(5)),
dbo.[NORMALIZA_STRING](ART.N_ARTICULO),
'2',
'0'
from  [DIARCOP001].[DIARCOP].dbo.[T051_ARTICULOS_SUCURSAL] ART_SUC
inner join [DIARCOP001].[DiarcoP].dbo.t050_articulos ART on ART_SUC.C_ARTICULO=ART.C_ARTICULO
inner join [DIARCOP001].[DIARCOP].DBO.T100_EMPRESA_SUC SUC on SUC.C_SUCU_EMPR=ART_SUC.C_SUCU_EMPR
WHERE	    SUC.C_SUCU_EMPR NOT IN (6,8,14,17,39,40,300, 80, 81, 83, 84, 88)
AND     SUC.M_SUCU_VIRTUAL = 'N'
AND      SUC.C_SUCU_EMPR NOT IN (SELECT C_SUCU_EMPR FROM [DIARCOP001].[DiarcoP].[dbo].[T900_SUCURSALES_EXCLUIDAS_GERENCIA_DB])
and M_OFERTA_SUCU='S'
union all
select distinct
convert(varchar,promo_venci.C_ARTICULO) as C_ARTICULO,
dbo.[NORMALIZA_STRING](ART.N_ARTICULO),
'2',
'1'
from  [DIARCOP001].[DIARCOP].DBO.T230_facturador_negocios_especiales_por_cantidad promo_venci
inner join [DIARCOP001].[DiarcoP].dbo.t050_articulos ART on ART.C_ARTICULO=promo_venci.C_ARTICULO
where REPLACE(CONVERT(VARCHAR,@fecha,111),'/','-') between F_DESDE and F_HASTA and q_unidades_kilos_saldo>0
union all
select distinct
convert(varchar,promo_venci.C_ARTICULO) as C_ARTICULO,
dbo.[NORMALIZA_STRING](ART.N_ARTICULO),
'2',
'1'
from  [DIARCO-BARRIO].[DIARCOBARRIO].DBO.T230_facturador_negocios_especiales_por_cantidad promo_venci
inner join [DIARCOP001].[DiarcoP].dbo.t050_articulos ART on ART.C_ARTICULO=promo_venci.C_ARTICULO
where REPLACE(CONVERT(VARCHAR,@fecha,111),'/','-') between F_DESDE and F_HASTA and q_unidades_kilos_saldo>0;

END;
GO


