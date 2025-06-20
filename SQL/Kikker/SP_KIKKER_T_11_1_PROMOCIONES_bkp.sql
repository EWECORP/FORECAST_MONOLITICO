USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_KIKKER_T_11_1_PROMOCIONES_bkp]    Script Date: 19/06/2025 15:47:39 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


create PROCEDURE [dbo].[SP_KIKKER_T_11_1_PROMOCIONES_bkp]
AS 


/*CODIGO_PROMOCAO 	Código de Promoción
NOME	Nombre
TIPO	Tipo
DATA_INICIO 	Feche Inicio
DATA_FINAL	Fecha Final*/

BEGIN
	SET NOCOUNT ON;

/*CAMBIADO EL 09012023 EN BASE A REUNIÓN CON HERNAN*/
select 'CODIGO_PROMOCAO','NOME','TIPO','DATA_INICIO','DATA_FINAL' union all
select
PROMO.C_ART,
PROMO.C_NOMBRE,
'2',
min(PROMO.F_DESDE),
max(PROMO.F_HASTA)
from 
(
select
distinct
cast(ART.C_ARTICULO as varchar(10)) as C_ART,
dbo.[NORMALIZA_STRING](ART.N_ARTICULO)  as C_NOMBRE,
convert(varchar, F_VIGENCIA_DESDE, 23) as F_DESDE,
convert(varchar, F_VIGENCIA_HASTA, 23) as F_HASTA
from DIARCOP001.DIARCOP.DBO.T900_PRECIOS_VIGENCIA  PV WITH(NOLOCK)
inner join [DIARCOP001].[DiarcoP].dbo.t050_articulos ART on PV.C_ARTICULO=ART.C_ARTICULO
where M_VIGENTE='S' and C_TIPO_PRECIO=2
union all 
select
distinct
cast(ART.C_ARTICULO as varchar(10)) as C_ART,
dbo.[NORMALIZA_STRING](ART.N_ARTICULO)  as C_NOMBRE,
convert(varchar, F_VIGENCIA_DESDE, 23) as F_DESDE,
convert(varchar, F_VIGENCIA_HASTA, 23) as F_HASTA
from [DIARCO-BARRIO].DIARCOBARRIO.DBO.T900_PRECIOS_VIGENCIA PV WITH(NOLOCK)
inner join [DIARCOP001].[DiarcoP].dbo.t050_articulos ART on PV.C_ARTICULO=ART.C_ARTICULO
where M_VIGENTE='S' and C_TIPO_PRECIO=2) as PROMO
group by PROMO.C_ART,PROMO.C_NOMBRE

/*declare @fecha as date =getdate();
--declare @fecha as date = '2022-07-15 00:00:00.000'; --ejecución por un día


/*Armado de temporal con las fechas vigentes de ofertas*/
SELECT distinct
C_ARTICULO,
min(cast(C_ANIO as varchar(10))+'-'+(case  len(C_MES) when 1 then concat('0',cast(C_MES as varchar(10))) else cast(C_MES as varchar(10)) end) +'-'+(case  len(rtrim(ltrim(substring(C_DIA,13,2)))) when 1 then concat('0',cast(rtrim(ltrim(substring(C_DIA,13,2))) as varchar(10))) else cast(rtrim(ltrim(substring(C_DIA,13,2))) as varchar(10)) end))  as F_ALTA,
max(cast(C_ANIO as varchar(10))+'-'+(case  len(C_MES) when 1 then concat('0',cast(C_MES as varchar(10))) else cast(C_MES as varchar(10)) end) +'-'+(case  len(rtrim(ltrim(substring(C_DIA,13,2)))) when 1 then concat('0',cast(rtrim(ltrim(substring(C_DIA,13,2))) as varchar(10))) else cast(rtrim(ltrim(substring(C_DIA,13,2))) as varchar(10)) end))  as F_BAJA
into #vigencia_promos
FROM   
(select 
C_ANIO,
C_MES, 
C_ARTICULO,
convert(char,M_OFERTA_DIA1) as M_OFERTA_DIA1,
convert(char,M_OFERTA_DIA2) as M_OFERTA_DIA2,
convert(char,M_OFERTA_DIA3) as M_OFERTA_DIA3,
convert(char,M_OFERTA_DIA4) as M_OFERTA_DIA4,
convert(char,M_OFERTA_DIA5) as M_OFERTA_DIA5,
convert(char,M_OFERTA_DIA6) as M_OFERTA_DIA6,
convert(char,M_OFERTA_DIA7) as M_OFERTA_DIA7,
convert(char,M_OFERTA_DIA8) as M_OFERTA_DIA8,
convert(char,M_OFERTA_DIA9) as M_OFERTA_DIA9,
convert(char,M_OFERTA_DIA10) as M_OFERTA_DIA10,
convert(char,M_OFERTA_DIA11) as M_OFERTA_DIA11,
convert(char,M_OFERTA_DIA12) as M_OFERTA_DIA12,
convert(char,M_OFERTA_DIA13) as M_OFERTA_DIA13,
convert(char,M_OFERTA_DIA14) as M_OFERTA_DIA14,
convert(char,M_OFERTA_DIA15) as M_OFERTA_DIA15,
convert(char,M_OFERTA_DIA16) as M_OFERTA_DIA16,
convert(char,M_OFERTA_DIA17) as M_OFERTA_DIA17,
convert(char,M_OFERTA_DIA18) as M_OFERTA_DIA18,
convert(char,M_OFERTA_DIA19) as M_OFERTA_DIA19,
convert(char,M_OFERTA_DIA20) as M_OFERTA_DIA20,
convert(char,M_OFERTA_DIA21) as M_OFERTA_DIA21,
convert(char,M_OFERTA_DIA22) as M_OFERTA_DIA22,
convert(char,M_OFERTA_DIA23) as M_OFERTA_DIA23,
convert(char,M_OFERTA_DIA24) as M_OFERTA_DIA24,
convert(char,M_OFERTA_DIA25) as M_OFERTA_DIA25,
convert(char,M_OFERTA_DIA26) as M_OFERTA_DIA26,
convert(char,M_OFERTA_DIA27) as M_OFERTA_DIA27,
convert(char,M_OFERTA_DIA28) as M_OFERTA_DIA28,
convert(char,M_OFERTA_DIA29) as M_OFERTA_DIA29,
convert(char,M_OFERTA_DIA30) as M_OFERTA_DIA30,
convert(char,M_OFERTA_DIA31) as M_OFERTA_DIA31
from [DIARCOP001].[DiarcoP].[dbo].[T710_ESTADIS_OFERTA_FOLDER] 
where C_ANIO=year(@fecha) AND C_MES=month(@fecha)  
) as Oferta
UNPIVOT  
   (OFERTA FOR C_DIA IN (M_OFERTA_DIA1,M_OFERTA_DIA2 ,M_OFERTA_DIA3,M_OFERTA_DIA4,M_OFERTA_DIA5,M_OFERTA_DIA6,M_OFERTA_DIA7,M_OFERTA_DIA8,M_OFERTA_DIA9,M_OFERTA_DIA10,M_OFERTA_DIA11,M_OFERTA_DIA12,
   M_OFERTA_DIA13,M_OFERTA_DIA14,M_OFERTA_DIA15,M_OFERTA_DIA16,M_OFERTA_DIA17,M_OFERTA_DIA18,M_OFERTA_DIA19,M_OFERTA_DIA20,M_OFERTA_DIA21,M_OFERTA_DIA22,M_OFERTA_DIA23,M_OFERTA_DIA24,
   M_OFERTA_DIA25,M_OFERTA_DIA26,M_OFERTA_DIA27,M_OFERTA_DIA28,M_OFERTA_DIA29,M_OFERTA_DIA30,M_OFERTA_DIA31)
) AS unpvt
where oferta='S'
group by C_ARTICULO
--having min(cast(C_ANIO as varchar(10))+'-'+(case  len(C_MES) when 1 then concat('0',cast(C_MES as varchar(10))) else cast(C_MES as varchar(10)) end) +'-'+(case  len(rtrim(ltrim(substring(C_DIA,13,2)))) when 1 then concat('0',cast(rtrim(ltrim(substring(C_DIA,13,2))) as varchar(10))) else cast(rtrim(ltrim(substring(C_DIA,13,2))) as varchar(10)) end)) >replace(convert(varchar,DATEADD(DAY,-15,@fecha),111),'/','-')

--select top 100 C_SUCU_EMPR,C_ARTICULO,C_DIA_PROMO from #vigencia_promos order by 1,2,3

select 'CODIGO_PROMOCAO','NOME','TIPO','DATA_INICIO','DATA_FINAL' union all 
select 
distinct  
cast(ART.C_ARTICULO as varchar (5)),
dbo.[NORMALIZA_STRING](ART.N_ARTICULO),
'2',
VP.F_ALTA,
VP.F_BAJA
from  [DIARCOP001].[DIARCOP].dbo.[T051_ARTICULOS_SUCURSAL] ART_SUC
inner join [DIARCOP001].[DiarcoP].dbo.t050_articulos ART on ART_SUC.C_ARTICULO=ART.C_ARTICULO
inner join #vigencia_promos VP on VP.C_ARTICULO = ART_SUC.C_ARTICULO
inner join [DIARCOP001].[DIARCOP].DBO.T100_EMPRESA_SUC SUC on SUC.C_SUCU_EMPR=ART_SUC.C_SUCU_EMPR
WHERE	    SUC.C_SUCU_EMPR NOT IN (6,8,14,17,39,40,300, 80, 81, 83, 84, 88)
AND     SUC.M_SUCU_VIRTUAL = 'N'
AND      SUC.C_SUCU_EMPR NOT IN (SELECT C_SUCU_EMPR FROM [DIARCOP001].[DiarcoP].[dbo].[T900_SUCURSALES_EXCLUIDAS_GERENCIA_DB])
and M_OFERTA_SUCU='S';
--group by ART.C_ARTICULO,ART.N_ARTICULO

drop table #vigencia_promos;
*/
END;



/*
--- PARA LA DIARIA EXTRAER DESDE ESTAS TABLAS.

SELECT *
FROM DIARCOP001.DIARCOP.DBO.T900_PRECIOS_VIGENCIA WITH(NOLOCK)

UNION ALL

SELECT *
FROM [DIARCO-BARRIO].DIARCOBARRIO.DBO.T900_PRECIOS_VIGENCIA WITH(NOLOCK)


*/
GO


