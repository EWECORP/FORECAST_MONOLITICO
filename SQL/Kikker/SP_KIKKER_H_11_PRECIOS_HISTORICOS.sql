USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_KIKKER_H_11_PRECIOS_HISTORICOS]    Script Date: 19/06/2025 15:36:00 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[SP_KIKKER_H_11_PRECIOS_HISTORICOS] @anio char(4), @mes char(2)
AS
/*
COD_REGIONAL - Nome do grupo econômico (não obrigatório)
COD_FORNECEDOR - Código do Fornecedor
COD_LOJA - Código da Loja (não obrigatório)
COD_PRODUTO - Código do Produto
QT_UN_COMPRA - Quantidade de produtos em unidade de venda na embalagem de compra
UN_COMPRA - Unidade de compra. (Ex: CX, UN, M,KG)
PRECO_UNITARIO - Preço unitário em unidade de venda
VALIDADE - Se o preço for válido até determinada data informar a data (formato AAAA-MM-DD)
PRECO_UNIT_SEM_IMPOSTO	Precio Unitario en unidades de compra sin impuestos	N
PRAZO_ENTREGA	Plazo de Entrega	S
PRINCIPAL	Verdadero o falso (si es el proveedor principal de la clave de la tienda del producto)	S
COD_CONTRATO	Código de contrato	N
PRAZO_PAGAMENTO	Plazo en dias	N
PESO	pedo producto unidades de compra	N
VOLUMEN	Pedo producto unidades de compra	N
N_CAMADAS_PALETE	Número de camadas	N
QTD_PRODUTOS_CAMADAS_PALETE	Número de productos en unidades de venta por camadas	N
*/

BEGIN
SET NOCOUNT ON;

IF OBJECT_ID('tempdb..##costos_historicos') IS NULL  
        BEGIN
         exec [dbo].[SP_KIKKER_H_1_COSTOS] @anio , @mes
        END;


	SELECT 'COD_REGIONAL','COD_FORNECEDOR','COD_LOJA','COD_PRODUTO','QT_UN_COMPRA','UN_COMPRA','PRECO_UNITARIO','VALIDADE','PRECO_UNIT_SEM_IMPOSTO','PRAZO_ENTREGA','PRINCIPAL'/*,'COD_CONTRATO','PRAZO_PAGAMENTO','PESO','VOLUMEN','N_CAMADAS_PALETE','QTD_PRODUTOS_CAMADAS_PALETE'*/
	UNION ALL
	SELECT 
	'' ,
	CONVERT(VARCHAR,ART.C_PROVEEDOR_PRIMARIO),
	case SUC.C_SUCU_EMPR when 41 then '41CD' else DBO.[NORMALIZA_STRING](SUC.C_SUCU_EMPR) end as C_SUCU_EMPR,
	CONVERT(VARCHAR,SUC.C_ARTICULO),
	CONVERT(VARCHAR, cast(PROV.Q_FACTOR_PROVEEDOR/SUC.Q_FACTOR_VTA_SUCU as decimal (10,2))),
	DBO.[NORMALIZA_STRING](COD.D_CODIGO_ABREV), --T001 TABLA CODIGO JOIN UNIDAD DE MEDIDA TABLA T050
	CONVERT(VARCHAR,COSTOS.L_PRECIOS),
	COSTOS.F_PRECIO,
	'',
	cast(round(((1+isnull(PROV1.Q_DIAS_PREPARACION,0)+isnull(U_DIA_SEMANA_OC_LIMITE,0))*1.5),0) as varchar(10)),
	'1'
--	'',
--	'',
--	'',
--	'',
--	'',
--	'' 
	FROM [DIARCOP001].[DIARCOP].dbo.T051_ARTICULOS_SUCURSAL SUC 
					inner JOIN [DIARCOP001].[DIARCOP].dbo.T050_ARTICULOS ART ON ART.C_ARTICULO=SUC.C_ARTICULO
				    inner JOIN [DIARCOP001].[DIARCOP].dbo.T052_ARTICULOS_PROVEEDOR PROV ON SUC.C_ARTICULO=PROV.C_ARTICULO AND ART.C_PROVEEDOR_PRIMARIO=PROV.C_PROVEEDOR and ART.C_ARTICULO=PROV.C_ARTICULO
					inner JOIN [DIARCOP001].[DIARCOP].dbo.T001_TABLA_CODIGO COD ON COD.C_CODIGO_TABLA=ART.C_UNIDAD_MEDIDA AND COD.C_TABLA=79
					inner JOIN ##costos_historicos COSTOS ON COSTOS.C_ARTICULO=SUC.C_ARTICULO AND COSTOS.C_SUCU_EMPR=SUC.C_SUCU_EMPR --AND COSTOS.C_PROVEEDOR=ART.C_PROVEEDOR_PRIMARIO
					left join [DIARCOP001].[DiarcoP].[dbo].[T020_PROVEEDOR_DIAS_ENTREGA_DETA] PROV1 on PROV1.C_PROVEEDOR=ART.C_PROVEEDOR_PRIMARIO and  PROV1.C_SUCU_EMPR=suc.C_SUCU_EMPR
	where  SUC.C_SUCU_EMPR<>300 and COSTOS.L_PRECIOS<>0


END;




GO


