USE [kikker]
GO

/****** Object:  StoredProcedure [dbo].[SP_KIKKER_M_3_PRODUCTOS]    Script Date: 19/06/2025 15:39:02 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

/*produtos csv 
Onde:
COD_ERP - Código do ERP para o Produto
DESCRICAO - Descrição comercial do Produto
COD_ERP_DEPARTAMENTO - Código do ERP para o Departamento do Produto
COD_ERP_SETOR - Código do ERP para o Setor
COD_ERP_GRUPO - Código do ERP para o Grupo do Produto
COD_ERP_SUB_GRUPO - Código do ERP para o Subgrupo do Produto
UNIDADE_DE_VENDA - Unidade de Venda
QTDE_UNID_VENDA - Quantidade na Unidade de Venda
UNIDADE_DE_COMPRA - Unidade de Compra
QTDE_UNID_COMPRA - Quantidade na Unidade de Compra
CLASSIFICACAO - Classificação do Produto
1 - Compra e Venda com o mesmo código
2 - Componente de receita
3 - Produto produzido na loja
PRAZO_VALIDADE - Prazo de validade em dias - Numérico
PRAZO_ACEITACAO - Prazo de aceitação na entrega - Numérico
PRAZO_RETIRADA_GONDOLA - Prazo de retirada da gôndola - Numérico
COD_FORNECEDOR - Código do Fornecedor do Produto
EAN - Código EAN do Produto
5
PRODUTO_BASE - Código do ERP dos produtos base separado por "|". Exemplo:
"654|2234|223"
PROP_BAIXA_BASE - Proporção de consumo do produtos bases pelo filho, por exemplo, 1Kg de
picanha fatiada consome 1,1Kg de picanha, nesse caso o valor seria 1.1, separados por "|".
Exemplo: "1.2|1.0|1.15"
PRECO_COMPRA - Preço que consta na tabela de compra do fornecedor
COD_ERP_PROD_COMPRA - Código do produto utilizado na compra. Por exemplo, no produto
cerveja, pode trazer aqui o código do fardo de 12 cervejas
CURVA_CLIENTE - Curva de compra del cliente
PESO - peso producto x U. Compra
VOLUME - volumen procucto x U. Compra
*/


CREATE procedure [dbo].[SP_KIKKER_M_3_PRODUCTOS]
AS

BEGIN
SET NOCOUNT ON;

IF OBJECT_ID('tempdb..##TEMP_MERCADOLOGICO_KIKKER') IS NULL  
		BEGIN

CREATE TABLE ##TEMP_MERCADOLOGICO_KIKKER	(
	C_ARTICULO			decimal(10, 0) 	NULL DEFAULT 0,
	C_FAMILIA 			decimal(10, 0) 	NULL DEFAULT 0,
	C_RUBRO  			decimal(10, 0) 	NULL DEFAULT  0,
	C_SUBRUBRO_1  		decimal(10, 0) 	NULL DEFAULT  0,
	C_SUBRUBRO_2 		decimal(10, 0) 	NULL DEFAULT  0,
	C_SUBRUBRO_3		DECIMAL(10,0)	NULL DEFAULT 0,
	C_SUBRUBRO_4		DECIMAL(10,0)	NULL DEFAULT 0,
	N_FAMILIA     		CHAR(100)        NULL ,
	N_RUBRO      		CHAR(100)      	NULL ,
	N_SUBRUBRO_1     	CHAR(100)      	NULL ,
	N_SUBRUBRO_2     	CHAR(100)        NULL ,
	N_SUBRUBRO_3		CHAR(100)		NULL ,
	N_SUBRUBRO_4		CHAR(100)		NULL )

	INSERT INTO ##TEMP_MERCADOLOGICO_KIKKER
	SELECT	
	C_ARTICULO,
	case C_FAMILIA when 0 then 999 else C_FAMILIA end, 
	case C_RUBRO when 0 then 999 else C_RUBRO end, 
	case C_SUBRUBRO_1 when 0 then 999 else C_SUBRUBRO_1 end, 
	case C_SUBRUBRO_2 when 0 then 999 else C_SUBRUBRO_2 end, 
	case C_SUBRUBRO_3 when 0 then 999 else C_SUBRUBRO_3 end, 
	case C_SUBRUBRO_4 when 0 then 999 else C_SUBRUBRO_4 end,
	'SIN CLASIFICAR','SIN CLASIFICAR','SIN CLASIFICAR','SIN CLASIFICAR','SIN CLASIFICAR','SIN CLASIFICAR'
	FROM	[DIARCOP001].[DiarcoP].dbo.T050_ARTICULOS WITH(NOLOCK)
	WHERE	C_ARTICULO NOT IN (SELECT C_ARTICULO FROM [DIARCOP001].[DiarcoP].dbo.T050_ARTICULOS_DIFERENCIAS_DE_PRECIOS WITH(NOLOCK))

	UPDATE ##TEMP_MERCADOLOGICO_KIKKER
	   SET N_FAMILIA = T2.D_RUBRO /*,  C_FAMILIA = T2.C_RUBRO*/
	FROM ##TEMP_MERCADOLOGICO_KIKKER T1 , [DIARCOP001].[DiarcoP].dbo.T114_RUBROS T2 WITH(NOLOCK)
	WHERE T1.C_FAMILIA = T2.C_RUBRO
	AND	T2.C_RUBRO_PADRE = 0
	AND T2.C_RUBRO_NIVEL = 1
	
	UPDATE ##TEMP_MERCADOLOGICO_KIKKER
	   SET N_RUBRO = T2.D_RUBRO /*,C_RUBRO = T2.C_RUBRO*/
	FROM ##TEMP_MERCADOLOGICO_KIKKER T1 , [DIARCOP001].[DiarcoP].dbo.T114_RUBROS T2 WITH(NOLOCK)
	WHERE T1.C_RUBRO = T2.C_RUBRO
	  AND T1.C_RUBRO <> 0 
	
	UPDATE ##TEMP_MERCADOLOGICO_KIKKER
	   SET N_SUBRUBRO_1 = T2.D_RUBRO /*, C_SUBRUBRO_1=T2.C_RUBRO*/
	FROM ##TEMP_MERCADOLOGICO_KIKKER T1 , [DIARCOP001].[DiarcoP].dbo.T114_RUBROS T2 WITH(NOLOCK)
	WHERE T1.C_SUBRUBRO_1 = T2.C_RUBRO
	  AND T1.C_SUBRUBRO_1 <> 0
	
	UPDATE ##TEMP_MERCADOLOGICO_KIKKER
	   SET N_SUBRUBRO_2 = T2.D_RUBRO /*, C_SUBRUBRO_2=T2.C_RUBRO*/
	FROM ##TEMP_MERCADOLOGICO_KIKKER T1 , [DIARCOP001].[DiarcoP].dbo.T114_RUBROS T2 WITH(NOLOCK)
	WHERE T1.C_SUBRUBRO_2 = T2.C_RUBRO
	  AND T1.C_SUBRUBRO_2 <> 0

	UPDATE ##TEMP_MERCADOLOGICO_KIKKER
	   SET N_SUBRUBRO_3 = T2.D_RUBRO /*, C_SUBRUBRO_3=T2.C_RUBRO*/
	FROM ##TEMP_MERCADOLOGICO_KIKKER T1 , [DIARCOP001].[DiarcoP].dbo.T114_RUBROS T2 WITH(NOLOCK)
	WHERE T1.C_SUBRUBRO_3 = T2.C_RUBRO
	  AND T1.C_SUBRUBRO_3 <> 0

	UPDATE ##TEMP_MERCADOLOGICO_KIKKER
	   SET N_SUBRUBRO_4 = T2.D_RUBRO /*, C_SUBRUBRO_4 = T2.C_RUBRO*/
	FROM ##TEMP_MERCADOLOGICO_KIKKER T1 , [DIARCOP001].[DiarcoP].dbo.T114_RUBROS T2 WITH(NOLOCK)
	WHERE T1.C_SUBRUBRO_4 = T2.C_RUBRO
	  AND T1.C_SUBRUBRO_4 <> 0
		END;
	
	SELECT  'COD_ERP','DESCRICAO','COD_ERP_DEPARTAMENTO','COD_ERP_SETOR','COD_ERP_GRUPO','COD_ERP_SUB_GRUPO','UNIDADE_DE_VENDA','QTDE_UNID_VENDA','UNIDADE_DE_COMPRA','QTDE_UNID_COMPRA','CLASSIFICACAO','PRAZO_VALIDADE','PRAZO_ACEITACAO','PRAZO_RETIRADA_GONDOLA','COD_FORNECEDOR','EAN','PRODUTO_BASE','PROP_BAIXA_BASE','COD_ERP_PROD_COMPRA','PRECO_COMPRA','ERP_ID'
    UNION ALL 
	select 
	cast(art.c_articulo as varchar(10)), --COD_ERP
	dbo.[NORMALIZA_STRING](art.N_ARTICULO), --DESCRICAO
	convert (varchar,art.c_rubro), --COD_ERP_DEPARTAMENTO
	convert (varchar,art.c_Subrubro_1), --COD_ERP_SETOR
	convert (varchar,art.c_subrubro_2), --COD_ERP_GRUPO
	convert (varchar,est.c_subrubro_3), --COD_ERP_SUB_GRUPO
	dbo.[NORMALIZA_STRING](cod.D_CODIGO_ABREV), --UNIDADE_DE_VENDA
	convert (varchar,avg(suc.Q_FACTOR_VTA_SUCU)), --QTDE_UNID_VENDA
	dbo.[NORMALIZA_STRING](cod.D_CODIGO_ABREV), --UNIDADE_DE_COMPRA
	--convert (varchar,avg(prov.Q_FACTOR_PROVEEDOR)), --QTDE_UNID_COMPRA
	CASE WHEN ART.M_VENDE_POR_PESO = 'N' THEN convert (varchar,avg(prov.Q_FACTOR_PROVEEDOR)) ELSE CONVERT(VARCHAR,AVG(ART.Q_PESO_UNIT_ART)) END, /*SE AGREGA ESTA LINEA PARA IDENTIFICAR LO QUE VIENE POR PESABLE EL FACTOR SON LOS KILOS, SINO ES EL FACTOR PROVEEDOR EN UNIDADES POR BULTO.*/--QTDE_UNID_COMPRA
	convert (varchar,1), --CLASSIFICACAO
	'', --PRAZO_VALIDADE
	'', --PRAZO_ACEITACAO
	'', --PRAZO_RETIRADA_GONDOLA
	convert (varchar,art.C_PROVEEDOR_PRIMARIO),--COD_FORNECEDOR
	LTRIM(RTRIM(convert (varchar,art.C_EAN))), --EAN
	'', --PRODUTO_BASE
	'', --PROP_BAIXA_BASE
	convert (varchar,art.c_articulo),--codD_ERP_PROD_COMPRA
	convert (varchar,avg(COST.I_LISTA_CALCULADO)),--PRECO_COMPRA
	''
--	'', --CURVA_CLIENTE
--	'', --PESO
--	'' --VOLUME
	 from [DIARCOP001].[DiarcoP].dbo.t050_articulos art WITH(NOLOCK)
		inner join [DIARCOP001].[DiarcoP].dbo.T051_ARTICULOS_SUCURSAL suc  WITH(NOLOCK) on suc.C_ARTICULO=art.C_ARTICULO
		inner join [DIARCOP001].[DiarcoP].dbo.T052_ARTICULOS_PROVEEDOR prov WITH(NOLOCK) on art.C_PROVEEDOR_PRIMARIO=prov.C_PROVEEDOR and art.C_ARTICULO=prov.C_ARTICULO and SUC.C_ARTICULO=PROV.C_ARTICULO --07/09/2023 se agrega nuevos join a la consulta por articulo
		inner join [DIARCOP001].[DiarcoP].dbo.T055_ARTICULOS_CONDCOMPRA_COSTOS cost WITH(NOLOCK) on ART.C_ARTICULO=cost.C_ARTICULO and cost.C_PROVEEDOR=art.C_PROVEEDOR_PRIMARIO AND COST.C_SUCU_EMPR=SUC.C_SUCU_EMPR --07/09/2023 se agrega nuevos join a la consulta por sucursal
		inner join [DIARCOP001].[DiarcoP].dbo.T001_TABLA_CODIGO cod WITH(NOLOCK) on art.c_UNIDAD_MEDIDA=cod.C_CODIGO_TABLA and c_tabla=79
		inner join ##TEMP_MERCADOLOGICO_KIKKER EST WITH(NOLOCK) on EST.C_ARTICULO=art.C_ARTICULO and EST.C_RUBRO=art.C_RUBRO and EST.C_subrubro_1=art.C_subrubro_1 and EST.C_subrubro_2=art.C_subrubro_2
	group by
	art.c_articulo, --COD_ERP
	art.N_ARTICULO, --DESCRICAO
	art.c_rubro, --COD_ERP_DEPARTAMENTO
	art.c_subrubro_1, --COD_ERP_SETOR
	art.c_subrubro_2, --COD_ERP_GRUPO
	est.c_subrubro_3, --COD_ERP_SUB_GRUPO
	cod.D_CODIGO_ABREV, --UNIDADE_DE_VENDA
	cod.D_CODIGO_ABREV, --UNIDADE_DE_COMPRA
	art.C_PROVEEDOR_PRIMARIO,
	art.C_EAN ,--EAN 
	ART.M_VENDE_POR_PESO
	

	DROP TABLE  ##TEMP_MERCADOLOGICO_KIKKER

END;

GO


