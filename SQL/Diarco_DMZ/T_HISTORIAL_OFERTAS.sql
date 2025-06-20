/***  ACUMULAR en FORMA DIARIA OFERTAS Y VIGENCIAS ***/
/***  SOLO AGREGAR REGISTROS Que no existen previamente ***/
/*** Tipos de Precios 
	1	Precio Regular                                    	P.Regular 
	2	Precio Oferta                                     	P.Oferta  
	3	Precio Folder                                     	P.Folder  
	4	Precio Sepa                                       	P.Sepa    
***/

USE [data-sync]
GO

INSERT INTO [dbo].[T_HISTORIAL_OFERTAS]
           ([C_ARTICULO]
           ,[C_SUCU_EMPR]
           ,[F_VIGENCIA_DESDE]
           ,[F_VIGENCIA_HASTA]
           ,[C_TIPO_PRECIO]
           ,[I_PRECIO_NUEVO]
           ,[I_PRECIO_RETORNO]
           ,[PRECIO_RELATIVO]
           ,[C_USUARIO]
           ,[C_TERMINAL]
           ,[F_MODIF]
           ,[M_VIGENTE]
           ,[U_PAGINA])
SELECT [C_ARTICULO]
      ,[C_SUCU_EMPR]
      ,[F_VIGENCIA_DESDE]
      ,[F_VIGENCIA_HASTA]
      ,[C_TIPO_PRECIO]
      ,[I_PRECIO_NUEVO]
      ,[I_PRECIO_RETORNO]
      ,[I_PRECIO_NUEVO]/[I_PRECIO_RETORNO] as PRECIO_RELATIVO
      ,[C_USUARIO]
      ,[C_TERMINAL]
      ,[F_MODIF]
      ,[M_VIGENTE]
      ,[U_PAGINA]
FROM [DIARCOP001].[DiarcoP].[dbo].[T900_PRECIOS_VIGENCIA] AS SRC
WHERE NOT EXISTS (
    SELECT 1
    FROM [dbo].[T_HISTORIAL_OFERTAS] AS DEST
    WHERE DEST.[C_ARTICULO] = SRC.[C_ARTICULO]
      AND DEST.[C_SUCU_EMPR] = SRC.[C_SUCU_EMPR]
      AND DEST.[F_VIGENCIA_DESDE] = SRC.[F_VIGENCIA_DESDE]
      AND DEST.[F_VIGENCIA_HASTA] = SRC.[F_VIGENCIA_HASTA]
      AND DEST.[C_TIPO_PRECIO] = SRC.[C_TIPO_PRECIO]
      AND DEST.[I_PRECIO_NUEVO] = SRC.[I_PRECIO_NUEVO]
      AND DEST.[I_PRECIO_RETORNO] = SRC.[I_PRECIO_RETORNO]
)
GO
