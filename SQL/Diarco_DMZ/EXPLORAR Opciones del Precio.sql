/*** EXPLORAR OPCIONES DE PRECIOS sobre SERVIDOR [DIARCOP001-BK] ****/

SELECT TOP 1000 *
  FROM [DIARCOP001-BK].[DiarcoP].[dbo].[T801_HIST_CAMB_PRECIOS]
  WHERE F_CAMBIO_PRECIO > '20240101'

SELECT TOP 1000 *
  FROM [DIARCOP001-BK].[DiarcoP].[dbo].[T900_PRECIOS]

SELECT TOP 1000 *
  FROM [DIARCOP001-BK].[DiarcoP].[dbo].[T900_PRECIOS_VIGENCIA]

SELECT TOP 1000 *
  FROM [DIARCOP001-BK].[DiarcoP].[dbo].[T710_ESTADIS_PRECIOS]
  WHERE C_ANIO >= '2024'

SELECT TOP 1000 *
  FROM [DIARCOP001-BK].[DiarcoP].[dbo].[T710_ESTADIS_OFERTA_FOLDER]
    WHERE C_ANIO >= '2024'

SELECT TOP 1000 *
  FROM [DIARCOP001-BK].[DiarcoP].[dbo].[T001_TABLA_CODIGO]
  WHERE C_TABLA = 137  
  /*** Tipos de Precios 
		1	Precio Regular                                    	P.Regular 
		2	Precio Oferta                                     	P.Oferta  
		3	Precio Folder                                     	P.Folder  
		4	Precio Sepa                                       	P.Sepa    
  ***/

  SELECT TOP 1000 *
  FROM [DIARCOP001-BK].[DiarcoP].[dbo].[T001_TABLA_CODIGO]
  WHERE C_TABLA = 36  
  /*** Tipos de Precios 
	1	Estaod OC Pendiente                               	Pendiente 
	2	Estado OC Cerrada                                 	Cumplida  
	3	Estado OC Anulada                                 	Anulada   
  ***/
SELECT TOP 1000 *
  FROM [DIARCOP001-BK].[DiarcoP].

SELECT TOP 1000 *
  FROM [DIARCOP001-BK].[DiarcoP].


SELECT TOP 1000 *
  FROM [DIARCOP001-BK].[DiarcoP].