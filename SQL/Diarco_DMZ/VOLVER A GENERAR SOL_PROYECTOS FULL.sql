USE [CAPEX]
GO

ALTER TABLE [dbo].[SOL_Proyectos] DROP CONSTRAINT [FK_SOL_Proyectos_SOL_Sectores]
GO

ALTER TABLE [dbo].[SOL_Proyectos] DROP CONSTRAINT [FK_SOL_Proyectos_SOL_Organizaciones]
GO

ALTER TABLE [dbo].[SOL_Proyectos] DROP CONSTRAINT [FK_SOL_Proyectos_SOL_Estados]
GO

ALTER TABLE [dbo].[SOL_Proyectos] DROP CONSTRAINT [FK_SOL_Proyectos_SOL_Areas]
GO

/** Buscar CONTRAINST **/

SELECT * 
FROM sys.foreign_keys
WHERE referenced_object_id = object_id('SOL_Proyectos')

SELECT 
    'ALTER TABLE [' +  OBJECT_SCHEMA_NAME(parent_object_id) +
    '].[' + OBJECT_NAME(parent_object_id) + 
    '] DROP CONSTRAINT [' + name + ']'
FROM sys.foreign_keys
WHERE referenced_object_id = object_id('SOL_Proyectos')

ALTER TABLE [dbo].[SOL_Estimacion] DROP CONSTRAINT [FK_SOL_Estimacion_SOL_Proyectos]
ALTER TABLE [dbo].[SOL_Planificacion] DROP CONSTRAINT [FK_SOL_Planificacion_SOL_Proyectos]
ALTER TABLE [dbo].[SOL_Presupuesto_BASE] DROP CONSTRAINT [FK_SOL_Presupuesto_BASE_SOL_Proyectos]

/****** Object:  Table [dbo].[SOL_Proyectos]    Script Date: 25/11/2021 10:32:18 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[SOL_Proyectos]') AND type in (N'U'))
DROP TABLE [dbo].[SOL_Proyectos]
GO

/****** Object:  Table [dbo].[SOL_Proyectos]    Script Date: 25/11/2021 10:32:18 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[SOL_Proyectos](
	[ID_Proyecto] [int] IDENTITY(1,1) NOT NULL,
	[PROJECT_NUMBER] [varchar](25) NULL,
	[PROYECTO_NOMBRE] [varchar](30) NULL,
	[NOMBRE_LARGO] [varchar](150) NULL,
	[DESCRIPCION] [varchar](250) NULL,
	[ESTADO_ID] [int] NULL,
	[PROYECTO_STATUS] [varchar](20) NULL,
	[AREA_ID] [int] NULL,
	[SECTOR_ID] [int] NULL,
	[ORGANIZACION_ID] [int] NULL,
	[PROYECTO_TYPE] [varchar](20) NULL,
	[PROYECTO_TYPE_DESCRIPTION] [varchar](100) NULL,
	[MONTO_BASE] [numeric](15, 2) NULL,
	[MONTO_ESTIMADO] [numeric](15, 2) NULL,
	[MONTO_PRESUPUESTADO] [numeric](15, 2) NULL,
	[PROYECTO_CON_PRESUPUESTO] [varchar](25) NULL,
	[FECHA_INICIO_BASE] [date] NULL,
	[FECHA_INICIO_PLAN] [date] NULL,
	[FECHA_INICIO_REAL] [date] NULL,
	[DURACION_ESTIMADA_Dias] [int] NULL,
	[INGRESO_DATE] [datetime2](7) NULL,
	[ESTIMADO_DATE] [datetime2](7) NULL,
	[PRESUPUESTADO_DATE] [datetime2](7) NULL,
	[COMPLETADO_DATE] [datetime2](7) NULL,
	[ACTUALIZADO_DATE] [datetime2](7) NULL,
	[ACTUALIZADO_POR] [varchar](25) NULL,
	[CREADO_DATE] [datetime2](7) NULL,
	[CREADO_POR] [varchar](25) NULL,
	[FECHA_SQL] [varchar](25) NULL,
 CONSTRAINT [PK_SOL_Proyectos] PRIMARY KEY CLUSTERED 
(
	[ID_Proyecto] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[SOL_Proyectos]  WITH CHECK ADD  CONSTRAINT [FK_SOL_Proyectos_SOL_Areas] FOREIGN KEY([AREA_ID])
REFERENCES [dbo].[SOL_Areas] ([AREA_ID])
GO

ALTER TABLE [dbo].[SOL_Proyectos] CHECK CONSTRAINT [FK_SOL_Proyectos_SOL_Areas]
GO

ALTER TABLE [dbo].[SOL_Proyectos]  WITH CHECK ADD  CONSTRAINT [FK_SOL_Proyectos_SOL_Estados] FOREIGN KEY([ESTADO_ID])
REFERENCES [dbo].[SOL_Estados] ([ESTADO_ID])
GO

ALTER TABLE [dbo].[SOL_Proyectos] CHECK CONSTRAINT [FK_SOL_Proyectos_SOL_Estados]
GO

ALTER TABLE [dbo].[SOL_Proyectos]  WITH CHECK ADD  CONSTRAINT [FK_SOL_Proyectos_SOL_Organizaciones] FOREIGN KEY([ORGANIZACION_ID])
REFERENCES [dbo].[SOL_Organizaciones] ([ORGANIZACION_ID])
GO

ALTER TABLE [dbo].[SOL_Proyectos] CHECK CONSTRAINT [FK_SOL_Proyectos_SOL_Organizaciones]
GO

ALTER TABLE [dbo].[SOL_Proyectos]  WITH CHECK ADD  CONSTRAINT [FK_SOL_Proyectos_SOL_Sectores] FOREIGN KEY([SECTOR_ID])
REFERENCES [dbo].[SOL_Sectores] ([SECTOR_ID])
GO

ALTER TABLE [dbo].[SOL_Proyectos] CHECK CONSTRAINT [FK_SOL_Proyectos_SOL_Sectores]
GO


