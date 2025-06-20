SELECT TOP (1000) 
	[C_PROVEEDOR]
      ,[N_PROVEEDOR]
		,[ID_TIENDA]
	   ,[SUC_ABREV]
      ,[SUC_NOMBRE]
      ,'en Pallet' as PALLET_x_CAMION

  FROM [data-sync].[dbo].[M_91_SUCURSALES], [data-sync].[dbo].[T020_PROVEEDOR]
  WHere [C_PROVEEDOR] IN (2676, 190, 6363, 3835)
