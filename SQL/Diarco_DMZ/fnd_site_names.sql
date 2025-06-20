SELECT [C_SUCU_EMPR] as code
      ,FORMAT(C_SUCU_EMPR, '000') + ' - ' + N_SUCURSAL_ABREV + ' - ' + N_SUCURSAL_ABREV2 AS name
 
 INTO repl.fnd_site_names      

  FROM [data-sync].[repl].[T100_EMPRESA_SUC]
