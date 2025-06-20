select * from area; --no tienefk
select * from Asignaturas;--fk con area
select * from Encargado;--no tienefk
select * from Estudiantes;--fk con encargado fk con profesiones y fk con staff
select * from Profesiones; -- no tiene fk
select * from Staff;-- fk con profesion, encargado y asignatura
--JOIN 1
select ag.Jornada, count (*) as cantidad_docentes, sum(ag.costo) as suma_total
from Staff sf
join Asignaturas ag on sf.Asignatura = ag.AsignaturasID
where ag.Nombre like 'Desarrollo Web'
group by ag.jornada;


-- Funciona SIN AS tambien
--JOIN 1
select ag.Jornada, count (*) cantidad_docentes, sum(ag.costo) as suma_total
from Staff as sf
join Asignaturas ag on sf.Asignatura = ag.AsignaturasID
where ag.Nombre like 'Desarrollo Web'
group by ag.jornada;

-- JOIN 2
select ec.Encargado_ID, ec.Nombre, ec.Apellido, count(*) as cant_docentes
from Encargado ec
join Staff as sf
on sf.Encargado = ec.Encargado_ID
group by ec.Encargado_ID, ec.Nombre, ec.Apellido
--having count(*) > 1
order by cant_docentes desc;

-- QUERY 3
select ag.*
from staff sf
right join Asignaturas ag on sf.Asignatura = ag.AsignaturasID
where sf.DocentesID is null

-- QUERY 4
Select CONCAT(sf.Apellido, ',',sf.Nombre) as Encargado 

from Staff sf


-- QUERY 5
select nombre, apellido, documento, 'staff' as marca_keywords
from staff 
union
select nombre, apellido, documento, 'encargado' as marca_keywords
from encargado
union
select nombre, apellido, documento, 'estudiantes' as marca_keywords
from Estudiantes;

select [Fecha de Nacimiento], count(*) as cant_estudiantes
from estudiantes
group by [Fecha de Nacimiento]
order by cant_estudiantes desc


select year([Fecha de Nacimiento]) as anio, month([Fecha de Nacimiento]) as mes,day([Fecha de Nacimiento]) as dia, count(*) as cant_estudiantes
from estudiantes
group by [Fecha de Nacimiento]
order by cant_estudiantes desc

select year([Fecha Ingreso]) as anio, month([Fecha Ingreso]) as mes,day([Fecha Ingreso]) as dia, count(*) as cant_estudiantes
from estudiantes
group by [Fecha Ingreso]
order by cant_estudiantes desc

select top 10 encargado, count(*) as cant_docentes_a_cargo
from staff
group by encargado
order by cant_docentes_a_cargo desc;

select top 10 encargado, count(*) as cant_docentes_a_cargo
from staff
group by encargado
order by cant_docentes_a_cargo desc;

-- profesiones con más estudiantes
select profesion, count(*) as cant_estudiantes
from estudiantes
group by profesion
having count(*) > 5
order by cant_estudiantes desc

-- profesiones con más estudiantes
select ec.profesion, pr.Profesiones, count(*) as cant_estudiantes
from estudiantes ec --renombro tabla
join profesiones pr on ec.Profesion = pr.ProfesionesID
group by ec.profesion, pr.Profesiones
having count(*) > 5
order by cant_estudiantes desc


--4
-- profesiones con más estudiantes
select ec.profesion, pr.Profesiones, count(*) as cant_estudiantes
from estudiantes ec --renombro tabla
join profesiones pr on ec.Profesion = pr.ProfesionesID
group by ec.profesion, pr.Profesiones
having count(*) > 5
order by cant_estudiantes desc

/* 1° vamos a staff , 2° vamos a asignatura, 3° vamos a area */
select ar.Nombre as nombre_area, ag.Nombre as nombre_asignatura, ag.Tipo, ag.Jornada, ag.costo, count(*) as cant_estudiantes
from estudiantes ec
right join staff sf on ec.Docente = sf.DocentesID
left join Asignaturas ag on ag.AsignaturasID = sf.Asignatura
join area ar on ar.AreaID = ag.Area
group by ar.Nombre , ag.Nombre , ag.Tipo, ag.Jornada,ag.costo
order by ag.costo desc

-- QUERYS NIVEL TACTICO
---------------------NIVEL TACTICO-------------------------
--1 análisis mensual de estudiantes por área
select format(ec.[Fecha Ingreso], 'yyyyMM')as fecha, ag.costo, count(*) as cant_estudiantes
from estudiantes ec
join staff sf on ec.Docente = sf.DocentesID
join Asignaturas ag on ag.AsignaturasID = sf.Asignatura
join area ar on ar.AreaID = ag.Area
group by format(ec.[Fecha Ingreso], 'yyyyMM'), ag.costo
order by cant_estudiantes desc

--2 análisis encargado tutores jornada noche
select ec.Nombre, ec.Apellido, ec.Documento, replace(camada, 'camada', '') as camada, ag.Jornada
from Encargado ec
join staff sf on sf.Encargado = ec.Encargado_ID
join Asignaturas ag on ag.AsignaturasID = sf.Asignatura
where trim(ec.Tipo) = 'Encargado Tutores' 
--and ag.Jornada = 'Noche'
order by camada desc


/****** 3 FORMAS E#QUIVALENTES ******/
SELECT-- [AsignaturasID]
      --,[Nombre],
      [Tipo]
      ,[Jornada]
 --     ,[Costo]
 --     ,[Area]
	,COUNT (AsignaturasID)
  FROM [CoderHouse].[dbo].[Asignaturas] 
  WHERE [AsignaturasID] NOT IN (Select Asignatura FROM [CoderHouse].[dbo].[Staff])
  GROUP BY Tipo, Jornada
  ORDER BY Tipo DESC

  --3 análisis asignaturas sin docentes ni tutores
select ag.Tipo, ag.Jornada, count(*) as cant_asignaturas
from staff sf
right join Asignaturas ag on sf.Asignatura = ag.AsignaturasID
where sf.DocentesID is null
group by ag.Tipo, ag.Jornada

--3 análisis asignaturas sin docentes ni tutores
select ag.Tipo, ag.Jornada, count(*) as cant_asignaturas
from Asignaturas ag
left join staff sf  on sf.Asignatura = ag.AsignaturasID
where sf.DocentesID is null
group by ag.Tipo, ag.Jornada

--4 Análisis de asignaturas mayor al promedioselect ag1.nombre, ag1.costo, ag1.tipo, ag1.Jornada, ag1.area from Asignaturas ag1where ag1.costo > (select AVG(ag2.costo) as promedio from Asignaturas ag2 where ag1.Area = ag2.Area);
select ar.AreaID, ar.Nombre, avg(costo) as promediofrom Asignaturas join area ar on ar.AreaID = Areagroup by ar.AreaID,ar.nombre;

--5 Análisis aumento de salario docenteselect sf.Nombre, sf.Apellido, sf.Documento, ag.AsignaturasID, ag.Jornada, ag.Nombre, ar.AreaID, ar.Nombre, ag.Costo,	case		when trim(ar.Nombre) = 'Marketing Digital' then CAST((ag.Costo * 1.17) as decimal (4,2))		when trim(ar.Nombre) like '%Diseño UX%' then CAST((ag.Costo * 1.2) as decimal (4,2))		when trim(ar.Nombre) = 'Programacion' then CAST((ag.Costo * 1.23) as decimal (4,2))		when trim(ar.Nombre) = 'Producto' then CAST((ag.Costo * 1.13) as decimal (4,2))		when trim(ar.Nombre) = 'Data' then CAST((ag.Costo * 1.15) as decimal (4,2))		when trim(ar.Nombre) = 'Herramientas' then CAST((ag.Costo * 1.08) as decimal (4,2))	end as nuevo_salariofrom staff sfjoin Asignaturas ag on sf.Asignatura = ag.AsignaturasIDjoin Area ar on ar.AreaID = ag.Areaorder by ag.Nombre;