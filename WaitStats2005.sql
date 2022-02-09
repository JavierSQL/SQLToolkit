SET NOCOUNT ON; 

DECLARE     @hours tinyint
     , @minutes  tinyint
     , @seconds  tinyint 
 
-- Cambiar la duracion
SET @hours = 0
SET @minutes = 30
SET @seconds = 0

-- 
-- Definición de variable table
--
DECLARE @t TABLE (
id int identity
, wait_type nvarchar(60)
, waiting_tasks_count bigint
, wait_time_ms bigint
, signal_wait_time_ms bigint)
 
-- 
-- Inserción de captura inicial
--
INSERT @t
(    wait_type
     , waiting_tasks_count
     , wait_time_ms
     , signal_wait_time_ms )
SELECT 
     wait_type
     , waiting_tasks_count
     , wait_time_ms
     , signal_wait_time_ms
FROM
     sys.dm_os_wait_stats
 
--
-- A esperar n tiempo
--
DECLARE @s CHAR(8)
SET @s = 
      RIGHT ('00' + CAST (@hours as VARCHAR(2)), 2) + ':'
     + RIGHT ('00' + CAST (@minutes as VARCHAR(2)), 2) + ':'
     + RIGHT ('00' + CAST (@seconds as VARCHAR(2)), 2)
 
WAITFOR DELAY @s
 
-- 
-- Inserción de segunda captura
--
INSERT @t
(    wait_type
     , waiting_tasks_count
     , wait_time_ms
     , signal_wait_time_ms )
SELECT 
     wait_type
     , waiting_tasks_count
     , wait_time_ms
     , signal_wait_time_ms
FROM
     sys.dm_os_wait_stats
 
--
-- calculos finales
--
;WITH detalle AS ( 
SELECT * FROM (
-- TOTALES POR TIPO DE ESPERA
SELECT 
     T1.wait_type
     , AVG(T2.waiting_tasks_count - T1.waiting_tasks_count) 
           waiting_tasks_count
     , AVG(T2.wait_time_ms - T1.wait_time_ms) 
wait_time_ms
, AVG(T2.signal_wait_time_ms - T1.signal_wait_time_ms) 
signal_wait_time_ms
FROM @T t1
JOIN @T t2
  ON T1.wait_type = T2.wait_type
 AND T1.id < T2.id
GROUP BY T1.wait_type
) v
WHERE 
     wait_time_ms <> 0
),
 suma AS (
-- TOTALES GENERALES (PARA CALCULO DE PORCENTAJES)
SELECT 
     SUM(waiting_tasks_count) waiting_tasks_count
     , SUM(wait_time_ms) wait_time_ms
     , SUM(signal_wait_time_ms) signal_wait_time_ms
FROM detalle
)
-- CALCULOS FINALES
SELECT 
     detalle.*
     , detalle.wait_time_ms * 1.00 / detalle.waiting_tasks_count as wait_per_request
     , CASE WHEN suma.waiting_tasks_count = 0 
      THEN 0 
      ELSE detalle.waiting_tasks_count * 1.00 / suma.waiting_tasks_count 
    END as porcen_waiting_tasks_count
     , CASE WHEN suma.wait_time_ms = 0 
      THEN 0 
      ELSE detalle.wait_time_ms * 1.00 /  suma.wait_time_ms 
    END as porcen_wait_time_ms
     , CASE WHEN suma.signal_wait_time_ms = 0 
      THEN 0 
      ELSE detalle.signal_wait_time_ms * 1.00 / suma.signal_wait_time_ms 
    END as porcen_signal_wait_time_ms
FROM detalle
, suma
