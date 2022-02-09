DECLARE @starttime AS DATETIME = '20211213'
DECLARE @endtime AS DATETIME   = '20211231'

SELECT TOP 20
    qt.query_sql_text AS QueryText
 , OBJECT_NAME(q.object_id) AS ObjectName
  , (SUM(rs.avg_cpu_time*rs.count_executions))/1000.0 AS Executions
  , SUM(rs.avg_duration*rs.count_executions)/1000.0 AS TotalDuration
  , (SUM(rs.avg_duration*rs.count_executions))
	/ (SUM(rs.count_executions)* 1000.0) AS [AvgDuration(ms)]
  , MAX(rs.max_duration)/1000.0 AS [MaxDuration(ms)]
  , (SUM(rs.avg_cpu_time*rs.count_executions))/1000.0 AS TotalCPU
  , (SUM(rs.avg_cpu_time*rs.count_executions))
	/ (SUM(rs.count_executions)* 1000.0) AS [AvgCPUTime(ms)]
  , MAX(rs.max_cpu_time)/1000.0 AS  [MaxCPUTime(ms)]
  , SUM(rs.avg_logical_io_reads*rs.count_executions) AS TotalLogicalReads
  , (SUM(rs.avg_logical_io_reads*rs.count_executions))
	/ (SUM(rs.count_executions)) AS [AvgLogicalReads]
  , SUM(rs.avg_logical_io_writes*rs.count_executions) AS TotalLogicalWrites
  , (SUM(rs.avg_logical_io_writes*rs.count_executions))
	/ (SUM(rs.count_executions)) AS [AvgLogicalWrites]

  , SUM(rs.avg_physical_io_reads*rs.count_executions) AS TotalPhysicalReads
  , (SUM(rs.avg_physical_io_reads*rs.count_executions))
	/ (SUM(rs.count_executions)) AS [AvgPhysicalReads]


  , (SUM(rs.avg_query_max_used_memory*rs.count_executions))
	/ (SUM(rs.count_executions)) AS [Avg_Memory(KB)]
  , MAX(rs.max_query_max_used_memory)/1000.0 AS [Max_Memory(KB)]	

  , SUM(rs.avg_rowcount*rs.count_executions) AS TotalRowCount
  , (SUM(rs.avg_rowcount*rs.count_executions))
	/ (SUM(rs.count_executions)) AS [AvgRowCount]
  , MAX(rs.max_rowcount) AS [MaxRowCount]	
 
/*
--  ,rs.avg_log_bytes_used / 1024 AS [Avg_Log_Used (KB)]    --Only for Azure SQL Database
--  ,rs.max_log_bytes_used / 1024 AS [Max_Log_Used (KB)]    --Only for Azure SQL Database
  ,p.plan_id AS [Plan_ID]
  ,rs.last_execution_time AS [Last_Execution_Time]

  ,q.avg_compile_duration / 1000 AS [Avg_Compile_Duration(ms)]
  ,p.compatibility_level AS [Compatibility_Level]
  ,rs.avg_dop AS [Avg_DOP]
*/ 
FROM sys.query_store_query_text AS qt
INNER JOIN sys.query_store_query AS q 
	ON qt.query_text_id = q.query_text_id
INNER JOIN sys.query_store_plan AS p 
	ON q.query_id = p.query_id
INNER JOIN sys.query_store_runtime_stats AS rs 
	ON p.plan_id = rs.plan_id
WHERE rs.last_execution_time BETWEEN @starttime  AND @endtime
	--AND qt.query_sql_text LIKE '%objectName%'
GROUP BY   qt.query_sql_text
  , OBJECT_NAME(q.object_id) 
ORDER BY SUM(rs.avg_logical_io_reads*rs.count_executions) DESC
--  SUM(rs.avg_logical_io_reads*rs.count_executions) DESC
-- SUM(rs.avg_logical_io_writes*rs.count_executions) desc
-- (SUM(rs.avg_cpu_time*rs.count_executions))/1000.0 DESC
-- (SUM(rs.avg_cpu_time*rs.count_executions))/1000.0 DESC
--- SUM(rs.avg_logical_io_reads*rs.count_executions) DESC

