SELECT TOP 20
	 creation_time
	, total_elapsed_time
	, last_execution_time
	,  execution_count
	, total_elapsed_time/execution_count AS [Avg total_elapsed_time] 
	 ,[total_logical_reads]
	, [total_logical_reads]/execution_count as [Avg logical Reads]
	, [total_logical_writes]
	, [total_logical_writes]/execution_count as [Avg logical Writes] 
	, (SELECT SUBSTRING(text,  (statement_start_offset+2) /2,
		(CASE WHEN statement_end_offset = -1 THEN LEN(CONVERT(nvarchar(max),
			text)) * 2 
			ELSE statement_end_offset END - statement_start_offset)/2)
			FROM sys.dm_exec_sql_text(sql_handle)) AS query_text
FROM sys.dm_exec_query_stats
order by total_logical_reads desc


