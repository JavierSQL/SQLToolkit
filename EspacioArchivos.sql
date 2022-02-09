USE tempdb
GO
 ---- Archivos
SELECT      DB.name, mf.Physical_name
            , CONVERT(VARCHAR,SUM(size)*8/1024)+' MB' AS [Total disk space]
			, growth
			, is_percent_growth
FROM        sys.databases AS DB
JOIN        sys.master_files as mf
ON          Db.database_id=Mf.database_id
GROUP BY    db.name, mf.Physical_name, growth, is_percent_growth
ORDER BY    db.name, mf.Physical_name

--- Volumenes
SELECT DISTINCT
       SERVERPROPERTY('MachineName') AS MachineName
     , ISNULL(SERVERPROPERTY('InstanceName'), 'MSSQLSERVER') AS InstanceName
     , vs.volume_mount_point AS VolumeName
     , vs.logical_volume_name AS VolumeLabel
     , vs.total_bytes AS VolumeCapacity
     , vs.available_bytes AS VolumeFreeSpace
     , CAST(vs.available_bytes * 100.0 / vs.total_bytes AS DECIMAL(5, 2)) AS PercentageFreeSpace
FROM sys.master_files AS mf
     CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.file_id) AS vs;




/*
SET NOCOUNT ON;

DECLARE @DBName varchar(100) 
DECLARE @Command nvarchar(200) 

DROP TABLE IF EXISTS #DBFiles
CREATE TABLE #DBFiles(
	DatabaseName SYSNAME			NOT NULL
	, FileName	 VARCHAR(1014)		NOT NULL
	, Size		 Int				NOT NULL
)
DECLARE CadaBD CURSOR FOR 
	SELECT name 
	FROM MASTER.sys.sysdatabases 
	WHERE name NOT IN ('master', 'tempdb', 'msdb', 'model')
OPEN CadaBD 
FETCH NEXT FROM CadaBD INTO @DBName 
	WHILE @@FETCH_STATUS = 0 
	BEGIN 
		 SELECT @Command = 'SELECT ' + '''' + @DBName + '''' + ', SF.filename, SF.size FROM sys.sysfiles SF'
		 INSERT #DBFiles
		 EXEC sp_executesql @Command 

		 FETCH NEXT FROM CadaBD INTO @DBName 
	END 

CLOSE CadaBD 
DEALLOCATE CadaBD

*/
