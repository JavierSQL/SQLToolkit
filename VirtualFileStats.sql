USE MASTER
GO
DECLARE 
     @hours tinyint
     , @minutes  tinyint
     ,@seconds  tinyint 

SET @hours=0
SET @minutes=0
SET @seconds=30
 
SET NOCOUNT ON;

DECLARE @BaseDatos SYSNAME
DECLARE @dbId INT
DECLARE @FileID INT
DECLARE @ComandoFiles NVARCHAR(200)
SET NOCOUNT ON;

CREATE TABLE #Archivos (
	  dbId 		INT
	, BaseDatos 	SYSNAME
	, fileid 	smallint NOT NULL
	, groupid 	smallint NOT NULL
	, size 		int NOT NULL 
	, maxsize 	int NOT NULL 
	, growth 		int NOT NULL 
	, status 		int NOT NULL 
	, perf 		int NOT NULL 
	, name 		nchar (128) 
	, filename 	nchar (260)
	, PRIMARY KEY (dbId, fileid)
)
CREATE TABLE #FileStatsToma1 (
	DbId 		smallint NOT NULL 
	, FileId 	smallint NOT NULL
	, TimeStamp 		int NOT NULL 
	, NumberReads 		bigint NOT NULL 
	, BytesRead 		bigint NOT NULL 
	, IoStallReadMS 	bigint NOT NULL 
	, NumberWrites 		bigint NOT NULL 
	, BytesWritten 		bigint NOT NULL 
	, IoStallWriteMS 	bigint NOT NULL 
	, IoStallMS 		bigint NOT NULL 
	, BytesOnDisk		bigint NOT NULL
	, FileHandle		binary(8)
	, PRIMARY KEY (dbId, fileid)
) 

CREATE TABLE #FileStatsToma2 (
	DbId 		smallint NOT NULL 
	, FileId 	smallint NOT NULL
	, TimeStamp 		int NOT NULL 
	, NumberReads 		bigint NOT NULL 
	, BytesRead 		bigint NOT NULL 
	, IoStallReadMS 	bigint NOT NULL 
	, NumberWrites 		bigint NOT NULL 
	, BytesWritten 		bigint NOT NULL 
	, IoStallWriteMS 	bigint NOT NULL 
	, IoStallMS 		bigint NOT NULL 
	, BytesOnDisk		bigint NOT NULL
	, FileHandle		binary(8)
	, PRIMARY KEY (dbId, fileid)
) 

DECLARE BasesDatos CURSOR FORWARD_ONLY
FOR SELECT Name, dbId FROM SYSDATABASES
OPEN BasesDatos

FETCH NEXT FROM BasesDatos INTO @BaseDatos, @dbId
WHILE @@FETCH_STATUS=0
	BEGIN
	SET @ComandoFiles= N'INSERT INTO #Archivos SELECT '+ CAST(@dbId AS VARCHAR(3)) + ', '''+@BaseDatos +''', * FROM ['+ @BaseDatos+ ']..SYSFILES'
	EXEC SP_EXECUTESQL @ComandoFiles
	FETCH NEXT FROM BasesDatos INTO @BaseDatos, @dbId
	END
CLOSE BasesDatos
DEALLOCATE BasesDatos

-- Toma1 de datos
DECLARE ArchivoStats1 CURSOR FORWARD_ONLY
FOR SELECT dbId, FileId FROM #Archivos
OPEN ArchivoStats1

FETCH NEXT FROM ArchivoStats1 INTO @dbId, @FileID
WHILE @@FETCH_STATUS=0
	BEGIN
	SET @ComandoFiles= N'INSERT INTO #FileStatsToma1 SELECT * FROM ::fn_virtualfilestats('+ CAST(@dbId AS VARCHAR(3))+ ', '+CAST(@FileID AS VARCHAR(3))+')'
	EXEC SP_EXECUTESQL @ComandoFiles
	FETCH NEXT FROM ArchivoStats1 INTO @dbId, @FileID
	END
CLOSE ArchivoStats1
DEALLOCATE ArchivoStats1

-- Espera 
DECLARE @s CHAR(8)
SET @s =   RIGHT ('00' + CAST (@hours as VARCHAR(2)), 2) + ':'
     + RIGHT ('00' + CAST (@minutes as VARCHAR(2)), 2) + ':'
     + RIGHT ('00' + CAST (@seconds as VARCHAR(2)), 2)
WAITFOR DELAY @s

-- Toma 2
DECLARE ArchivoStats2 CURSOR FORWARD_ONLY
FOR SELECT dbId, FileId FROM #Archivos

OPEN ArchivoStats2
FETCH NEXT FROM ArchivoStats2 INTO @dbId, @FileID
WHILE @@FETCH_STATUS=0
	BEGIN
	SET @ComandoFiles= N'INSERT INTO #FileStatsToma2 SELECT * FROM ::fn_virtualfilestats('
			+ CAST(@dbId AS VARCHAR(3))+ ', '+CAST(@FileID AS VARCHAR(3))+')'
	EXEC SP_EXECUTESQL @ComandoFiles
	FETCH NEXT FROM ArchivoStats2 INTO @dbId, @FileID
	END

CLOSE ArchivoStats2
DEALLOCATE ArchivoStats2
SELECT A.*
	, (ISNULL(T2.NumberReads, 0)	- ISNULL(T1.NumberReads, 0)) AS NumberReads
	, (ISNULL(T2.BytesRead, 0)		- ISNULL(T1.BytesRead, 0)) AS BytesRead
	, (ISNULL(T2.IoStallReadMS, 0)  - ISNULL(T1.IoStallReadMS, 0)) AS IoStallReadMS
	, (ISNULL(T2.NumberWrites, 0)	- ISNULL(T1.NumberWrites, 0)) AS NumberWrites
	, (ISNULL(T2.BytesWritten, 0)	- ISNULL(T1.BytesWritten, 0)) AS BytesWritten
	, (ISNULL(T2.IoStallWriteMS, 0) - ISNULL(T1.IoStallWriteMS, 0)) AS BytesWritten
	, (ISNULL(T2.IoStallMS, 0)		- ISNULL(T1.IoStallMS, 0)) AS IoStallMS
	, (ISNULL(T2.BytesOnDisk, 0)    - ISNULL(T1.BytesOnDisk, 0)) AS BytesOnDisk
FROM #Archivos AS A
LEFT JOIN #FileStatsToma1 T1
ON  A.dbId=T1.DbId
    AND A.fileid=T1.FileId
LEFT JOIN #FileStatsToma2 T2
ON  A.dbId=T2.DbId
    AND A.fileid=T2.FileId

DROP TABLE #Archivos
DROP TABLE #FileStatsToma1
DROP TABLE #FileStatsToma2

