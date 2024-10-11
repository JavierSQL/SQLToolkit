-- =====================================================================================
--  Author: Javier Loria (JavierSQL)
-- Date: 2024-10-01
-- License: MIT License
-- =====================================================================================
-- File: JobExecutionHistory.SQL
-- Description: This script creates a table named JobExecutionHistory to store the 
--              execution history of jobs, including details such as job ID, job name,
--              step ID, step name, run status, run date, run time, run duration, 
--              error messages, database name, and subsystem. It also defines a stored 
--              procedure named LoadJobExecutionHistory to load data into this table.
--              Use this script in Azure SQL Managed Instance since:
--				"SQL Server Agent settings are read only."
--              "The procedure sp_set_agent_properties isn't supported in SQL Managed Instance."
--              Use this script in Azure SQL Server managed instance or SQL Server on-premises
--              in a DBA database and schecdule a job to execute the LoadJobExecutionHistory regularly.

-- MIT License
-- 
-- Permission is hereby granted, free of charge, to any person obtaining a copy
-- of this software and associated documentation files (the "Software"), to deal
-- in the Software without restriction, including without limitation the rights
-- to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
-- copies of the Software, and to permit persons to whom the Software is
-- furnished to do so, subject to the following conditions:
-- 
-- The above copyright notice and this permission notice shall be included in all
-- copies or substantial portions of the Software.
-- 
-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
-- IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
-- FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
-- AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
-- LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
-- OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
-- SOFTWARE.

-- Table: dbo.JobExecutionHistory
-- Columns:
--   - InstanceID: Unique identifier for the instance.
--   - Server: Name of the server where the job was executed.
--   - JobID: Unique identifier for the job.
--   - JobName: Name of the job.
--   - StepID: Identifier for the job step. 0 is the job itself.
--   - StepName: Name of the job step.
--   - RunStatus: Status of the job run (e.g., Succeeded, Failed). Filtered: 4 = In Progress
--   - RunDate: Date when the job was run.
--   - RunTime: Time when the job was run.
--   - RunDurationSeconds: Duration of the job run in seconds.
--   - ErrorMessage: Error message if the job failed.
--   - database_name: Name of the database associated with the job step.
--   - Subsystem: Subsystem used by the job step.
-- 
-- Stored Procedure: LoadJobExecutionHistory
-- Description: Inserts data into the JobExecutionHistory table from the job history 
--              and related tables, ensuring that duplicate records are not inserted.
-- =====================================================================================


DROP TABLE  IF EXISTS dbo.JobExecutionHistory;

CREATE TABLE dbo.JobExecutionHistory(
	InstanceID			int					NOT NULL,
	Server				sysname				NOT NULL,
	JobID				uniqueidentifier	NOT NULL,
	JobName				sysname				NOT NULL,
	StepID				int					NOT NULL,
	StepName			sysname				NOT NULL,
	RunStatus			varchar(9)			NOT NULL,
	RunDate				date				NULL,
	RunTime				varchar(8)			NULL,
	RunDurationSeconds	int					NULL,
	ErrorMessage		nvarchar(4000)		NULL,
	database_name		sysname				NULL,
	Subsystem			nvarchar(40)		NOT NULL
	CONSTRAINT PK_JobExecutionHistory 
		PRIMARY KEY (InstanceID, server)
		WITH(DATA_COMPRESSION=PAGE)
			-- Required for HAG that may run jobs in multiple servers. 
			-- Single Server may use InstanceId only.
)
GO

CREATE OR ALTER PROC LoadJobExecutionHistory
AS
BEGIN
	SET XACT_ABORT, NOCOUNT ON;
	BEGIN TRY
		INSERT dbo.JobExecutionHistory
		( InstanceID, Server, JobID, JobName, StepID, StepName, RunStatus, RunDate, RunTime
		, RunDurationSeconds, ErrorMessage, database_name, Subsystem)
		SELECT
			sh.instance_id,
			sh.server,
			j.job_id	AS JobID,
			j.name		AS JobName,
			sh.step_id	AS StepID,
			sh.step_name AS StepName,
			CASE sh.run_status
				WHEN 0 THEN 'Failed'
				WHEN 1 THEN 'Succeeded'
				WHEN 2 THEN 'Retry'
				WHEN 3 THEN 'Canceled'
				ELSE 'Unknown'
			END AS RunStatus,
			-- Convert run_date to a DATE type
			CONVERT(DATE, CAST(sh.run_date AS CHAR(8)), 112) AS RunDate,
			-- Format run_time as HH:MM:SS
			STUFF(STUFF(RIGHT('000000' 
				+ CAST(sh.run_time AS VARCHAR(6)), 6), 3, 0, ':'), 6, 0, ':') AS RunTime,
			shp.RunDurationSeconds,
			sh.message AS ErrorMessage,
			js.database_name,
			-- Determine the subsystem, default to 'job' if NULL
			COALESCE(js.subsystem,'job') as Subsystem
		FROM msdb.dbo.sysjobs AS j
		INNER JOIN msdb.dbo.sysjobhistory AS sh 
			ON j.job_id = sh.job_id
		-- Left because step_id =0 is the job itself
		LEFT JOIN msdb.dbo.sysjobsteps AS js
			ON j.job_id=js.job_id
			AND js.step_id=sh.step_id
		-- Calculate run duration in seconds
		CROSS APPLY (
			SELECT	(sh.run_duration / 10000) * 3600 -- convert hours to seconds, can be greater than 24
				+ ((sh.run_duration % 10000) / 100) * 60 -- convert minutes to seconds
				+ (sh.run_duration % 100) AS RunDurationSeconds
				) AS shp
		WHERE sh.run_status IN (0, 1,2,3) 
			AND NOT EXISTS (SELECT	*
							FROM dbo.JobExecutionHistory AS D
								WHERE  d.InstanceID=sh.instance_id
									and d.Server=sh.server);
		END TRY
		BEGIN CATCH
			DECLARE @ErrorMessage NVARCHAR(4000);
			DECLARE @ErrorSeverity INT;
			DECLARE @ErrorState INT;
			SELECT 
				@ErrorMessage = ERROR_MESSAGE(),
				@ErrorSeverity = ERROR_SEVERITY(),
				@ErrorState = ERROR_STATE();

			RAISERROR (@ErrorMessage, -- Message text.
						@ErrorSeverity, -- Severity.
						@ErrorState -- State.
						);
			RETURN -1;
		END CATCH
	RETURN 0;
END
--- EXEC LoadJobExecutionHistory
