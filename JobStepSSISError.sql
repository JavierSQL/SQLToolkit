CREATE PROC dbo.JobStepSSISError
AS
/*********************************************************************************************
JobStepSSISError	V1.00 (2021-07-14)
(C) (2021-), Javier Loria
Feedback: mailto:javier@loria.com
Description: This procedure helps report SSIS errors when packages are executed from
             a job step. Instead of the usual:
				Package execution on IS Server failed. Execution ID: <ID>, Execution Status:4.  
				To view the details for the execution, right-click on the Integration Services Catalog, 
				and open the [All Executions] report
			The job will return the following erro:
				Job <JobName> Failed: Step: <StepName>, Package: <Package Name>.
				              Error Message: <SSIS Error Messages>
Usage:       EXEC dbo.JobStepSSISError MUST BE THE FIRST STEP OF THE JOB.
			 All other steps, or at least the steps that use SSIS packages,
			 must be configured to: On Failure Go To Step 1.

License: 
	This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses
*********************************************************************************************/
DECLARE @JobID UNIQUEIDENTIFIER
	 , @JobName  sysname
	 , @StepName sysname
	 , @FullSSISCommand VARCHAR(4000)
	 , @PackageName NVARCHAR(260)
	 , @OperationID BIGINT
	 , @ExecutionPath nvarchar(4000)
	 , @NotInJobErrorMessage  NVARCHAR(4000)= N'Use in Job Step'
	 , @SSISErrorMessage NVARCHAR(4000)='Job %s Failed: Step: %s, Package: %s.'+CHAR(13)+' Error Message:'+CHAR(13)+'%s'
	 , @OtherErrorMessage NVARCHAR(4000)=N'Job %s Failed: Step: %s'
	 , @Messages VARCHAR(MAX);

SET XACT_ABORT, NOCOUNT ON;

/* Get JobID and JobName */
SELECT @JobID = Job_id, @JobName=name
FROM	msdb.sys.dm_exec_sessions AS JSes
INNER JOIN msdb.dbo.sysjobs AS jobs 
ON jobs.job_id = Cast(CONVERT(BINARY(16)
							, SUBSTRING(JSes.program_name
											, CHARINDEX('(Job 0x',JSes.program_name,1) + 5 
											, 34),1) as UNIQUEIDENTIFIER)
WHERE	    JSes.session_id = @@SPID  -- Testing: 75
		AND JSes.program_name like 'SQLAgent - TSQL JobStep%';

IF @JobID IS NULL
	BEGIN
	/* Not in Job */
	RAISERROR (@NotInJobErrorMessage,16, 1);
	RETURN 1;
	END
ELSE
BEGIN
    /* Get last SSIS step that failed */
	SELECT TOP 1  @FullSSISCommand=JS.Command
	 , @StepName=js.step_name
	FROM msdb.dbo.sysjobhistory as JH
	JOIN msdb.dbo.sysjobsteps AS JS
	 ON JH.Job_id = JS.job_id
	 AND JH.step_id=JS.step_id
	WHERE	JH.Run_status = 0  -- Failed
		  AND JH.Step_id > 1  -- 0=Job outcome, 1= Error Reporting
		  AND JH.Job_id = @JobID
		  AND JS.Command LIKE '%.dtsx%' -- Only DTSX Errors
	 ORDER BY JS.step_id DESC;
	 /* If it is a SSIS package */
	 IF @FullSSISCommand IS NOT NULL
		  BEGIN
		  /* Hack to get PackageName */
		  SELECT @PackageName=S4.S4
		  FROM (SELECT SUBSTRING(@FullSSISCommand,1, CHARINDEX('.dtsx', @FullSSISCommand)+4)
								AS S1) AS  S1
		  CROSS APPLY (SELECT REVERSE(S1.S1)) AS S2(S2)
		  CROSS APPLY (SELECT SUBSTRING(S2.S2,1, CHARINDEX('\', S2.S2)-1)) AS S3(S3)
		  CROSS APPLY (SELECT REVERSE(S3.S3)) AS S4(S4)

		  SELECT TOP 1 @OperationID=operation_id
				, @ExecutionPath=execution_path
		  FROM	SSISDB.catalog.Event_messages
		  WHERE Package_name= @PackageName
	 			AND Event_name = 'OnError'
				AND message_time>=DATEADD(HOUR, -1, GETDATE())
		  ORDER BY event_message_id DESC;
		  SELECT  event_message_id,  message
		  INTO #Errors
		  FROM	SSISDB.catalog.Event_messages
		  WHERE	operation_id=@OperationID
		  			 AND Event_name = 'OnError';

		   --- SQL 2019+ Replace with STRING_AGG
		  ;WITH Numbered AS (
				select cast(message as varchar(max)) as message
					 , ROW_NUMBER() OVER(ORDER BY event_message_id) AS Rn
	 				 , ROW_NUMBER() OVER(ORDER BY event_message_id desc) AS RnR
				from #Errors
		  ), AggString AS (
				SELECT  message, RN, RnR
				FROM	   Numbered
				WHERE	  rn=1
				UNION ALL
				SELECT CONCAT(A.message, N.message) AS Message
					 , n.Rn
					 , n.RnR
				FROM AggString AS A
				JOIN Numbered AS N
					 ON A.Rn+1=N.Rn
		  )
		  SELECT @Messages=message
		  FROM	 AggString
		  WHERE	 RnR=1
		  RAISERROR (@SSISErrorMessage, 16, 1
					, @JobName, @StepName, @PackageName, @Messages);  
	END
	ELSE
		 BEGIN
		 RAISERROR (@OtherErrorMessage, 16, 1
					, @JobName, @StepName);
	END
	RETURN 0;
END

