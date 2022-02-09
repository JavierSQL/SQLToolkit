SELECT OBJECT_NAME(Oper.OBJECT_ID) AS TableName
	,   INDEXES.NAME AS IndexName
	,  Oper.partition_number
-- Costo	
	,  Oper.LEAF_INSERT_COUNT AS LeafInserts
     , Oper.LEAF_UPDATE_COUNT AS LeafUpdates
     , Oper.LEAF_DELETE_COUNT  AS LeafDeletes
-- Beneficio     
    ,  Usag.USER_SEEKS
    ,  Usag.USER_SCANS 
    ,  Usag.USER_LOOKUPS 
    ,  Usag.USER_UPDATES
FROM  SYS.INDEXES
left JOIN  SYS.DM_DB_INDEX_OPERATIONAL_STATS (DB_ID(),NULL,NULL,NULL ) AS Oper
 ON INDEXES.OBJECT_ID=Oper.OBJECT_ID 
    AND INDEXES.INDEX_ID=Oper.INDEX_ID 
left JOIN   SYS.DM_DB_INDEX_USAGE_STATS AS Usag
ON INDEXES.OBJECT_ID = Usag.OBJECT_ID 
	  AND INDEXES.INDEX_ID = Usag.INDEX_ID 
	  AND USAG.database_id= DB_ID()
WHERE  OBJECTPROPERTY(INDEXES.OBJECT_ID,'IsUserTable') = 1
	AND OBJECT_NAME(Oper.OBJECT_ID) IN (
						select TOP 50 sysobjects.name
						from sysobjects
						join sysindexes
						on sysobjects.id=sysindexes.id
						where xtype='U'
							AND sysindexes.indid<2 
						    --- AND sysobjects.name='TableName'
						group by sysobjects.name
						order by sum(rows) desc)
	   AND   (Usag.USER_SEEKS+Usag.USER_SCANS+Usag.USER_LOOKUPS )<10
order by OBJECT_NAME(Oper.OBJECT_ID), INDEXES.NAME



