# Databricks notebook source
from datetime import datetime

# COMMAND ----------

# MAGIC %run "/Shared/Database Config"

# COMMAND ----------

spark.conf.set(
  ADLSConnectionURI,
  ADLSConnectionKey
)

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC dbutils.widgets.text("TableName", "","")
# MAGIC val GoldDimTableName = dbutils.widgets.get("TableName")
# MAGIC val GoldFactTableName = "Gold.FCT_NX_INV_LINE_ITEM_TRANS"
# MAGIC print (GoldDimTableName)
# MAGIC print (GoldFactTableName)

# COMMAND ----------

# Set the path for Silver layer for Nexsu

now = datetime.now() 
#sourceSilverPath = "OrgStructure/Nexsure/DimOrgStructure/2021/05"
sourceSilverPath = "OrgStructure/Nexsure/DimOrgStructure/" +now.strftime("%Y") + "/" + now.strftime("%m")
sourceSilverPath = SilverContainerPath + sourceSilverPath

sourceSilverFile = "DimOrgStructure_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
#sourceSilverFile = "DimOrgStructure_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_21.parquet"
sourceSilverFilePath = sourceSilverPath + "/" + sourceSilverFile

dbutils.widgets.text("BatchId", "","")
BatchId = dbutils.widgets.get("BatchId")

dbutils.widgets.text("WorkFlowId", "","")
WorkFlowId = dbutils.widgets.get("WorkFlowId")

dbutils.widgets.text("TableName", "","")
GoldDimTableName = dbutils.widgets.get("TableName")

#Set the file path to log error
badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"


now = datetime.now() # current date and time
date_time = now.strftime("%Y%m%dT%H%M%S")
badRecordsFilePath = badRecordsPath + date_time + "/" + "ErrorRecords"
#badRecordsPath = "abfss://c360logs@dlsldpdev01v8nkg988.dfs.core.windows.net/Dim_NX_ORG/"
#badRecordsFilePath = "abfss://c360logs@dlsldpdev01v8nkg988.dfs.core.windows.net/Dim_NX_ORG/" + date_time
recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"

#Set the file path to log error
#badRecordsPath = badRecordsRootPath + "/" + sourceTable + "/"

print ("Param -\'Variables':")
print (sourceSilverFilePath)
print (badRecordsFilePath)
print (recordCountFilePath)

# COMMAND ----------

# Temporary cell - DELETE
now = datetime.now() 
GoldDimTableName = "DIM_NX_ORG"
GoldFactTableName = "FCT_NX_INV_LINE_ITEM_TRANS"
sourceSilverPath = "OrgStructure/Nexsure/DimOrgStructure/" +now.strftime("%Y") + "/05"
sourceSilverPath = SilverContainerPath + sourceSilverPath
sourceSilverFile = "DimOrgStructure_2021_05_21.parquet"
sourceSilverFilePath = sourceSilverPath + "/" + sourceSilverFile
badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"
recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"
BatchId = "1afc2b6c-d987-48cc-ae8c-a7f41ea27249"
WorkFlowId ="8fc2895d-de32-4bf4-a531-82f0c6774221"

# COMMAND ----------

# MAGIC %scala
# MAGIC // Temporary cell - DELETE
# MAGIC val GoldDimTableName = "Dim_NX_ORG"

# COMMAND ----------

 # Do not proceed if any of the parameters are missing
if (GoldDimTableName == "" or sourceSilverPath == "" or sourceSilverFile == ""):
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Input parameters are missing"}}})

# COMMAND ----------

 # Read source file
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
try:
 
   sourceSilverDF = spark.read.option("badRecordsPath", badRecordsPath).schema("OrgStructureKey int,DepartmentNum int,DepartmentName string,BranchNum int,BranchName string,RegionName string,OrgName string,EntityLevelName string,DBSourceKey int,AuditKey int").parquet(sourceSilverFilePath)
   #display(sourceSilverDF)
except:
  print("Schema mismatch")
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Schema mismatch: " + sourceSilverFilePath}}})

# COMMAND ----------

# Register table so it is accessible via SQL Context
sourceSilverDF.createOrReplaceTempView("DIM_NX_ORG")
#display(sourceSilverDF)

# COMMAND ----------

dummyDataDF = spark.sql(
f""" 
SELECT
-99999 AS ORG_KEY,
-1 As DEPT_NO,
-1 as DB_SRC_KEY,
-1 as SRC_AUDT_KEY,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId }' AS ETL_WRKFLW_ID,
current_timestamp() AS ETL_CREATED_DT,
current_timestamp() AS ETL_UPDATED_DT
FROM DIM_NX_ORG e LIMIT 1
"""
)
#display(dummyDataDF)

# COMMAND ----------

# Get final set of records
finalDataDF = spark.sql(
f""" 
SELECT
OrgStructureKey AS ORG_KEY,
DepartmentNum As DEPT_NO,
DepartmentName As DEPT_NAME,
BranchNum As BRNCH_NO,
BranchName As BRNCH_NAME,
RegionName As REGION_NAME,
OrgName	As ORG_NAME,
EntityLevelName	As ENTY_LVL_NAME,
DBSourceKey As DB_SRC_KEY,
AuditKey As SRC_AUDT_KEY,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId }' AS ETL_WRKFLW_ID,
current_timestamp() AS ETL_CREATED_DT,
current_timestamp() AS ETL_UPDATED_DT
FROM DIM_NX_ORG
"""
)
#display(finalDataDF)

# COMMAND ----------

# Do not proceed if there are no records to insert
if (finalDataDF.count() == 0):
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "There are no records to insert: " + sourceSilverFilePath}}})

# COMMAND ----------

# Create a dataframe for record count
sourceRecordCount = sourceSilverDF.count()
targetRecordCount = finalDataDF.count()
recordCountDF = spark.createDataFrame([
    (sourceRecordCount,targetRecordCount)
  ],["SourceRecordCount","TargetRecordCount"])

# Write the record count to ADLS
recordCountDF.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(recordCountFilePath)

# COMMAND ----------

# MAGIC %scala
# MAGIC // Truncate Fact table and Delete data from Dimension table
# MAGIC val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
# MAGIC val stmt = connection.createStatement()
# MAGIC //val sql = "truncate table Gold.FCT_NX_INV_LINE_ITEM_TRANS; delete from Gold.DIM_NX_INV_LINE_ITEM_ENTITY; DBCC CHECKIDENT ('Gold.DIM_NX_INV_LINE_ITEM_ENTITY', RESEED, 0)"
# MAGIC //val sql = "truncate table " + GoldFactTableName + "; delete from " + GoldDimTableNameComplete + "; DBCC CHECKIDENT ('" + GoldDimTableNameComplete + "', RESEED, 0)";
# MAGIC val sql_truncate = "truncate table " + GoldFactTableName
# MAGIC stmt.execute(sql_truncate)
# MAGIC val sql = "exec [Admin].[DropAndCreateFKContraints] @GoldTableName = '" + GoldDimTableName + "'"
# MAGIC stmt.execute(sql)
# MAGIC connection.close()

# COMMAND ----------

GoldDimTableNameComplete = "gold." + GoldDimTableName
dummyDataDF.write.jdbc(url=Url, table=GoldDimTableNameComplete, mode="append")
finalDataDF.write.jdbc(url=Url, table=GoldDimTableNameComplete, mode="append")