# Databricks notebook source
from datetime import datetime

# COMMAND ----------

# MAGIC %run "/Shared/Database Config"

# COMMAND ----------

# Setup a connection to ADLS
spark.conf.set(
  ADLSConnectionURI,
  ADLSConnectionKey
)

# COMMAND ----------

# Cleanup the widgets
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC dbutils.widgets.text("TableName", "","")
# MAGIC lazy val GoldDimTableName = dbutils.widgets.get("TableName")
# MAGIC lazy val GoldFactTableName = "Gold.FCT_NX_INV_LINE_ITEM_TRANS"
# MAGIC print (GoldDimTableName)
# MAGIC print (GoldFactTableName)

# COMMAND ----------

# Get the parameters from ADF
now = datetime.now() # current date and time

dbutils.widgets.text("TableName", "","")
GoldDimTableName = dbutils.widgets.get("TableName")

# Set the path for Silver layer for Nexsure
sourceSilverPath = "Client/Nexsure/DimEntity/" +now.strftime("%Y") + "/" + now.strftime("%m")
sourceSilverPath = SilverContainerPath + sourceSilverPath

sourceSilverFile = "DimEntity_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
sourceSilverFilePath = sourceSilverPath + "/" + sourceSilverFile

dbutils.widgets.text("BatchId", "","")
BatchId = dbutils.widgets.get("BatchId")
dbutils.widgets.text("WorkFlowId", "","")
WorkFlowId = dbutils.widgets.get("WorkFlowId")

#Set the file path for logging 
badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"

date_time = now.strftime("%Y%m%dT%H%M%S")
badRecordsFilePath = badRecordsPath + date_time + "/" + "ErrorRecords"
#badRecordsPath = "abfss://c360logs@dlsldpdev01v8nkg988.dfs.core.windows.net/Dim_NX_Carrier/"
#badRecordsFilePath = "abfss://c360logs@dlsldpdev01v8nkg988.dfs.core.windows.net/Dim_NX_Carrier/" + date_time
recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"

print ("Param -\'Variables':")
print (sourceSilverFilePath)
#print (SelectQuery)
print (badRecordsPath)
print (badRecordsFilePath)
print (recordCountFilePath)

# COMMAND ----------

# Temporary cell - DELETE
now = datetime.now() 
GoldDimTableName = "Dim_NX_Carrier"
GoldFactTableName = "FCT_NX_INV_LINE_ITEM_TRANS"
sourceSilverPath = "Client/Nexsure/DimEntity/" +now.strftime("%Y") + "/05"
sourceSilverPath = SilverContainerPath + sourceSilverPath
sourceSilverFile = "DimEntity_2021_05_25.parquet"
sourceSilverFilePath = sourceSilverPath + "/" + sourceSilverFile
badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"
recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"
BatchId = "1afc2b6c-d987-48cc-ae8c-a7f41ea27249"
WorkFlowId ="8fc2895d-de32-4bf4-a531-82f0c6774221"

# COMMAND ----------

# MAGIC %scala
# MAGIC // Temporary cell - DELETE
# MAGIC lazy val GoldDimTableName = "Dim_NX_Carrier"

# COMMAND ----------

 # Do not proceed if any of the parameters are missing
if (GoldDimTableName == "" or sourceSilverPath == "" or sourceSilverFile == ""):
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Input parameters are missing"}}})

# COMMAND ----------

# Read source file
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")

try:
  sourceSilverDF = spark.read.option("badRecordsPath", badRecordsPath).schema("EntityKey int,EntityID int,EntityClass string,EntityName string,EntityType string,DBSourceKey int,AuditKey int").parquet(sourceSilverFilePath)
#  display(sourceSilverDF)
#  sourceSilverDF.printSchema
except:
  print("Schema mismatch")
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Schema mismatch: " + sourceSilverFilePath}}})

# COMMAND ----------

# Register table so it is accessible via SQL Context
sourceSilverDF.createOrReplaceTempView("DIM_NX_CARRIER")

# COMMAND ----------

dummyDataDF = spark.sql(
f""" 
SELECT
 -99999 AS CARIER_KEY
,-1 AS CARIER_ID
,-1 AS CARIER_CLAS
,-1 AS CARIER_NAME
,-1 AS CARIER_TYP
,-1 AS DB_SRC_KEY
,-1 AS SRC_AUDT_KEY
,'{ BatchId }' AS ETL_BATCH_ID
,'{ WorkFlowId }' AS ETL_WRKFLW_ID
,current_timestamp() AS ETL_CREATED_DT
,current_timestamp() AS ETL_UPDATED_DT
from DIM_NX_CARRIER e LIMIT 1
"""
)

# COMMAND ----------

# Get final set of records
finalDataDF = spark.sql(
f""" 
SELECT
 e.EntityKey AS CARIER_KEY
,e.EntityID AS CARIER_ID
,e.EntityClass AS CARIER_CLAS
,e.EntityName AS CARIER_NAME
,e.EntityType AS CARIER_TYP
,e.DBSourceKey AS DB_SRC_KEY
,e.AuditKey AS SRC_AUDT_KEY
,'{ BatchId }' AS ETL_BATCH_ID
,'{ WorkFlowId }' AS ETL_WRKFLW_ID
,current_timestamp() AS ETL_CREATED_DT
,current_timestamp() AS ETL_UPDATED_DT
from DIM_NX_CARRIER e
where e.EntityClass = 'Carrier'
"""
)

# COMMAND ----------

# Do not proceed if there are no records to insert
if (finalDataDF.count() == 0):
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "There are no records to insert: " + sourceSilverFilePath}}})

# COMMAND ----------

# Create a dataframe for record count
sourceRecordCount = sourceSilverDF.count()
targetRecordCount = finalDataDF.count()
#errorRecordCount = errorDataDF.count()
recordCountDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceRecordCount,targetRecordCount,sourceSilverFilePath,BatchId,WorkFlowId)
  ],["TableName","DateTime","SourceRecordCount","TargetRecordCount","Filename","BatchId","WorkflowId"])

# Write the record count to ADLS
recordCountDF.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(recordCountFilePath)

# COMMAND ----------

# MAGIC %scala
# MAGIC // Truncate Fact table and Delete data from Dimension table
# MAGIC lazy val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
# MAGIC lazy val stmt = connection.createStatement()
# MAGIC //val sql = "truncate table Gold.FCT_NX_INV_LINE_ITEM_TRANS; delete from Gold.DIM_NX_INV_LINE_ITEM_ENTITY; DBCC CHECKIDENT ('Gold.DIM_NX_INV_LINE_ITEM_ENTITY', RESEED, 0)"
# MAGIC //val sql = "truncate table " + GoldFactTableName + "; delete from " + GoldDimTableNameComplete + "; DBCC CHECKIDENT ('" + GoldDimTableNameComplete + "', RESEED, 0)";
# MAGIC lazy val sql_truncate = "truncate table " + GoldFactTableName
# MAGIC stmt.execute(sql_truncate)
# MAGIC lazy val sql = "exec [Admin].[DropAndCreateFKContraints] @GoldTableName = '" + GoldDimTableName + "'"
# MAGIC stmt.execute(sql)
# MAGIC connection.close()

# COMMAND ----------

GoldDimTableNameComplete = "gold." + GoldDimTableName
dummyDataDF.write.jdbc(url=Url, table=GoldDimTableNameComplete, mode="append")
finalDataDF.write.jdbc(url=Url, table=GoldDimTableNameComplete, mode="append")