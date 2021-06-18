# Databricks notebook source
from datetime import datetime

# COMMAND ----------

# MAGIC %run "/Project/Database Config"

# COMMAND ----------

spark.conf.set(
  ADLSConnectionURI,
  ADLSConnectionKey
)

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.widgets.text("TableName", "","")
# MAGIC lazy val GoldDimTableName = dbutils.widgets.get("TableName")

# COMMAND ----------

# Set the path for Silver layer for Nexsure

now = datetime.now() 
#sourceSilverPath = "Invoice/Nexsure/DimRateType/2021/05"
sourceSilverFolderPath = "Invoice/Nexsure/DimRateType/" +now.strftime("%Y") + "/" + now.strftime("%m")
sourceSilverPath = SilverContainerPath + sourceSilverFolderPath

sourceSilverFile = "DimEntity_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
#sourceSilverFile = "DimRateType_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_25.parquet"
sourceSilverFilePath = sourceSilverPath + "/" + sourceSilverFile

dbutils.widgets.text("TableName", "","")
GoldDimTableName = dbutils.widgets.get("TableName")

dbutils.widgets.text("BatchId", "","")
BatchId = dbutils.widgets.get("BatchId")

dbutils.widgets.text("WorkFlowId", "","")
WorkFlowId = dbutils.widgets.get("WorkFlowId")

#Set the file path to log error
badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"


now = datetime.now() # current date and time
date_time = now.strftime("%Y%m%dT%H%M%S")
badRecordsFilePath = badRecordsPath + date_time + "/" + "ErrorRecords"
#badRecordsPath = "abfss://c360logs@dlsldpdev01v8nkg988.dfs.core.windows.net/Dim_NX_Rate_Type/"
#badRecordsFilePath = "abfss://c360logs@dlsldpdev01v8nkg988.dfs.core.windows.net/Dim_NX_Rate_Type/" + date_time
recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"

#Set the file path to log error
#badRecordsPath = badRecordsRootPath + "/" + sourceTable + "/"

print ("Param -\'Variables':")
print (sourceSilverFilePath)
print (badRecordsFilePath)
print (recordCountFilePath)

# COMMAND ----------

# Temporary cell to run manually - DELETE
if (GoldDimTableName == "" or sourceSilverPath == "" or sourceSilverFile == ""):
  now = datetime.now() 
  GoldDimTableName = "DIM_NX_RATE_TYPE"
  GoldFactTableName = "FCT_NX_INV_LINE_ITEM_TRANS"
  sourceSilverPath = "Invoice/Nexsure/DimRateType/" +now.strftime("%Y") + "/05"
  sourceSilverPath = SilverContainerPath + sourceSilverPath
  sourceSilverFile = "DimRateType_2021_05_25.parquet"
  sourceSilverFilePath = sourceSilverPath + "/" + sourceSilverFile
  badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"
  recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"
  BatchId = "1afc2b6c-d987-48cc-ae8c-a7f41ea27249"
  WorkFlowId ="8fc2895d-de32-4bf4-a531-82f0c6774221"
  sourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Invoice/Nexsure/DimRateType/2021/06/DimRateType_2021_06_04.parquet"

# COMMAND ----------

# MAGIC %scala
# MAGIC // Temporary cell to run manually - DELETE
# MAGIC if (GoldDimTableName == "") {
# MAGIC  val GoldDimTableName = "Dim_NX_RATE_TYPE"
# MAGIC }  

# COMMAND ----------

# Do not proceed if any of the parameters are missing
if (GoldDimTableName == "" or sourceSilverPath == "" or sourceSilverFile == ""):
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Input parameters are missing"}}})

# COMMAND ----------

# Read source file
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
try:
 
   sourceSilverDF = spark.read.parquet(sourceSilverFilePath)
   #display(sourceSilverDF)
except:
  # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceSilverFilePath}}})  

# COMMAND ----------

sourceSilverDF.createOrReplaceTempView("DIM_NX_RATE_TYPE")

# COMMAND ----------

dummyDataDF = spark.sql(
f""" 
SELECT
-99999 as NX_RATE_TYP_KEY,
-1 as NX_RATE_TYP_ID,
-1 as DB_SRC_KEY,
-1 as SRC_AUDT_KEY,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId }' AS ETL_WRKFLW_ID,
current_timestamp() AS ETL_CREATED_DT,
current_timestamp() AS ETL_UPDATED_DT
FROM DIM_NX_RATE_TYPE e LIMIT 1
"""
)
#display(dummyDataDF)

# COMMAND ----------

# Get final set of records
finalDataDF = spark.sql(
f""" 
SELECT
RateTypeKey as NX_RATE_TYP_KEY,
RateTypeID as NX_RATE_TYP_ID,
RateType  as RATE_TYP,
DBSourceKey as DB_SRC_KEY,
AuditKey as SRC_AUDT_KEY,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId }' AS ETL_WRKFLW_ID,
current_timestamp() AS ETL_CREATED_DT,
current_timestamp() AS ETL_UPDATED_DT
FROM DIM_NX_RATE_TYPE
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
reconDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceRecordCount,targetRecordCount,sourceSilverFilePath,BatchId,WorkFlowId)
  ],["TableName","ETL_CREATED_DT","SourceRecordCount","TargetRecordCount","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID"])

# Write the recon record to SQL DB
reconDF.write.jdbc(url=Url, table=reconTable, mode="append")

# COMMAND ----------

# MAGIC %scala
# MAGIC // Truncate Fact table and Delete data from Dimension table
# MAGIC lazy val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
# MAGIC lazy val stmt = connection.createStatement()
# MAGIC //lazy val sql_truncate = "truncate table " + finalTableSchema + "." + "FCT_NX_INV_LINE_ITEM_TRANS"
# MAGIC //stmt.execute(sql_truncate)
# MAGIC lazy val sql = "exec " + finalTableSchema + ".[DropAndCreateFKContraints] @GoldTableName = '" + GoldDimTableName + "'"
# MAGIC stmt.execute(sql)
# MAGIC connection.close()

# COMMAND ----------

GoldDimTableNameComplete = finalTableSchema + "." + GoldDimTableName
dummyDataDF.write.jdbc(url=Url, table=GoldDimTableNameComplete, mode="append")
finalDataDF.write.jdbc(url=Url, table=GoldDimTableNameComplete, mode="append")

# COMMAND ----------

# Write the final parquet file to Gold zone
dbutils.widgets.text("ProjectFolderName", "","")
sourceGoldPath = dbutils.widgets.get("ProjectFolderName")
dbutils.widgets.text("ProjectFileName", "","")
sourceGoldFile = dbutils.widgets.get("ProjectFileName")

spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInWrite=CORRECTED")
sourceGoldFilePath = GoldContainerPath + sourceGoldPath + "/" + sourceGoldFile
finalDataDF.write.mode("overwrite").parquet(sourceGoldFilePath)