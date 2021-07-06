# Databricks notebook source
from datetime import datetime

# COMMAND ----------

# MAGIC %run "/Project/Database Config"

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
# MAGIC var GoldDimTableName = dbutils.widgets.get("TableName")
# MAGIC 
# MAGIC // USE WHEN RUN IN DEBUG MODE
# MAGIC if (RunInDebugMode != "No") {
# MAGIC   GoldDimTableName = "BP_NX_REF_CARRIER_MAPPING"
# MAGIC }

# COMMAND ----------

# Get the parameters from ADF
now = datetime.now() # current date and time

dbutils.widgets.text("TableName", "","")
GoldDimTableName = dbutils.widgets.get("TableName")

# Set the path for Silver layer for Nexsure
sourceSilverFolderPath = "Carrier/MDS2/Mapping_Carrier/" +now.strftime("%Y") + "/" + now.strftime("%m")
sourceSilverPath = SilverContainerPath + sourceSilverFolderPath

sourceSilverFile = "Mapping_Carrier_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
sourceSilverFilePath = sourceSilverPath + "/" + sourceSilverFile

dbutils.widgets.text("BatchId", "","")
BatchId = dbutils.widgets.get("BatchId")
dbutils.widgets.text("WorkFlowId", "","")
WorkFlowId = dbutils.widgets.get("WorkFlowId")

#Set the file path for logging 
badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"

date_time = now.strftime("%Y%m%dT%H%M%S")
badRecordsFilePath = badRecordsPath + date_time + "/" + "ErrorRecords"
recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"

print ("Param -\'Variables':")
print (sourceSilverFilePath)
#print (SelectQuery)
print (badRecordsPath)
print (badRecordsFilePath)
print (recordCountFilePath)

# COMMAND ----------

# USE WHEN RUN IN DEBUG MODE
if (RunInDebugMode != 'No'):
  now = datetime.now() 
  GoldDimTableName = "BP_NX_REF_CARRIER_MAPPING"
  badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"
  recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"
  BatchId = "1afc2b6c-d987-48cc-ae8c-a7f41ea27249"
  WorkFlowId ="8fc2895d-de32-4bf4-a531-82f0c6774221"
  sourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Carrier/MDS2/Mapping_Carrier/" + yymmManual + "/Mapping_Carrier_" + yyyymmddManual + ".parquet"

# COMMAND ----------

# Read source file
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")

try:
 
  sourceSilverDF = spark.read.parquet(sourceSilverFilePath)
 # display(sourceSilverDF)
except:
  # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  #dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceSilverFilePath}}}) 

# COMMAND ----------

# Register table so it is accessible via SQL Context
sourceSilverDF.createOrReplaceTempView("BP_NX_REF_CARRIER_MAPPING")

# COMMAND ----------

# Get final set of records
finalDataDF = spark.sql(
f""" 
SELECT
CarrierSourceID as CARIER_SRC_ID  ,
CarrierName as CARIER_NAME  ,
Source as SRC  ,
ValidPremium as VLD_PREM  ,
Description as CARIER_DESC  ,
AMBestNumber as AM_BST_NUM  ,
BCO_Category as BCO_CATG  ,
PreferredWholesaler as PREF_WHOL_SALER  ,
PreferredGroup as PREF_GRP  ,
PS_BCO_Code as PS_BCO_CD  ,
PS_BCO_Description as PS_BCO_DESC  ,
PS_CarrierStatus as PS_CARIER_STATUS  ,
StartDate as STRT_DT  ,
EndDate as END_DT  
,'{ BatchId }' AS ETL_BATCH_ID
,'{ WorkFlowId }' AS ETL_WRKFLW_ID
,current_timestamp() AS ETL_CREATED_DT
,current_timestamp() AS ETL_UPDATED_DT
from BP_NX_REF_CARRIER_MAPPING
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
# MAGIC lazy val GoldDimTableNameComplete = finalTableSchema + "." + GoldDimTableName
# MAGIC // Truncate table and Delete data from Dimension table
# MAGIC lazy val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
# MAGIC lazy val stmt = connection.createStatement()
# MAGIC //lazy val sql = "exec " + finalTableSchema + ".[DropAndCreateFKContraints] @GoldTableName = '" + GoldDimTableName + "' , @ReseedTo = " + 1
# MAGIC lazy val sql = "truncate table " + GoldDimTableNameComplete;
# MAGIC stmt.execute(sql)
# MAGIC lazy val sqlReseed = "DBCC CHECKIDENT (" + GoldDimTableNameComplete + ", RESEED, 1 )"
# MAGIC //stmt.execute(sqlReseed)
# MAGIC connection.close()

# COMMAND ----------

GoldDimTableNameComplete = finalTableSchema + "." + GoldDimTableName
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