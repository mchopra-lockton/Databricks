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
# MAGIC 
# MAGIC dbutils.widgets.text("TableName", "","")
# MAGIC var GoldDimTableName = dbutils.widgets.get("TableName")
# MAGIC 
# MAGIC // USE WHEN RUN IN DEBUG MODE
# MAGIC if (RunInDebugMode != "No") {
# MAGIC   GoldDimTableName = "Dim_NX_POL_INFO"
# MAGIC }

# COMMAND ----------

# Set the path for Silver layer for Nexsure

now = datetime.now() 
#sourceSilverPath = "Invoice/Nexsure/DimRateType/2021/06"
sourceSilverFolderPath = "Policy/Nexsure/FactPolicyInfo/" +now.strftime("%Y") + "/" + now.strftime("%m")
sourceSilverPath = SilverContainerPath + sourceSilverFolderPath

sourceSilverFile = "FactPolicyInfo_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
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
recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"

#Set the file path to log error

print ("Param -\'Variables':")
print (sourceSilverFilePath)
print (badRecordsFilePath)
print (recordCountFilePath)

# COMMAND ----------

# USE WHEN RUN IN DEBUG MODE
if (RunInDebugMode != 'No'):
  now = datetime.now() 
  GoldDimTableName = "DIM_NX_POL_INFO"
  GoldFactTableName = "FCT_NX_INV_LINE_ITEM_TRANS"
  badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"
  recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"
  BatchId = "1afc2b6c-d987-48cc-ae8c-a7f41ea27249"
  WorkFlowId ="8fc2895d-de32-4bf4-a531-82f0c6774221"
  sourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Policy/Nexsure/FactPolicyInfo/" + yymmManual + "/FactPolicyInfo_" + yyyymmddManual + ".parquet"

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
  #dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceSilverFilePath}}})  

# COMMAND ----------

sourceSilverDF.createOrReplaceTempView("DIM_NX_POL_INFO")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_NX_CLIENT]) client"
clientDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(carrierDF)
# Register table so it is accessible via SQL Context
clientDF.createOrReplaceTempView("DIM_NX_CLIENT")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_NX_ORG]) org"
orgDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(carrierDF)
# Register table so it is accessible via SQL Context
orgDF.createOrReplaceTempView("DIM_NX_ORG")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_NX_POL_LOB]) org"
polLOBDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(carrierDF)
# Register table so it is accessible via SQL Context
polLOBDF.createOrReplaceTempView("DIM_NX_POL_LOB")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_NX_POL]) org"
polDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(carrierDF)
# Register table so it is accessible via SQL Context
polDF.createOrReplaceTempView("DIM_NX_POL")

# COMMAND ----------

 dummyDataDF = spark.sql(
f""" 
SELECT
-99999 as POL_INFO_KEY,
0 as POL_ID,
0 as CLIENT_ID,
0 as ORG_ID,
0 as POL_LOB_ID,
-1 as DB_SRC_KEY,
-1 as SRC_AUDT_INS_KEY,
-1 as SRC_AUDT_UPD_KEY
,'{ BatchId }' AS ETL_BATCH_ID
,'{ WorkFlowId }' AS ETL_WORKFLOW_ID
,current_timestamp() AS ETL_CREATED_DT
,current_timestamp() AS ETL_UPDATED_DT
FROM DIM_NX_POL_INFO e LIMIT 1
"""
)
display(dummyDataDF)

# COMMAND ----------

# Get final set of records
finalDataDF = spark.sql(
f""" 
SELECT
PolicyInfoKey as POL_INFO_KEY,
p.SURR_POL_ID as POL_ID,
c.SURR_CLIENT_ID as CLIENT_ID,
o.SURR_ORG_ID as ORG_ID,
pl.SURR_POL_LOB_ID as POL_LOB_ID,
DBSourceKey as DB_SRC_KEY,
InsertAuditKey as SRC_AUDT_INS_KEY,
UpdateAuditKey as SRC_AUDT_UPD_KEY,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId }' AS ETL_WORKFLOW_ID,
current_timestamp() AS ETL_CREATED_DT,
current_timestamp() AS ETL_UPDATED_DT
FROM DIM_NX_POL_INFO pi
JOIN DIM_NX_POL p on pi.PolicyKey = p.NX_POLICY_KEY
JOIN DIM_NX_POL_LOB pl on Pi.PolicyLOBKey = pl.NX_POL_LOB_KEY
JOIN DIM_NX_ORG o on pi.OrgStructureKey = o.NX_ORG_KEY
JOIN DIM_NX_CLIENT c on pi.ClientKey = c.NX_CLIENT_KEY
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