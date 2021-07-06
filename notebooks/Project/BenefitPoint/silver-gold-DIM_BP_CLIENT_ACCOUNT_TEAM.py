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
# MAGIC   GoldDimTableName = "DIM_BP_CLIENT_ACCOUNT_TEAM"
# MAGIC }

# COMMAND ----------

# Set the path for Silver layer for Nexsure

now= datetime.now()   

sourceSilverFilePath = SilverContainerPath + "Client/Benefits/vw_CLIENT_ACCOUNT_TEAM_AllRecs/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "vw_CLIENT_ACCOUNT_TEAM_AllRecs_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"

dbutils.widgets.text("TableName", "","")
GoldDimTableName = dbutils.widgets.get("TableName")

dbutils.widgets.text("BatchId", "","")
BatchId = dbutils.widgets.get("BatchId")

dbutils.widgets.text("WorkFlowId", "","")
WorkFlowId = dbutils.widgets.get("WorkFlowId")

#Set the file path to log error
badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"

date_time = now.strftime("%Y%m%dT%H%M%S")
badRecordsFilePath = badRecordsPath + date_time + "/" + "ErrorRecords"
recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"

print ("Param -\'Variables':")
print (sourceSilverFilePath)
print (badRecordsFilePath)
print (recordCountFilePath)

# COMMAND ----------

# USE WHEN RUN IN DEBUG MODE
if (RunInDebugMode != 'No'):
  now = datetime.now() 
  GoldDimTableName = "DIM_BP_CLIENT_ACCOUNT_TEAM"
  badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"
  recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"
  BatchId = "1afc2b6c-d987-48cc-ae8c-a7f41ea27249"
  WorkFlowId ="8fc2895d-de32-4bf4-a531-82f0c6774221"
  sourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Client/Benefits/vw_CLIENT_ACCOUNT_TEAM_AllRecs/" + yymmManual + "/vw_CLIENT_ACCOUNT_TEAM_AllRecs_" + yyyymmddManual + ".parquet"

# COMMAND ----------

# Read source file
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")

try:
 
  sourceSilverDF = spark.read.parquet(sourceSilverFilePath)
  display(sourceSilverDF)
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
sourceSilverDF.createOrReplaceTempView("DIM_BP_CLIENT_ACCOUNT_TEAM")


# COMMAND ----------

pushdown_query = "(select BP_CLNT_ID,SURR_CLNT_ID from [dbo].[DIM_BP_CLIENT]) PC"
carrierDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# Register table so it is accessible via SQL Context
carrierDF.createOrReplaceTempView("DIM_BP_CLIENT")

# COMMAND ----------

pushdown_query = "(select BP_BROKR_ID,SURR_BROKR_ID from [dbo].[DIM_BP_BROKER]) PC"
carrierDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# Register table so it is accessible via SQL Context
carrierDF.createOrReplaceTempView("DIM_BP_BROKER")

# COMMAND ----------

dummyDataDF = spark.sql(
  f"""
SELECT
-99999 BP_CLNT_ACOUNT_TEAM_ID,
-99999 BRKR_ID,
0 CLNT_ID,
'0' ACOUNT_TEAM_OWNR_IND,
'0' RENWL_OWNR_IND,
CURRENT_TIMESTAMP() LAST_MOD_DT,
CURRENT_TIMESTAMP() SRC_ROW_BEGN_DT,
CURRENT_TIMESTAMP() SRC_ROW_END_DT,
0 As SURR_CLNT_ID,  
0 As SURR_BROKR_ID,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId}' AS ETL_WRKFLW_ID,
CURRENT_TIMESTAMP() as ETL_CREATED_DT,
CURRENT_TIMESTAMP() as ETL_UPDATED_DT
FROM DIM_BP_CLIENT_ACCOUNT_TEAM LIMIT 1
""")

# COMMAND ----------

finalDataDF = spark.sql(
f"""
SELECT 
0 As BP_CLNT_ACOUNT_TEAM_ID,
BROKER_ID As BRKR_ID,
CLIENT_ID As CLNT_ID,
ACCOUNT_TEAM_OWNER_IND As ACOUNT_TEAM_OWNR_IND,
RENEWAL_OWNER_IND As RENWL_OWNR_IND,
LAST_MODIFIED_DATE As LAST_MOD_DT,
RowBeginDate As SRC_ROW_BEGN_DT,
RowEndDate As SRC_ROW_END_DT,
coalesce(client.SURR_CLNT_ID,0) As SURR_CLNT_ID,  -- TO BE ADDED LATER FK
coalesce(broker.SURR_BROKR_ID,0) As SURR_BROKR_ID,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId}' AS ETL_WRKFLW_ID,
CURRENT_TIMESTAMP() as ETL_CREATED_DT,
CURRENT_TIMESTAMP() as ETL_UPDATED_DT
FROM DIM_BP_CLIENT_ACCOUNT_TEAM cat
LEFT JOIN DIM_BP_CLIENT client on cat.CLIENT_ID = client.BP_CLNT_ID
LEFT JOIN DIM_BP_BROKER broker on cat.BROKER_ID = broker.BP_BROKR_ID
""")
display(finalDataDF)

# COMMAND ----------

# Do not proceed if there are no records to insert
if (finalDataDF.count() == 0):
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "There are no records to insert: " + sourceSilverFilePath}}})

# COMMAND ----------

finalDataDF.count()

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

# COMMAND ----------

