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
# MAGIC var GoldDimTableName = dbutils.widgets.get("TableName")
# MAGIC 
# MAGIC // USE WHEN RUN IN DEBUG MODE
# MAGIC if (RunInDebugMode != "No") {
# MAGIC   GoldDimTableName = "DIM_BP_BROKER"
# MAGIC }

# COMMAND ----------

# Set the path for Silver layer for Nexsure
now= datetime.now()   

sourceSilverFilePath = SilverContainerPath + "Person/Benefits/vw_BROKER_AllRecs/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "vw_BROKER_AllRecs_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"

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
  GoldDimTableName = "DIM_BP_BROKER"
  badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"
  recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"
  BatchId = "1afc2b6c-d987-48cc-ae8c-a7f41ea27249"
  WorkFlowId ="8fc2895d-de32-4bf4-a531-82f0c6774221"
  sourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Person/Benefits/vw_BROKER_AllRecs/" + yymmManual + "/vw_BROKER_AllRecs_" + yyyymmddManual + ".parquet"
  print(BatchId)

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
sourceSilverDF.createOrReplaceTempView("DIM_BP_BROKER")


# COMMAND ----------

dummyDataDF = spark.sql(
  f"""
SELECT '0' ACEPT_LOGIN_AGRMNT_IND,
-99999 BP_BROKR_ID,
-99999 BROKR_IDNTITY,
-99999 BROKR_ROLE_TYP_ID,
-99999 BROKRGE_DEPT_ID,
-99999 BRNCH_ID,
'None' EMAIL_ADDR,
'None' FAX_AREA_CODE,
'None' FAX_PHONE_NUMBER,
CURRENT_TIMESTAMP()	ROW_BEGIN_DT,
CURRENT_TIMESTAMP() ROW_END_DT,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId}' AS ETL_WRKFLW_ID,
CURRENT_TIMESTAMP() as ETL_CREATED_DT,
CURRENT_TIMESTAMP() as ETL_UPDATED_DT
FROM DIM_BP_BROKER LIMIT 1
""")

# COMMAND ----------

finalDataDF = spark.sql(
f"""
SELECT ACCEPT_LOGIN_AGREEMENT_IND As ACEPT_LOGIN_AGRMNT_IND,
BROKER_ID As BP_BROKR_ID,
BROKER_IDENTITY As BROKR_IDNTITY,
BROKER_ROLE_TYPE_DESC As BROKR_ROLE_TYP_DESC,
coalesce(BROKER_ROLE_TYPE_ID,0) as BROKR_ROLE_TYP_ID,
BROKERAGE_DEPARTMENT_ID As BROKRGE_DEPT_ID,
FIRST_NAME As FIRST_NAME,
LAST_NAME As LAST_NAME,
OFFICE_ID As BRNCH_ID,
EMAIL_ADDR As EMAIL_ADDR,
FAX_AREA_CODE As FAX_AREA_CODE,
FAX_PHONE_NUMBER As	FAX_PHONE_NUMBER,
RowBeginDate As	ROW_BEGIN_DT,
RowEndDate As ROW_END_DT,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId}' AS ETL_WRKFLW_ID,
CURRENT_TIMESTAMP() as ETL_CREATED_DT,
CURRENT_TIMESTAMP() as ETL_UPDATED_DT
FROM DIM_BP_BROKER
""")
#display(finalDataDF)

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