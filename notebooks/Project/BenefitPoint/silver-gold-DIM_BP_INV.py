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
# MAGIC   GoldDimTableName = "Dim_BP_Inv"
# MAGIC }

# COMMAND ----------

# Set the path for Silver layer for Nexsure

now = datetime.now() 

sourceSilverFilePath = SilverContainerPath + "Revenue/Benefits/vw_STATEMENT_AllRecs/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "vw_STATEMENT_AllRecs_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"

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
  GoldDimTableName = "Dim_BP_Inv"
  GoldFactTableName = "FCT_BP_INV_LINE_ITEM_TRANS"
  badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"
  recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"
  BatchId = "1afc2b6c-d987-48cc-ae8c-a7f41ea27249"
  WorkFlowId ="8fc2895d-de32-4bf4-a531-82f0c6774221"
  sourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Revenue/Benefits/vw_STATEMENT_AllRecs/" + yymmManual + "/vw_STATEMENT_AllRecs_" + yyyymmddManual + ".parquet"
  print(sourceSilverFilePath)

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
sourceSilverDF.createOrReplaceTempView("DIM_BP_INV")


# COMMAND ----------

dummyDataDF = spark.sql(
  f"""
SELECT -99999 as BP_STMNT_ID,
       -99999 as BRNCH_ID,
       -99999 as CARIER_ID,
       -99999 as CRETD_BY_BROKR_ID,
       CURRENT_TIMESTAMP() as BP_LAST_POSTD_DT,
       -99999 as REVISD_INV_ID,
       -99999 as INV_ID,
       -99999 as INV_STATUS_DESC,
       -99999 as INV_STATUS_ID,
       CURRENT_TIMESTAMP() as ACCT_MONTH_DT,
       CURRENT_TIMESTAMP() as CHECK_DT,
       CURRENT_TIMESTAMP() as CHECK_DEPOSIT_DT,
       CURRENT_TIMESTAMP() as ENTRY_DT,
       CURRENT_TIMESTAMP() as INV_DT,
       CURRENT_TIMESTAMP() as VOIDED_DT ,
       CURRENT_TIMESTAMP() as BP_LAST_MODIFIED_DT,
       -99999 as CHECK_ISSUED_BY,
       -99999 as CHECK_NUMBER,
       -99999 as CHECK_PAYABLE_TO,
       CURRENT_TIMESTAMP() as ENDING_DT_RANGE,
       CURRENT_TIMESTAMP() as STARTING_DT_RANGE,
       -99999 as NOTES,
       -99999 as OVERRIDE_IND,
       -99999 as OVERRIDE_PAYE_ID,
       -99999 as PAYMNT_METHOD_DESC,
       -99999 as PRI_INV_COLUMN_DESC,
       -99999 as SEC_INV_COLUMN_DESC,
       -99999 as USE_EST_PREMIUM_IND,
       CURRENT_TIMESTAMP() as SRC_ROW_BEGIN_DT,
       CURRENT_TIMESTAMP() as SRC_ROW_END_DT,
     '{ BatchId }' AS ETL_BATCH_ID,
      '{ WorkFlowId}' AS ETL_WRKFLW_ID,
      CURRENT_TIMESTAMP() as ETL_CREATED_DT,
      CURRENT_TIMESTAMP() as ETL_UPDATED_DT
      FROM DIM_BP_INV LIMIT 1
""")

# COMMAND ----------

finalDataDF = spark.sql(
f"""
SELECT distinct STARTING_DATE_RANGE 
      FROM DIM_BP_INV 
      order by STARTING_DATE_RANGE 
""")
display(finalDataDF)

# COMMAND ----------

finalDataDF = spark.sql(
f"""
SELECT 
       STATEMENT_ID as BP_STMNT_ID,
       BROKERAGE_OFFICE_ID as BRNCH_ID,
       CARRIER_ID as CARIER_ID,
       CREATED_BY_BROKER_ID as CRETD_BY_BROKR_ID, 
       LAST_POSTED_DATE as BP_LAST_POSTD_DT,
       REVISED_STATEMENT_ID as REVISD_INV_ID,
       STATEMENT_ID as INV_ID,
       STATEMENT_STATUS_DESC as INV_STATUS_DESC,
       STATEMENT_STATUS_ID as INV_STATUS_ID,
       ACCOUNTING_MONTH_DATE as ACCT_MONTH_DT,
       CHECK_DATE as CHECK_DT,
       CHECK_DEPOSIT_DATE as CHECK_DEPOSIT_DT,
       VOIDED_DATE as ENTRY_DT,
       STATEMENT_DATE as INV_DT,
       VOIDED_DATE as VOIDED_DT ,
       LAST_MODIFIED_DATE as BP_LAST_MODIFIED_DT,
       CHECK_ISSUED_BY as CHECK_ISSUED_BY,
       CHECK_NUMBER as CHECK_NUMBER,
       CHECK_PAYABLE_TO as CHECK_PAYABLE_TO,
       ENDING_DATE_RANGE as ENDING_DT_RANGE,
       STARTING_DATE_RANGE as STARTING_DT_RANGE,
       NOTES as NOTES,
       OVERRIDE_IND as OVERRIDE_IND,
       OVERRIDE_PAYEE_ID as OVERRIDE_PAYE_ID,
       PAYMENT_METHOD_TYPE_DESC as PAYMNT_METHOD_DESC,
       PRI_STATEMENT_COLUMN_DESC as PRI_INV_COLUMN_DESC,
       SEC_STATEMENT_COLUMN_DESC as SEC_INV_COLUMN_DESC,
       USE_EST_PREMIUM_IND as USE_EST_PREMIUM_IND,
       RowBeginDate as SRC_ROW_BEGIN_DT,
       RowEndDate as SRC_ROW_END_DT,
     '{ BatchId }' AS ETL_BATCH_ID,
      '{ WorkFlowId}' AS ETL_WRKFLW_ID,
      CURRENT_TIMESTAMP() as ETL_CREATED_DT,
      CURRENT_TIMESTAMP() as ETL_UPDATED_DT
      FROM DIM_BP_INV inv 
""")
display(finalDataDF)

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

# COMMAND ----------

