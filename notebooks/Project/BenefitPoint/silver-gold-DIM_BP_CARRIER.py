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
# MAGIC   GoldDimTableName = "DIM_BP_CARRIER"
# MAGIC }

# COMMAND ----------

# Set the path for Silver layer for Nexsure

now = datetime.now() 

sourceSilverFilePath = SilverContainerPath + "Carrier/Benefits/vw_CARRIER_AllRecs/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "vw_CARRIER_AllRecs_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"

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
  GoldDimTableName = "DIM_BP_CARRIER"
  badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"
  recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"
  BatchId = "1afc2b6c-d987-48cc-ae8c-a7f41ea27249"
  WorkFlowId ="8fc2895d-de32-4bf4-a531-82f0c6774221"
  sourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Carrier/Benefits/vw_CARRIER_AllRecs/" + yymmManual + "/vw_CARRIER_AllRecs_" + yyyymmddManual + ".parquet"

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
sourceSilverDF.createOrReplaceTempView("DIM_BP_CARRIER")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[BP_NX_REF_CARRIER_MAPPING]) client"
carriermapDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(clientDF)
# Register table so it is accessible via SQL Context
carriermapDF.createOrReplaceTempView("BP_NX_REF_CARRIER_MAPPING")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[BP_NX_REF_AM_BEST_MONTHLY_IMPORT_MAPPING]) client"
ambDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(clientDF)
# Register table so it is accessible via SQL Context
ambDF.createOrReplaceTempView("BP_NX_REF_AM_BEST_MONTHLY_IMPORT_MAPPING")

# COMMAND ----------

dummyDataDF = spark.sql(
f"""
 SELECT
'0' As BILING_CARIER_ONLY_IND,
'0' As CARIER_ACTV_IND,
-99999 As CARIER_ID,
-99999 As CARIER_IDNTITY,
'0' As INSURER_IND,
-99999 As PARNT_CARIER_ID,
'0' As PARNT_IND,
'None'	WEBSITE,
'0'	RAP_INDICATOR,
'None' NAIC_CMPANY_NUMBER,
'None' AMB_NUMBER,
'None' AMB_CMPANY_NAME,
'None' AMB_PARENT_NUMBER,
'None' AMB_PARENT_NAME,
'None' AMB_ULTMATE_PRENT_NUMBER,
'None' AMB_ULTMATE_PRENT_NAME,
'None' COUNTRY_OF_DOMICILE,
'None' BEST_FIN_STRGTH_RATG_CUR,
'None' VALID_PREMIMUM,
'None' DESCRIPTION,
'None' BCO_CATEGORY,	
'None' PREFERRED_WHOLESALER,
'None' PREFERRED_GROUP,
'None' PS_BCO_CODE,
'None' PS_BCO_DESCRIPTION,
'None' PS_CARRIER_STATUS,
'1800-12-31' START_DT,	
'1800-12-31' END_DT,
CURRENT_TIMESTAMP() SRC_ROW_BEGIN_DT,
CURRENT_TIMESTAMP() SRC_ROW_END_DT,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId}' AS ETL_WRKFLW_ID,
CURRENT_TIMESTAMP() as ETL_CREATED_DT,
CURRENT_TIMESTAMP() as ETL_UPDATED_DT
FROM DIM_BP_CARRIER LIMIT 1
""")

# COMMAND ----------

finalDataDF = spark.sql(
f"""
SELECT 
BILLING_CARRIER_ONLY_IND As BILING_CARIER_ONLY_IND,
CARRIER_ACTIVE_IND As CARIER_ACTV_IND,
CARRIER_ID As CARIER_ID,
CARRIER_IDENTITY As CARIER_IDNTITY,
INSURER_IND As INSURER_IND,
NAME As NAME,
PARENT_CARRIER_ID As PARNT_CARIER_ID,
PARENT_IND As PARNT_IND,
WEBSITE As WEBSITE,
RAP_IND As RAP_INDICATOR,
NAIC_CMPNY_NUM As NAIC_CMPANY_NUMBER,
AMB_NUM As AMB_NUMBER,
AMB_CMPNY_NAME As AMB_CMPANY_NAME,
PARNT_NUM As AMB_PARENT_NUMBER,
PARNT_NAME As AMB_PARENT_NAME,
AMB_ULTMTE_PARNT_NUM As AMB_ULTMATE_PRENT_NUMBER,
AMB_ULTMTE_PARNT_NAME As AMB_ULTMATE_PRENT_NAME,
CNTRY_OF_DOMCLE As COUNTRY_OF_DOMICILE,
-1 As BEST_FIN_STRGTH_RATG_CUR,
VLD_PREM As VALID_PREMIMUM,
-1 As DESCRIPTION,
BCO_CATG As BCO_CATEGORY,
PREF_WHOL_SALER As PREFERRED_WHOLESALER,
PREF_GRP As PREFERRED_GROUP,
PS_BCO_CD As PS_BCO_CODE,
PS_BCO_DESC As PS_BCO_DESCRIPTION,
PS_CARIER_STATUS As PS_CARRIER_STATUS,
'1800-12-31' As START_DT,
'1800-12-31' As END_DT,
RowBeginDate As SRC_ROW_BEGIN_DT,
RowBeginDate As	SRC_ROW_END_DT,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId}' AS ETL_WRKFLW_ID,
CURRENT_TIMESTAMP() as ETL_CREATED_DT,
CURRENT_TIMESTAMP() as ETL_UPDATED_DT
FROM DIM_BP_CARRIER e
JOIN BP_NX_REF_CARRIER_MAPPING mc on e.CARRIER_ID = mc.CARIER_SRC_ID and mc.SRC = 'BP' and mc.END_DT is null
LEFT JOIN BP_NX_REF_AM_BEST_MONTHLY_IMPORT_MAPPING amb on amb.AMB_NUM = mc.AM_BST_NUM
""")
display(finalDataDF)

# COMMAND ----------

finalDataDF.count()

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

