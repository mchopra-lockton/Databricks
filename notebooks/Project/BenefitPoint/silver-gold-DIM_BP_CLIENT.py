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
# MAGIC lazy val GoldDimTableName = dbutils.widgets.get("TableName")
# MAGIC 
# MAGIC lazy val GoldFactTableName = "Gold.FCT_BP_INV_LINE_ITEM_TRANS"
# MAGIC print (GoldDimTableName)
# MAGIC print (GoldFactTableName)

# COMMAND ----------

# Set the path for Silver layer for Nexsure

now = datetime.now()
sourceSilverFolderPath = "Client/Benefits/vw_CLIENT_AllClients/" +now.strftime("%Y") + "/" + now.strftime("%m")

sourceSilverFilePath = SilverContainerPath + "Client/Benefits/vw_CLIENT_AllClients/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "vw_CLIENT_AllClients_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"

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

# Temporary cell - DELETE
now = datetime.now()
GoldDimTableName = "DIM_BP_CLIENT"
GoldFactTableName = "FCT_BP_INV_LINE_ITEM_TRANS"
badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"
recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"
BatchId = "1afc2b6c-d987-48cc-ae8c-a7f41ea27249"
WorkFlowId ="8fc2895d-de32-4bf4-a531-82f0c6774221"
sourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Client/Benefits/vw_CLIENT_AllClients/" + yymmManual + "/vw_CLIENT_AllClients_" + yyyymmddManual + ".parquet"

# COMMAND ----------

# MAGIC %scala
# MAGIC // Temporary cell - DELETE
# MAGIC lazy val GoldDimTableName = "DIM_BP_CLIENT"

# COMMAND ----------

 # Do not proceed if any of the parameters are missing
if (GoldDimTableName == "" or sourceSilverFilePath == ""):
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Input parameters are missing"}}})

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
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceSilverFilePath}}}) 

# COMMAND ----------

# Register table so it is accessible via SQL Context
sourceSilverDF.createOrReplaceTempView("DIM_BP_CLIENT")

# COMMAND ----------

dummyDataDF = spark.sql(
  f"""
SELECT
-99999 ACCT_NUM,
'0' ACTIV_IND,
-99999 CLNT_CLASIFICTN_TYP_ID,
-99999 CLNT_FUNDG_TYP_ID,
-99999 CLNT_ID,
-99999 CLNT_IDNTITY,
'None' CLNT_STATUS_DESC,
-99999 CLNT_STATUS_ID,
-99999 LAST_REVIEWD_BRKR_ID,
'None' NAME,
-99999 NMBR_OF_FULL_TIME_EMP,
-99999 OWNR_DEPT_ID,
-99999 OWNR_OFC_ID,
'None' SSN,
'None' TAX_PAYR_ID,
-99999 ORG_ID,
-99999 BUSINESS_TYPE_ID,
'None' BUSINESS_TYPE_DESC,
'None' WEBSITE,
'None' SIC_CODE,
'None' NAICS_CODE,
-99999 PRIMARY_INDSTRY_ID,
'None' PRIMARY_INDSTRY_DESC,
CURRENT_TIMESTAMP() SRC_ROW_BEGIN_DT,
CURRENT_TIMESTAMP() SRC_ROW_END_DT,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId}' AS ETL_WRKFLW_ID,
CURRENT_TIMESTAMP() as ETL_CREATED_DT,
CURRENT_TIMESTAMP() as ETL_UPDATED_DT
FROM DIM_BP_CLIENT LIMIT 1
""")

# COMMAND ----------

finalDataDF = spark.sql(
f"""
SELECT 
ACCOUNT_NUM As ACCT_NUM,
ACTIVE_IND As ACTIV_IND,
CLIENT_CLASSIFICATION_TYPE_ID As CLNT_CLASIFICTN_TYP_ID,
CLIENT_FUNDING_TYPE_ID As CLNT_FUNDG_TYP_ID,
CLIENT_ID As CLNT_ID,
CLIENT_IDENTITY As CLNT_IDNTITY,
CLIENT_STATUS_DESC As CLNT_STATUS_DESC,
CLIENT_STATUS_ID As CLNT_STATUS_ID,
LAST_REVIEWED_BROKER_ID As LAST_REVIEWD_BRKR_ID,
NAME As NAME,
NUMBER_OF_FULL_TIME_EMPLOYEES As NMBR_OF_FULL_TIME_EMP,
OWNER_DEPARTMENT_ID As OWNR_DEPT_ID,
OWNER_OFFICE_ID As OWNR_OFC_ID,
SSN As SSN,
TAX_PAYER_ID As TAX_PAYR_ID,
-1 As ORG_ID,
BUSINESS_TYPE_ID As BUSINESS_TYPE_ID,
BUSINESS_TYPE_DESC As BUSINESS_TYPE_DESC,
WEBSITE	As WEBSITE,
SIC_CODE As	SIC_CODE,
NAICS_CODE As NAICS_CODE,
PRIMARY_INDUSTRY_ID	As PRIMARY_INDSTRY_ID,
PRIMARY_INDUSTRY_DESC As PRIMARY_INDSTRY_DESC,
RowBeginDate As SRC_ROW_BEGIN_DT,
RowBeginDate As	SRC_ROW_END_DT,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId}' AS ETL_WRKFLW_ID,
CURRENT_TIMESTAMP() as ETL_CREATED_DT,
CURRENT_TIMESTAMP() as ETL_UPDATED_DT
FROM DIM_BP_CLIENT
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

# COMMAND ----------

