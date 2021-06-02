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
# MAGIC lazy val GoldDimTableName = dbutils.widgets.get("TableName")
# MAGIC lazy val GoldFactTableName = "Gold.FCT_NX_INV_LINE_ITEM_TRANS"
# MAGIC print (GoldDimTableName)
# MAGIC print (GoldFactTableName)

# COMMAND ----------

# Set the path for Silver layer for Nexsu
/
now = datetime.now() 
#sourceSilverPath = "Policy/Nexsure/DimPolicy/2021/05"
sourceSilverPath = "Policy/Nexsure/DimPolicy/" +now.strftime("%Y") + "/" + now.strftime("%m")
sourceSilverPath = SilverContainerPath + sourceSilverPath

sourceSilverFile = "DimPolicy_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
#sourceSilverFile = "DimPolicy_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_21.parquet"
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
#badRecordsPath = "abfss://c360logs@dlsldpdev01v8nkg988.dfs.core.windows.net/Dim_NX_POL/"
#badRecordsFilePath = "abfss://c360logs@dlsldpdev01v8nkg988.dfs.core.windows.net/Dim_NX_POL/" + date_time
recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"

#BatchId = "1afc2b6c-d987-48cc-ae8c-a7f41ea27249"
#WorkFlowId ="8fc2895d-de32-4bf4-a531-82f0c6774221"
#Set the file path to log error
#badRecordsPath = badRecordsRootPath + "/" + sourceTable + "/"

print ("Param -\'Variables':")
print (sourceSilverFilePath)
print (badRecordsFilePath)
print (recordCountFilePath)

# COMMAND ----------

# Temporary cell - DELETE
now = datetime.now() 
GoldDimTableName = "Dim_NX_Pol"
GoldFactTableName = "FCT_NX_INV_LINE_ITEM_TRANS"
sourceSilverPath = "Policy/Nexsure/DimPolicy/" +now.strftime("%Y") + "/05"
sourceSilverPath = SilverContainerPath + sourceSilverPath
sourceSilverFile = "DimPolicy_2021_05_21.parquet"
sourceSilverFilePath = sourceSilverPath + "/" + sourceSilverFile
badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"
recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"
BatchId = "1afc2b6c-d987-48cc-ae8c-a7f41ea27249"
WorkFlowId ="8fc2895d-de32-4bf4-a531-82f0c6774221"

# COMMAND ----------

# MAGIC %scala
# MAGIC // Temporary cell - DELETE
# MAGIC lazy val GoldDimTableName = "Dim_NX_Pol"

# COMMAND ----------

 # Do not proceed if any of the parameters are missing
if (GoldDimTableName == "" or sourceSilverPath == "" or sourceSilverFile == ""):
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Input parameters are missing"}}})

# COMMAND ----------

sourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Policy/Nexsure/DimPolicy/2021/05/DimPolicy_2021_05_21.parquet" 
# Read source file
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
try:
 
   sourceSilverDF = spark.read.option("badRecordsPath", badRecordsPath).schema("PolicyKey INT,PolicyID INT,ClientKey INT,EffectiveDate timestamp,ExpirationDate	timestamp,PolicyNumber string,PolicyType string,PolicyHistoryFlag boolean,PolicyStage string,PolicyMode string,PolicyDescription string,PolicyTermID INT,PolicyGroupID int,IssuingCarrierNameId int,BillingCarrierNameId int,DBSourceKey int,AuditKey int").parquet(sourceSilverFilePath)
   #display(sourceSilverDF)
except:
  print("Schema mismatch")
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Schema mismatch: " + sourceSilverFilePath}}})

# COMMAND ----------

# Register table so it is accessible via SQL Context
sourceSilverDF.createOrReplaceTempView("DIM_NX_POL")
#display(sourceSilverDF)

# COMMAND ----------

dummyDataDF = spark.sql(
  f""" 
SELECT
-99999 As POLICY_KEY,
-1 As POLICY_ID,
-1 As CLIENT_KEY,
-1 As POLICY_EFF_DT,
-1 As POLICY_EXP_DT,
-1 As POLICY_NO,
-1 As POLICY_TYP,
-1 As POLICY_HIST_FLG,
-1 As POLICY_STG,
-1 As POLICY_MODE,
-1 As POLICY_DESC,
-1 As POLICY_TERM_ID,
-1 As POLICY_GRP_ID,
-1 As ISSUING_CARRIER_KEY,
-1 As BILLING_CARRIER_KEY,
-1 As DB_SRC_KEY,
-1 As SRC_AUDT_KEY,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId }' AS ETL_WRKFLW_ID,
current_timestamp() AS ETL_CREATED_DT,
current_timestamp() AS ETL_UPDATED_DT
FROM DIM_NX_POL LIMIT 1
"""
)

# COMMAND ----------

# Get final set of records
finalDataDF = spark.sql(
f""" 
SELECT
PolicyKey As POLICY_KEY,
PolicyID As POLICY_ID,
ClientKey As CLIENT_KEY,
EffectiveDate As POLICY_EFF_DT,
ExpirationDate As POLICY_EXP_DT,
PolicyNumber As POLICY_NO,
PolicyType As POLICY_TYP,
PolicyHistoryFlag As POLICY_HIST_FLG,
PolicyStage As POLICY_STG,
PolicyMode As POLICY_MODE,
PolicyDescription As POLICY_DESC,
PolicyTermID As POLICY_TERM_ID,
PolicyGroupID As POLICY_GRP_ID,
IssuingCarrierNameId As ISSUING_CARRIER_KEY,
BillingCarrierNameId As BILLING_CARRIER_KEY,
DBSourceKey As DB_SRC_KEY,
AuditKey As SRC_AUDT_KEY,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId }' AS ETL_WRKFLW_ID,
current_timestamp() AS ETL_CREATED_DT,
current_timestamp() AS ETL_UPDATED_DT
FROM DIM_NX_POL
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
reconTable = "qc.Recon"
reconDF.write.jdbc(url=Url, table=reconTable, mode="append")

# COMMAND ----------

# MAGIC %scala
# MAGIC // Truncate Fact table and Delete data from Dimension table
# MAGIC lazy val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
# MAGIC lazy val stmt = connection.createStatement()
# MAGIC lazy val sql_truncate = "truncate table " + GoldFactTableName
# MAGIC stmt.execute(sql_truncate)
# MAGIC lazy val sql = "exec [Admin].[DropAndCreateFKContraints] @GoldTableName = '" + GoldDimTableName + "'"
# MAGIC stmt.execute(sql)
# MAGIC connection.close()

# COMMAND ----------

GoldDimTableNameComplete = "gold." + GoldDimTableName
dummyDataDF.write.jdbc(url=Url, table=GoldDimTableNameComplete, mode="append")
finalDataDF.write.jdbc(url=Url, table=GoldDimTableNameComplete, mode="append")