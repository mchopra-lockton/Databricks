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

# COMMAND ----------

# Set the path for Silver layer for Nexsu
/
now = datetime.now() 
#sourceSilverPath = "Policy/Nexsure/DimPolicy/2021/05"
sourceSilverFolderPath = "Policy/Nexsure/DimPolicy/" +now.strftime("%Y") + "/" + now.strftime("%m")
sourceSilverPath = SilverContainerPath + sourceSilverFolderPath

sourceSilverFile = "FactPolicyInfo_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
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

# Temporary cell to run manually - DELETE
if (GoldDimTableName == "" or sourceSilverPath == "" or sourceSilverFile == ""):
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
  sourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Policy/Nexsure/DimPolicy/2021/06/DimPolicy_2021_06_04.parquet"

# COMMAND ----------

# MAGIC %scala
# MAGIC // Temporary cell to run manually - DELETE
# MAGIC if (GoldDimTableName == "") {
# MAGIC  lazy val GoldDimTableName = "Dim_NX_Pol"
# MAGIC }

# COMMAND ----------

 # Do not proceed if any of the parameters are missing
if (GoldDimTableName == "" or sourceSilverPath == "" or sourceSilverFile == ""):
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Input parameters are missing"}}})

# COMMAND ----------

sourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Policy/Nexsure/DimPolicy/2021/06/DimPolicy_2021_06_04.parquet" 
# Read source file
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
try:
 
   sourceSilverDF = spark.read.parquet(sourceSilverFilePath)
except:
  # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceSilverFilePath}}})  

# COMMAND ----------

sourceSilverDF.createOrReplaceTempView("DIM_NX_POL")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_NX_CLIENT]) client"
clientDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(carrierDF)
# Register table so it is accessible via SQL Context
clientDF.createOrReplaceTempView("DIM_NX_CLIENT")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_NX_CARRIER]) carrier"
carrierDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(carrierDF)
# Register table so it is accessible via SQL Context
carrierDF.createOrReplaceTempView("DIM_NX_CARRIER")

# COMMAND ----------

dummyDataDF = spark.sql(
  f""" 
SELECT
-99999 As NX_POLICY_KEY,
-1 As NX_POLICY_ID,
-1 As CLIENT_KEY,
0 As CLIENT_ID,
-1 As POLICY_EFF_DT,
-1 As POLICY_EXP_DT,
-1 As POLICY_NO,
-1 As POLICY_TYP,
-1 As POLICY_HIST_FLG,
-1 As POLICY_STG,
-1 As POLICY_MODE,
-1 As POLICY_DESC,
-1 as ISSUING_CARRIER_KEY,
-1 As BILLING_CARRIER_KEY,
0 As ISSUING_CARRIER_ID,
0 As BILLING_CARRIER_ID,
-1 As COVG_EFF_DATE,
-1 As COVG_EXP_DATE,
-1 As POL_ORGN_DT,
-1 As PRIM_STATE,
-1 As POL_BILL_MTHD,
-1 As POL_STATUS,
-1 As NON_RENWL_STATUS,
-1 As ADMTED_CARIER_FLG,
-1 As DB_SRC_KEY,
-1 As SRC_AUDT_KEY,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId }' AS ETL_WRKFLW_ID,
current_timestamp() AS ETL_CREATED_DT,
current_timestamp() AS ETL_UPDATED_DT
FROM DIM_NX_POL LIMIT 1
"""
)
display(dummyDataDF)

# COMMAND ----------

# Get final set of records
finalDataDF = spark.sql(
f""" 
SELECT
PolicyKey As NX_POLICY_KEY,
PolicyID As NX_POLICY_ID,
ClientKey As CLIENT_KEY,
SURR_CLIENT_ID As CLIENT_ID,
EffectiveDate As POLICY_EFF_DT,
ExpirationDate As POLICY_EXP_DT,
PolicyNumber As POLICY_NO,
PolicyType As POLICY_TYP,
PolicyHistoryFlag As POLICY_HIST_FLG,
PolicyStage As POLICY_STG,
PolicyMode As POLICY_MODE,
PolicyDescription As POLICY_DESC,
IssuingCarrierNameId As ISSUING_CARRIER_KEY,
BillingCarrierNameId As BILLING_CARRIER_KEY,
icarr.SURR_CARIER_ID As ISSUING_CARRIER_ID,
bcarr.SURR_CARIER_ID As BILLING_CARRIER_ID,
CoverageEffectiveDate AS COVG_EFF_DATE,
CoverageExpirationDate AS COVG_EXP_DATE,
PolicyOriginationDate AS POL_ORGN_DT,
PrimaryState AS PRIM_STATE,
PolicyBillMethod AS POL_BILL_MTHD,
PolicyStatus AS POL_STATUS,
NonRenewalStatus AS NON_RENWL_STATUS,
IsAdmittedCarrier AS ADMTED_CARIER_FLG,
DBSourceKey As DB_SRC_KEY,
AuditKey As SRC_AUDT_KEY,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId }' AS ETL_WRKFLW_ID,
current_timestamp() AS ETL_CREATED_DT,
current_timestamp() AS ETL_UPDATED_DT
FROM DIM_NX_POL pi
JOIN DIM_NX_CLIENT c on pi.ClientKey = c.NX_CLIENT_KEY
JOIN DIM_NX_CARRIER icarr on pi.IssuingCarrierNameId = icarr.NX_CARIER_ID
JOIN DIM_NX_CARRIER bcarr on pi.BillingCarrierNameId = bcarr.NX_CARIER_ID
"""
)

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

