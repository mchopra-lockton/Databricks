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

sourceSilverFilePath = SilverContainerPath + "Policy/Benefits/vw_PLAN_AllPlans/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "vw_PLAN_AllPlans_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"

#adhoc product file
adhocSourceSilverFilePath = SilverContainerPath + "Policy/Benefits/vw_ADHOC_PRODUCT_AllPlans/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "vw_ADHOC_PRODUCT_AllPlans_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"

#plan type file
planTypeSourceSilverFilePath = SilverContainerPath + "Policy/Benefits/vw_PLAN_TYPE_AllTypes/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "vw_PLAN_TYPE_AllTypes_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"


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

print ("Param -\'Variables':")
print (sourceSilverFilePath)
print (badRecordsFilePath)
print (recordCountFilePath)

# COMMAND ----------

# Temporary cell - DELETE
now = datetime.now() 
GoldDimTableName = "DIM_BP_POL"
sourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Policy/Benefits/vw_PLAN_AllPlans/" + yymmManual + "/vw_PLAN_AllPlans_" + yyyymmddManual + ".parquet"
adhocSourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Policy/Benefits/vw_ADHOC_PRODUCT_AllPlans/" + yymmManual + "/vw_ADHOC_PRODUCT_AllPlans_" + yyyymmddManual + ".parquet"
planTypeSourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Policy/Benefits/vw_PLAN_TYPE_AllTypes/" + yymmManual + "/vw_PLAN_TYPE_AllTypes_" + yyyymmddManual + ".parquet"
print(sourceSilverFilePath)
print(adhocSourceSilverFilePath)
print(planTypeSourceSilverFilePath)

# COMMAND ----------

# MAGIC %scala
# MAGIC // Temporary cell - DELETE
# MAGIC  val GoldDimTableName = "DIM_BP_POL"

# COMMAND ----------

# Do not proceed if any of the parameters are missing
if (GoldDimTableName == "" or sourceSilverFilePath == "" or adhocSourceSilverFilePath == ""):
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Input parameters are missing"}}})

# COMMAND ----------

# Read  source file
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

# Read  adhoc source file
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
try:
 
  adhocSourceSilverDF = spark.read.parquet(adhocSourceSilverFilePath)
except:
  # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,adhocSourceSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + adhocSourceSilverFilePath}}}) 

# COMMAND ----------

# Read  plan type source file
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
try:
 
  planTypeSourceSilverDF = spark.read.parquet(planTypeSourceSilverFilePath)
except:
  # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,planTypeSourceSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + planTypeSourceSilverFilePath}}}) 

# COMMAND ----------

sourceSilverDF.createOrReplaceTempView("DIM_BP_PLAN")
adhocSourceSilverDF.createOrReplaceTempView("DIM_BP_ADHOC_PRODUCT")
planTypeSourceSilverDF.createOrReplaceTempView("DIM_BP_PLAN_TYPE")

# COMMAND ----------

# Get union set of records
unionDataDF = spark.sql(
f""" 
SELECT
BILLING_TYPE_DESC,
BILLING_TYPE_ID,
COMMISSIONS_PAID_BY_ID,
EFF_DATE,
NON_PAYABLE_IND,
NON_REVENUE_IND,
PARENT_CLIENT_PLAN_ID,
PLAN_NAME,
POLICY_NUM,
POLICY_ORIGIN_REASON_DESC,
RENEWAL_DATE,
PLAN_ID As POL_ID,
PLAN_FUNDING_TYPE_DESC,
PLAN_STAGE_FLG_DESC,
PLAN_TYPE_ID,
CLIENT_ID,
CREATE_DATE,
ORIG_EFF_DATE,
CANCELLATION_DATE,
REINSTATEMENT_DATE,
BROKER_OF_RECORD,
CANCELLATION_OTHER_REASON,
CANCELLATION_REASON_DESC,
REINSTATEMENT_OTHER_REASON,
REINSTATEMENT_REASON_DESC,
COMMISSION_START_DATE,
CONTINUOUS_IND,
NUMBER_OF_EE,
ORIG_REASON_QUALIFIER_DESC,
PREMIUM_PAY_FREQ_DESC,
UNION_IND,
VOLUNTARY_IND,
LAST_MODIFIED_DATE,
BILLING_CARRIER_ID,
CARRIER_ID,
'Plan' As BP_SOURCE_TABLE
FROM DIM_BP_PLAN 
UNION 
SELECT
BILLING_TYPE_DESC,
BILLING_TYPE_ID,
COMMISSIONS_PAID_BY,
EFF_DATE,
NON_PAYABLE_IND,
NON_REVENUE_IND,
PARENT_ADHOC_PRODUCT_ID,
NAME,
POLICY_NUMBER,
POLICY_ORIGIN_REASON_DESC,
RENEWAL_DATE,
ADHOC_PRODUCT_ID As POL_ID,
'' as PLAN_FUNDING_TYPE_DESC,
'' as PLAN_STAGE_FLG_DESC,
PLAN_TYPE_ID,
CLIENT_ID,
CREATE_DATE,
ORIG_EFF_DATE,
CANCELLATION_DATE,
REINSTATEMENT_DATE,
BROKER_OF_RECORD,
CANCELLATION_REASON_OTHER,
CANCELLATION_REASON_DESC,
REINSTATEMENT_OTHER_REASON,
REINSTATEMENT_REASON_DESC,
COMMISSION_START_DATE,
CONTINUOUS_IND,
NUMBER_OF_EE,
ORIG_REASON_QUALIFIER_DESC,
PREMIUM_PAY_FREQ_DESC,
UNION_IND,
VOLUNTARY_IND,
LAST_MODIFIED_DATE,
BILLING_CARRIER_ID,
CARRIER_ID,
'Adhoc Product' As BP_SOURCE_TABLE
FROM DIM_BP_ADHOC_PRODUCT
"""
)
unionDataDF.createOrReplaceTempView("DIM_BP_PLAN_ADHOC_PRODUCT")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_BP_CARRIER]) carrier"
carrierDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# Register table so it is accessible via SQL Context
carrierDF.createOrReplaceTempView("DIM_BP_CARRIER")

# COMMAND ----------

pushdown_query = "(select CLNT_ID,SURR_CLNT_ID from [dbo].[DIM_BP_CLIENT]) PC"
carrierDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# Register table so it is accessible via SQL Context
carrierDF.createOrReplaceTempView("DIM_BP_CLIENT")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_BP_LOB]) carrier"
carrierDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# Register table so it is accessible via SQL Context
carrierDF.createOrReplaceTempView("DIM_BP_LOB")

# COMMAND ----------

#add query for dummy data frame
dummyDataDF = spark.sql(
f""" 
SELECT
-99999 as BILING_TYP_ID,
-99999 as COMS_PAID_BY_ID,
'1800-01-01' as EFF_DT,
0 as NON_PYBLE_IND,
0 as NON_REVNUE_IND,
-99999 as PARNT_CLNT_POL_ID,
-99999 as POL_ID,
'None' as PLN_STAGE_FLG_DESC,
0 as ADHOC_PRDCT_IND,
'None' as LOC_DESC,
-99999 as LOC_ID,
-99999 as POL_LOB_ID,
-99999 as CLNT_ID,
'1800-01-01' as CREATED_DT,
'1800-01-01' as ORG_EFF_DT,
'1800-01-01' as CANCEL_DT,
'1800-01-01' as REINSTMENT_DT,
'1800-01-01' as BROKER_OF_REC,
'None' as CANCEL_OTHR_RESON,
'None' as CANCEL_RESON_DESC,
'None' as REINSTMNT_OTHR_RESON,
'None' as REINSTMNT_RESON_DESC,
'1800-01-01' as COMS_START_DT,
0 as NUMBER_OF_EE,
'None' as ORG_REASON_QUA_DESC,
'None' as PREMM_PAY_FEQ_DEC,
'1800-01-01' as BP_LAST_MODIFIED_DT,
'None' as BP_SOURCE_TABLE,
0 As BILNG_CARIER_ID,
0 As ISUNG_CARIER_ID,
0 As SURR_ISUNG_CARIER_ID,
0 As SURR_BILNG_CARIER_ID,
0 As SURR_CLNT_ID,
0 As SURR_LOB_ID,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId }' AS ETL_WRKFLW_ID,
current_timestamp() AS ETL_CREATED_DT,
current_timestamp() AS ETL_UPDATED_DT
FROM DIM_BP_PLAN_ADHOC_PRODUCT limit 1
"""
)
#display(dummyDataDF)

# COMMAND ----------

# Get final set of records
finalDataDF = spark.sql(
f""" 
SELECT
pa.BILLING_TYPE_DESC as BILING_TYP_DESC,
pa.BILLING_TYPE_ID as BILING_TYP_ID,
pa.COMMISSIONS_PAID_BY_ID as COMS_PAID_BY_ID,
pa.EFF_DATE as EFF_DT,
pa.NON_PAYABLE_IND as NON_PYBLE_IND,
pa.NON_REVENUE_IND as NON_REVNUE_IND,
pa.PARENT_CLIENT_PLAN_ID as PARNT_CLNT_POL_ID,
pa.PLAN_NAME as POL_NAME,
pa.POLICY_NUM as POL_NUM,
pa.POLICY_ORIGIN_REASON_DESC as POL_ORGN_RSN_DESC,
pa.RENEWAL_DATE as RENWL_DT,
pa.POL_ID as POL_ID,
pa.PLAN_FUNDING_TYPE_DESC as PLN_FNDG_TYP_DESC,
pa.PLAN_STAGE_FLG_DESC as PLN_STAGE_FLG_DESC,
lob.ADHOC_PRODUCT_IND as ADHOC_PRDCT_IND,
pt.LOC_DESC as LOC_DESC,
pt.LOC_ID as LOC_ID,
pt.PLAN_TYPE_DESC PLN_TYP_DESC,
pt.PLAN_TYPE_ID as POL_LOB_ID,
CLIENT_ID as CLNT_ID,
coalesce(CREATE_DATE,'1800-01-01') as CREATED_DT,
coalesce(ORIG_EFF_DATE,'1800-01-01') as ORG_EFF_DT,
coalesce(CANCELLATION_DATE,'1800-01-01') as CANCEL_DT,
coalesce(REINSTATEMENT_DATE,'1800-01-01') as REINSTMENT_DT,
BROKER_OF_RECORD as BROKER_OF_REC,
CANCELLATION_OTHER_REASON as CANCEL_OTHR_RESON,
CANCELLATION_REASON_DESC as CANCEL_RESON_DESC,
REINSTATEMENT_OTHER_REASON as REINSTMNT_OTHR_RESON,
REINSTATEMENT_REASON_DESC as REINSTMNT_RESON_DESC,
COMMISSION_START_DATE as COMS_START_DT,
CONTINUOUS_IND as CONTIS_IND,
NUMBER_OF_EE as NUMBER_OF_EE,
ORIG_REASON_QUALIFIER_DESC as ORG_REASON_QUA_DESC,
PREMIUM_PAY_FREQ_DESC as PREMM_PAY_FEQ_DEC,
UNION_IND as UNION_IND,
VOLUNTARY_IND as VOLUNTARY_IND,
LAST_MODIFIED_DATE as BP_LAST_MODIFIED_DT,
BP_SOURCE_TABLE,
BILLING_CARRIER_ID As BILNG_CARIER_ID,
CARRIER_ID As ISUNG_CARIER_ID,
coalesce(icarr.SURR_CARIER_ID,0) As SURR_ISUNG_CARIER_ID,
coalesce(bcarr.SURR_CARIER_ID,0) As SURR_BILNG_CARIER_ID,
coalesce(client.SURR_CLNT_ID,0) As SURR_CLNT_ID,
coalesce(lob.SURR_LOB_ID,0) As SURR_LOB_ID,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId }' AS ETL_WRKFLW_ID,
current_timestamp() AS ETL_CREATED_DT,
current_timestamp() AS ETL_UPDATED_DT
FROM DIM_BP_PLAN_ADHOC_PRODUCT pa
LEFT JOIN DIM_BP_CARRIER icarr on pa.CARRIER_ID = icarr.CARIER_ID
LEFT JOIN DIM_BP_CARRIER bcarr on pa.BILLING_CARRIER_ID = bcarr.CARIER_ID
LEFT JOIN DIM_BP_CLIENT client on pa.CLIENT_ID = client.CLNT_ID
LEFT JOIN DIM_BP_LOB lob on pa.PLAN_TYPE_ID = lob.BP_LOB_ID
LEFT JOIN DIM_BP_PLAN_TYPE pt on pa.PLAN_TYPE_ID = pt.PLAN_TYPE_ID
--JOIN DIM_BP_LOB p on pa.PLAN_ID = p.PLAN_ID
"""
)
#display(finalDataDF)

# COMMAND ----------

finalDataDF.count()

# COMMAND ----------

# Do not proceed if there are no records to insert
if (finalDataDF.count() == 0):
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "There are no records to insert: " + sourceSilverFilePath}}})

# COMMAND ----------

# Create a dataframe for record count
sourceRecordCount = unionDataDF.count()
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
# MAGIC //lazy val sql_truncate = "truncate table " + GoldFactTableName
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