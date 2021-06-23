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
# MAGIC lazy val GoldFactTableName = dbutils.widgets.get("TableName")

# COMMAND ----------

# Set the path for Silver layer for Nexsure

now = datetime.now() 

dbutils.widgets.text("TableName", "","")
GoldFactTableName = dbutils.widgets.get("TableName")

POSsourceSilverFilePath = SilverContainerPath + "Revenue/Benefits/vw_POSTING_RECORD_AllRecs/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "POSTING_RECORD_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
SPLsourceSilverFilePath = SilverContainerPath + "Revenue/Benefits/vw_POSTED_SPLIT_AllRecs/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "POSTED_SPLIT_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
SENTsourceSilverFilePath = SilverContainerPath + "Revenue/Benefits/ vw_STATEMENT_ENTRY_AllRecs/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "STATEMENT_ENTRY_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
STMTsourceSilverFilePath = SilverContainerPath + "Revenue/Benefits/ vw_STATEMENT_AllRecs/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "STATEMENT_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
planSourceSilverFilePath = SilverContainerPath + "Policy/Benefits/vw_PLAN_AllPlans/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "PLAN_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
adhocSourceSilverFilePath = SilverContainerPath + "Policy/Benefits/vw_ADHOC_PRODUCT_AllPlans/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "ADHOC_PRODUCT_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"

dbutils.widgets.text("BatchId", "","")
BatchId = dbutils.widgets.get("BatchId")

dbutils.widgets.text("WorkFlowId", "","")
WorkFlowId = dbutils.widgets.get("WorkFlowId")

#Set the file path to log error
badRecordsPath = badRecordsRootPath + GoldFactTableName + "/"


now = datetime.now() # current date and time
date_time = now.strftime("%Y%m%dT%H%M%S")
badRecordsFilePath = badRecordsPath + date_time + "/" + "ErrorRecords"
recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"

#Set the file path to log error
#badRecordsPath = badRecordsRootPath + "/" + sourceTable + "/"

print ("Param -\'Variables':")
print (POSsourceSilverFilePath)
print (SPLsourceSilverFilePath)
print (SENTsourceSilverFilePath)
print (STMTsourceSilverFilePath)
print (badRecordsFilePath)
print (recordCountFilePath)

# COMMAND ----------

# Temporary cell - DELETE
#now = datetime.now() 
GoldFactTableName = "FCT_BP_INV_LINE_ITEM_TRANS"
#POSsourceSilverPath = "Revenue/Benefits/POSTING_RECORD/" +now.strftime("%Y") + "/06"
#POSsourceSilverPath = SilverContainerPath + POSsourceSilverPath
#POSsourceSilverFile = "POSTING_RECORD_2021_06_04.parquet"
#POSsourceSilverFilePath = POSsourceSilverPath + "/" + POSsourceSilverFile
#badRecordsPath = badRecordsRootPath + GoldFactTableName + "/"
#recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"
#BatchId = "1afc2b6c-d987-48cc-ae8c-a7f41ea27249"
#WorkFlowId ="8fc2895d-de32-4bf4-a531-82f0c6774221"
POSsourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Revenue/Benefits/vw_POSTING_RECORD_AllRecs/" + yymmManual + "/vw_POSTING_RECORD_AllRecs_" + yyyymmddManual + ".parquet"

SPLsourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Revenue/Benefits/vw_POSTED_SPLIT_AllRecs/" + yymmManual + "/vw_POSTED_SPLIT_AllRecs_" + yyyymmddManual + ".parquet"

SENTsourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Revenue/Benefits/vw_STATEMENT_ENTRY_AllRecs/" + yymmManual + "/vw_STATEMENT_ENTRY_AllRecs_" + yyyymmddManual + ".parquet"

STMTsourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Revenue/Benefits/vw_STATEMENT_AllRecs/" + yymmManual + "/vw_STATEMENT_AllRecs_" + yyyymmddManual + ".parquet"

planSourceSilverFilePath ="abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Policy/Benefits/vw_PLAN_AllPlans/" + yymmManual + "/vw_PLAN_AllPlans_" + yyyymmddManual + ".parquet"

adhocSourceSilverFilePath="abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Policy/Benefits/vw_ADHOC_PRODUCT_AllPlans/" + yymmManual + "/vw_ADHOC_PRODUCT_AllPlans_" + yyyymmddManual + ".parquet"

# COMMAND ----------

# MAGIC %scala
# MAGIC // Temporary cell - DELETE
# MAGIC lazy val GoldFactTableName = "FCT_BP_INV_LINE_ITEM_TRANS"

# COMMAND ----------

# Do not proceed if any of the parameters are missing
if (GoldFactTableName == "" or POSsourceSilverFilePath == ""):
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Input parameters are missing"}}})

# COMMAND ----------

# Read Posting record source file
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
try:
 
  POSsourceSilverDF = spark.read.parquet(POSsourceSilverFilePath)
  #display(POSsourceSilverDF)
except:
  # Log the error message
  errorDF = spark.createDataFrame([
    (GoldFactTableName,now,POSsourceSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + POSsourceSilverFilePath}}})  

# COMMAND ----------

# Read Posted Split source file
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
try:
 
  SPLsourceSilverDF = spark.read.parquet(SPLsourceSilverFilePath)
  #display(SPLsourceSilverDF)
except:
  # Log the error message
  errorDF = spark.createDataFrame([
    (GoldFactTableName,now,SPLsourceSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + SPLsourceSilverFilePath}}})

# COMMAND ----------

# Read source file
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
try:
 
  SENTsourceSilverDF = spark.read.parquet(SENTsourceSilverFilePath)
  #display(SENTsourceSilverDF)
except:
  # Log the error message
  errorDF = spark.createDataFrame([
    (GoldFactTableName,now,SENTsourceSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + SENTsourceSilverFilePath}}})

# COMMAND ----------

# Read statement source file
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
try:
 
  STMTsourceSilverDF = spark.read.parquet(STMTsourceSilverFilePath)
  #display(STMTsourceSilverDF)
except:
  # Log the error message
  errorDF = spark.createDataFrame([
    (GoldFactTableName,now,STMTsourceSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + STMTsourceSilverFilePath}}})

# COMMAND ----------

# Read Plan file
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
try:
 
  plansourceSilverDF = spark.read.parquet(planSourceSilverFilePath)
  #display(STMTsourceSilverDF)
except:
  # Log the error message
  errorDF = spark.createDataFrame([
    (GoldFactTableName,now,planSourceSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + planSourceSilverFilePath}}})

# COMMAND ----------

# Read adhoc product file
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
try:
 
  adhocSourceSilverDF = spark.read.parquet(adhocSourceSilverFilePath)
  #display(STMTsourceSilverDF)
except:
  # Log the error message
  errorDF = spark.createDataFrame([
    (GoldFactTableName,now,adhocSourceSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + adhocSourceSilverFilePath}}})

# COMMAND ----------

POSsourceSilverDF.createOrReplaceTempView("POSTING_RECORD")
SPLsourceSilverDF.createOrReplaceTempView("POSTED_SPLIT")
SENTsourceSilverDF.createOrReplaceTempView("STATEMENT_ENTRY")
STMTsourceSilverDF.createOrReplaceTempView("STATEMENT")
plansourceSilverDF.createOrReplaceTempView("PLAN")
adhocSourceSilverDF.createOrReplaceTempView("ADHOC_PRODUCT")

# COMMAND ----------

# Get union set of records
unionDataDF = spark.sql(
f""" 
SELECT
BILLING_CARRIER_ID,
BOR_CLIENT_ID,
BROKERAGE_DEPARTMENT_ID,
BROKERAGE_OFFICE_ID,
CARRIER_ID,
CLIENT_ID,
PLAN_TYPE_ID,
SALES_LEAD_ID,
SERVICE_LEAD_ID,
PLAN_ID As PLAN_ADHOC,
BROKERAGE_OFFICE_ID
FROM PLAN 
UNION 
SELECT
BILLING_CARRIER_ID,
BOR_CLIENT_ID,
BROKERAGE_DEPARTMENT_ID,
BROKERAGE_OFFICE_ID,
CARRIER_ID,
CLIENT_ID,
PLAN_TYPE_ID,
SALES_LEAD_ID,
SERVICE_LEAD_ID,
ADHOC_PRODUCT_ID As PLAN_ADHOC,
BROKERAGE_OFFICE_ID
FROM ADHOC_PRODUCT 
"""
)
#display(unionDataDF)

# COMMAND ----------

unionDataDF.createOrReplaceTempView("PLAN_ADHOC_PRODUCT")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_BP_LOB]) carrier"
carrierDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# Register table so it is accessible via SQL Context
carrierDF.createOrReplaceTempView("DIM_BP_LOB")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_BP_CARRIER]) carrier"
carrierDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
display(carrierDF)
# Register table so it is accessible via SQL Context
carrierDF.createOrReplaceTempView("DIM_BP_CARRIER")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_BP_CLIENT]) client"
clientDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
display(clientDF)
# Register table so it is accessible via SQL Context
clientDF.createOrReplaceTempView("DIM_BP_CLIENT")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_BP_BROKER]) BROKER"
brokerDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
display(brokerDF)
# Register table so it is accessible via SQL Context
brokerDF.createOrReplaceTempView("DIM_BP_BROKER")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_BP_ORG]) ORG"
carrierDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
display(carrierDF)
# Register table so it is accessible via SQL Context
carrierDF.createOrReplaceTempView("DIM_BP_ORG")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_BP_CONTACT]) contact"
carrierDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
display(carrierDF)
# Register table so it is accessible via SQL Context
carrierDF.createOrReplaceTempView("DIM_BP_CONTACT")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_BP_INV]) INV"
carrierDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
display(carrierDF)
# Register table so it is accessible via SQL Context
carrierDF.createOrReplaceTempView("DIM_BP_INV")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_BP_POL]) POL"
carrierDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
display(carrierDF)
# Register table so it is accessible via SQL Context
carrierDF.createOrReplaceTempView("DIM_BP_POL")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_BP_PRODUCER_CODE]) PC"
carrierDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
display(carrierDF)
# Register table so it is accessible via SQL Context
carrierDF.createOrReplaceTempView("DIM_BP_PRODUCER_CODE")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_BP_CLIENT_CONTACT]) cc"
carrierDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
display(carrierDF)
# Register table so it is accessible via SQL Context
carrierDF.createOrReplaceTempView("DIM_BP_CLIENT_CONTACT")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_BP_CLIENT_ADDRESS]) ca"
carrierDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
display(carrierDF)
# Register table so it is accessible via SQL Context
carrierDF.createOrReplaceTempView("DIM_BP_CLIENT_ADDRESS")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_BP_CLIENT_ACCOUNT_TEAM]) cat"
carrierDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
display(carrierDF)
# Register table so it is accessible via SQL Context
carrierDF.createOrReplaceTempView("DIM_BP_CLIENT_ACCOUNT_TEAM")

# COMMAND ----------

finalDataDF = spark.sql(
f"""
SELECT distinct
pr.POSTING_RECORD_ID as INV_LINE_ITM_ID,
pr.STATEMENT_ENTRY_ID as INV_ENTRY_ID,
POSTED_AMOUNT as POSTD_AMT,
EFFECTIVE_DATE as EFF_DT,
POSTED_DATE as POSTD_DT,
SPLIT_BASIS_TYPE_DESC as SPLIT_BASIS_TYP_DESC,
STATEMENT_SPLIT_IND as INV_SPLT_IND,
pr.SPLIT_COLUMN_TYPE_DESC as SPLT_COLMN_TYP_DESC,
POSTED_SPLIT_ID as INV_LINE_ITM_TRNS_ID,
ps.POSTING_RECORD_ID as INV_LINE_ITM_SPLT_ID,
PAYEE_ID as PRDUCR_ID,
ps.AMOUNT as AMT,
PERCENTAGE as PERCNTGE,
DEPENDENT_PAYEE_IND as DEPENDNT_PAYEE_IND,
se.STATEMENT_ENTRY_ID INV_STMNT_ENTRY_ID,
se.STATEMENT_ID as INV_ID ,
CASE WHEN se.PLAN_ID IS NOT NULL THEN se.PLAN_ID ELSE ADHOC_PRODUCT_ID END as POL_ID,
IS_POSTED as IS_POSTD,
PREMIUM_AMOUNT as PREM_AMT,
TAM_TRANSACTION_TYPE_ID as TAM_TRNS_TYPE_ID,
STATEMENT_SPLIT_ID as INV_SPLIT_ID,
ADHOC_PRODUCT_ID as ADHC_POL_ID,
LAST_POSTED_DATE as LAST_POSTD_DATE,--statement
paunion.BILLING_CARRIER_ID as BILING_CARIER_ID,
paunion.CARRIER_ID as CARIER_ID,
BOR_CLIENT_ID as BOR_CLINT_ID,
BROKERAGE_DEPARTMENT_ID as BRKERGE_DEPT_ID,
paunion.BROKERAGE_OFFICE_ID as BRNCH_ID,
CLIENT_ID as CLNT_ID,
PLAN_TYPE_ID as LOB_ID,
SALES_LEAD_ID as SALES_LEAD_ID,
--add surrogate Ids here
coalesce(SURR_ORG_ID,0) As SURR_ORG_ID ,
--SURR_BROKR_ID ,
coalesce(icarr.SURR_CARIER_ID,0) As SURR_ISSNG_CARIER_ID,
coalesce(bcarr.SURR_CARIER_ID,0) As SURR_BLLNG_CARIER_ID,
coalesce(SURR_CLNT_ID,0) As SURR_CLNT_ID,
--SURR_CNTCT_ID ,
coalesce(SURR_INV_ID,0) As SURR_INV_ID,
coalesce(pol.SURR_POL_ID,0) As SURR_POL_ID,
SURR_PRODCR_CD_ID,
coalesce(SURR_LOB_ID,0) As SURR_LOB_ID ,
--pr.POSTING_RECORD_ID as BP_POSTING_REC_ID,
'' as PRODUCER_CODE_ID,
SERVICE_LEAD_ID as SERVICE_LEAD_ID,
s.CHECK_NUMBER as CHECK_NUMBER, --statement
s.CHECK_DATE as CHECK_DT, --statement
VOID_IND as VOID_INDICATOR,
VOIDED_RECORD_ID as BP_VOID_POSTING_REC_ID,
'' as ACPT_TOLERANCE_IND,
APPLY_TO_DATE as APLY_TO_DT,
SAGITTA_TRANSACTION_CODE_DESC as SAGITTA_TRANS_CODE_DESC,
TAM_TRANSACTION_TYPE_DESC as TAM_TRANS_TYPE_DESC,
NUMBER_OF_LIVES as NO_OF_LIVES,
s.RowBeginDate as SRC_ROW_BEGIN_DT,
s.RowEndDate as SRC_ROW_END_DT
FROM POSTED_SPLIT ps
JOIN POSTING_RECORD pr on ps.POSTING_RECORD_ID = pr.POSTING_RECORD_ID
JOIN STATEMENT_ENTRY se on pr.STATEMENT_ENTRY_ID = se.STATEMENT_ENTRY_ID
JOIN STATEMENT s on se.STATEMENT_ID = s.STATEMENT_ID
JOIN DIM_BP_PRODUCER_CODE pc on ps.PAYEE_ID = pc.PRODCR_CD_ID
LEFT JOIN DIM_BP_POL pol on (pol.POL_ID = se.PLAN_ID or pol.POL_ID = se.ADHOC_PRODUCT_ID)
LEFT JOIN PLAN_ADHOC_PRODUCT paunion on (paunion.PLAN_ADHOC = se.PLAN_ID or paunion.PLAN_ADHOC = se.ADHOC_PRODUCT_ID)
LEFT JOIN DIM_BP_CLIENT c on paunion.CLIENT_ID = c.CLNT_ID
LEFT JOIN DIM_BP_ORG org on c.OWNR_OFC_ID = org.BRNCH_NUM
LEFT JOIN DIM_BP_CARRIER icarr on paunion.CARRIER_ID = icarr.CARIER_ID
LEFT JOIN DIM_BP_CARRIER bcarr on paunion.BILLING_CARRIER_ID = bcarr.CARIER_ID
LEFT JOIN DIM_BP_INV inv on se.STATEMENT_ID = inv.INV_ID
LEFT JOIN DIM_BP_LOB lob on paunion.PLAN_TYPE_ID = lob.BP_LOB_ID
-- where ps.POSTING_RECORD_ID = 411685;
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
    (GoldFactTableName,now,sourceRecordCount,targetRecordCount,sourceSilverFilePath,BatchId,WorkFlowId)
  ],["TableName","ETL_CREATED_DT","SourceRecordCount","TargetRecordCount","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID"])

# Write the recon record to SQL DB
reconDF.write.jdbc(url=Url, table=reconTable, mode="append")

# COMMAND ----------

# MAGIC %scala
# MAGIC // Truncate Fact table
# MAGIC lazy val GoldFactTableNameComplete = finalTableSchema + "." + GoldFactTableName
# MAGIC lazy val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
# MAGIC lazy val stmt = connection.createStatement()
# MAGIC lazy val sql = "truncate table " + GoldFactTableNameComplete;
# MAGIC stmt.execute(sql)
# MAGIC connection.close()

# COMMAND ----------

# MAGIC %scala
# MAGIC // Disable Constraints for Fact table
# MAGIC lazy val GoldFactTableNameComplete = finalTableSchema + "." + GoldFactTableName
# MAGIC lazy val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
# MAGIC lazy val stmt = connection.createStatement()
# MAGIC lazy val sql = "ALTER TABLE " + GoldFactTableNameComplete + " NOCHECK CONSTRAINT ALL";
# MAGIC stmt.execute(sql)
# MAGIC connection.close()

# COMMAND ----------

GoldFactTableNameComplete = finalTableSchema + "." + GoldFactTableName
finalDataDF.write.jdbc(url=Url, table=GoldFactTableNameComplete, mode="append")

# COMMAND ----------

# MAGIC %scala
# MAGIC // Enable Constraints for Fact table
# MAGIC lazy val GoldFactTableNameComplete = finalTableSchema + "." + GoldFactTableName
# MAGIC lazy val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
# MAGIC lazy val stmt = connection.createStatement()
# MAGIC lazy val sql = "ALTER TABLE " + GoldFactTableNameComplete + " WITH CHECK CHECK CONSTRAINT ALL";
# MAGIC stmt.execute(sql)
# MAGIC connection.close()

# COMMAND ----------

# Write the final parquet file to Gold zone
dbutils.widgets.text("ProjectFolderName", "","")
sourceGoldPath = dbutils.widgets.get("ProjectFolderName")
dbutils.widgets.text("ProjectFileName", "","")
sourceGoldFile = dbutils.widgets.get("ProjectFileName")

spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInWrite=CORRECTED")
sourceGoldFilePath = GoldContainerPath + sourceGoldPath + "/" + sourceGoldFile
finalDataDF.write.mode("overwrite").parquet(sourceGoldFilePath)