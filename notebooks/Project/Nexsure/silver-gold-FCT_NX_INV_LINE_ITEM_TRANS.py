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

#sourceSilverPath = "Invoice/Nexsure/DimRateType/2021/05"
#FactInvoiceLineItem table
sourceSilverFolderPath = "Invoice/Nexsure/FactInvoiceLineItem/" +now.strftime("%Y") + "/" + now.strftime("%m")
sourceSilverPath = SilverContainerPath + sourceSilverFolderPath
sourceSilverFile = "FactInvoiceLineItem_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
sourceSilverFilePath = sourceSilverPath + "/" + sourceSilverFile

#FactPolicyInfo table
factPInfoSourceSilverFolderPath = "Policy/Nexsure/FactPolicyInfo/" +now.strftime("%Y") + "/" + now.strftime("%m")
factPInfoSourceSilverPath = SilverContainerPath + factPInfoSourceSilverFolderPath
factPInfoSourceSilverFile = "FactPolicyInfo_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"

dbutils.widgets.text("BatchId", "","")
BatchId = dbutils.widgets.get("BatchId")

dbutils.widgets.text("WorkFlowId", "","")
WorkFlowId = dbutils.widgets.get("WorkFlowId")

#Set the file path to log error
badRecordsPath = badRecordsRootPath + GoldFactTableName + "/"


now = datetime.now() # current date and time
date_time = now.strftime("%Y%m%dT%H%M%S")
badRecordsFilePath = badRecordsPath + date_time + "/" + "ErrorRecords"
#badRecordsPath = "abfss://c360logs@dlsldpdev01v8nkg988.dfs.core.windows.net/Dim_NX_Rate_Type/"
#badRecordsFilePath = "abfss://c360logs@dlsldpdev01v8nkg988.dfs.core.windows.net/Dim_NX_Rate_Type/" + date_time
recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"

#Set the file path to log error
#badRecordsPath = badRecordsRootPath + "/" + sourceTable + "/"

print ("Param -\'Variables':")
print (sourceSilverFilePath)
print (badRecordsFilePath)
print (recordCountFilePath)

# COMMAND ----------

# Temporary cell to run manually - DELETE
if (GoldFactTableName == "" or sourceSilverPath == "" or sourceSilverFile == ""):
  now = datetime.now() 
  GoldFactTableName = "FCT_NX_INV_LINE_ITEM_TRANS"
  sourceSilverPath = "Invoice/Nexsure/FactInvoiceLineItem/" +now.strftime("%Y") + "/05"
  sourceSilverPath = SilverContainerPath + sourceSilverPath
  sourceSilverFile = "FactInvoiceLineItem_2021_05_21.parquet"
  sourceSilverFilePath = sourceSilverPath + "/" + sourceSilverFile
  badRecordsPath = badRecordsRootPath + GoldFactTableName + "/"
  recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"
  BatchId = "1afc2b6c-d987-48cc-ae8c-a7f41ea27249"
  WorkFlowId ="8fc2895d-de32-4bf4-a531-82f0c6774221"
  sourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Invoice/Nexsure/FactInvoiceLineItem/2021/06/FactInvoiceLineItem_2021_06_04.parquet"

# COMMAND ----------

# MAGIC %scala
# MAGIC // Temporary cell to run manually - DELETE
# MAGIC if (GoldFactTableName == "") {
# MAGIC lazy val GoldFactTableName = "FCT_NX_INV_LINE_ITEM_TRANS"
# MAGIC }  

# COMMAND ----------

# Do not proceed if any of the parameters are missing
if (GoldFactTableName == "" or sourceSilverPath == "" or sourceSilverFile == ""):
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Input parameters are missing"}}})

# COMMAND ----------

# Read source file
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
try:
 
  sourceSilverDF = spark.read.parquet(sourceSilverFilePath)
  #display(sourceSilverDF)
except:
  # Log the error message
  errorDF = spark.createDataFrame([
    (GoldFactTableName,now,sourceSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceSilverFilePath}}})  

# COMMAND ----------

sourceSilverDF.createOrReplaceTempView("FCT_NX_INV_LINE_ITEM_TRANS")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_NX_CARRIER] where NX_CARIER_KEY <> -1) carrier"
carrierDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(carrierDF)
# Register table so it is accessible via SQL Context
carrierDF.createOrReplaceTempView("DIM_NX_CARRIER")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_NX_CLIENT] where NX_CLIENT_KEY <> -1) client"
clientDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(clientDF)
# Register table so it is accessible via SQL Context
clientDF.createOrReplaceTempView("DIM_NX_CLIENT")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_NX_COMM_TAX]) commtax"
commtaxDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(commtaxDF)
# Register table so it is accessible via SQL Context
commtaxDF.createOrReplaceTempView("DIM_NX_COMM_TAX")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_NX_INV]) inv"
invDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(invDF)
# Register table so it is accessible via SQL Context
invDF.createOrReplaceTempView("DIM_NX_INV")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_NX_INV_LINE_ITEM_ENTITY]) invlnitment"
invlnitmentDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(invlnitmentDF)
# Register table so it is accessible via SQL Context
invlnitmentDF.createOrReplaceTempView("DIM_NX_INV_LINE_ITEM_ENTITY")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_NX_LOB] where NX_LINE_ITEM_LOB_KEY <> -1) lob"
lobDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(lobDF)
# Register table so it is accessible via SQL Context
lobDF.createOrReplaceTempView("DIM_NX_LOB")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_NX_ORG]) org"
orgDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(orgDF)
# Register table so it is accessible via SQL Context
orgDF.createOrReplaceTempView("DIM_NX_ORG")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_NX_RATE_TYPE]) ratetype"
ratetypeDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(ratetypeDF)
# Register table so it is accessible via SQL Context
ratetypeDF.createOrReplaceTempView("DIM_NX_RATE_TYPE")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_NX_RESPONSIBILITY] where NX_RESPBLTY_KEY <> -1) resp"
respDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(respDF)
# Register table so it is accessible via SQL Context
respDF.createOrReplaceTempView("DIM_NX_RESPONSIBILITY")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_NX_EMP]) emp"
empDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(empDF)
# Register table so it is accessible via SQL Context
empDF.createOrReplaceTempView("DIM_NX_EMP")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_NX_POL] where NX_POLICY_KEY <> -1) pol"
polDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(empDF)
# Register table so it is accessible via SQL Context
polDF.createOrReplaceTempView("DIM_NX_POL")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_NX_DATE]) date"
dtDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(empDF)
# Register table so it is accessible via SQL Context
dtDF.createOrReplaceTempView("DIM_NX_DATE")

# COMMAND ----------

# Get final set of records
finalDataDF = spark.sql(
f""" 
SELECT
InvoiceLineItemKey as NX_INV_LINE_ITEM_TRNS_KEY ,
CommissionFeeBasedOnKey as COMM_FEE_BSD_ON_KEY  ,
GLPeriodKey as GL_PRD_KEY ,
DateBookedKey as DT_BOKD_KEY  ,
EffectiveDateKey as EFF_DT_KEY ,
InvoiceKey as INV_KEY  ,
fact.LineItemLoBKey as LOB_KEY  ,
fact.PolicyKey as POLICY_KEY  ,
fact.ClientKey as CLIENT_KEY  ,
fact.InvoiceLineItemEntityKey as INV_LINE_ITEM_ENTY_KEY ,
RateTypeKey as RATE_TYPE_KEY  ,
fact.CommissionableTaxableKey as COMM_TAX_KEY ,
ProductionResponsibilityKey as PRD_RESP_KEY  ,
CreatedByEmployeeKey as CRETD_BY_EMP_KEY  ,
BasisAmt as BASIS_AMT  ,
DueAmt as DUE_AMT  ,
AppliedRate as APPLIED_RATE  ,
ProductionAmt as PRD_AMT  ,
fact.ProductionCreditPct as PRD_CREDIT_PCT  ,
InternalID as INT_ID  ,
BKFactTable as BK_FACT_TABLE  ,
fact.InsertAuditKey as SRC_AUDT_INS_KEY  ,
fact.UpdateAuditKey as SRC_AUDT_UPD_KEY,
fact.ParentCarrierKey as PARNT_CARIER_KEY  ,
fact.BillingCarrierKey as BLLNG_CARIER_KEY  ,
fact.IssuingCarrierKey as ISSNG_CARIER_KEY  ,
fact.OrgStructureKey as ORG_KEY  ,
coalesce(SURR_RESP_ID,0) as SURR_RESP_ID  ,
SURR_RATE_ID as SURR_RATE_ID ,
SURR_ORG_ID as SURR_ORG_ID  ,
coalesce(SURR_LOB_ID,0) as SURR_LOB_ID  ,
SURR_INV_ID as SURR_INV_ID,
coalesce(SURR_POL_ID,0) as SURR_POL_ID,
SURR_EMP_ID as SURR_EMP_ID,
SURR_LINE_ITEM_ID as SURR_LINE_ITEM_ID  ,
SURR_COMM_TAX_ID as SURR_COMM_TAX_ID  ,
coalesce(SURR_CLIENT_ID,0) as SURR_CLIENT_ID  ,
coalesce(ICarr.SURR_CARIER_ID, 0) as SURR_ISSNG_CARIER_ID ,
coalesce(BCarr.SURR_CARIER_ID, 0) as SURR_BLLNG_CARIER_ID ,
SURR_DATE_ID as	SURR_DATE_ID  ,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId }' AS ETL_WRKFLW_ID,
current_timestamp() AS ETL_CREATED_DT,
current_timestamp() AS ETL_UPDATED_DT
FROM FCT_NX_INV_LINE_ITEM_TRANS fact
LEFT JOIN DIM_NX_CARRIER BCarr on fact.BillingCarrierKey = BCarr.NX_CARIER_KEY
LEFT JOIN DIM_NX_CARRIER ICarr on fact.IssuingCarrierKey = ICarr.NX_CARIER_KEY
LEFT JOIN DIM_NX_RESPONSIBILITY Rsp on fact.ProductionResponsibilityKey = Rsp.NX_RESPBLTY_KEY
LEFT JOIN DIM_NX_POL pol on fact.PolicyKey = pol.NX_POLICY_KEY
LEFT JOIN DIM_NX_CLIENT cl on fact.ClientKey = cl.NX_CLIENT_KEY
LEFT JOIN DIM_NX_LOB LOB on fact.LineItemLoBKey = LOB.NX_LINE_ITEM_LOB_KEY
JOIN DIM_NX_DATE dt on fact.DateBookedKey = dt.DT_KEY
JOIN DIM_NX_INV inv on fact.InvoiceKey = inv.NX_INV_KEY
JOIN DIM_NX_EMP emp on fact.CreatedByEmployeeKey = emp.NX_EMP_KEY
JOIN DIM_NX_RATE_TYPE Rtp on fact.RateTypeKey = Rtp.NX_RATE_TYP_KEY
JOIN DIM_NX_ORG Org on fact.OrgStructureKey = Org.NX_ORG_KEY
JOIN DIM_NX_INV_LINE_ITEM_ENTITY ILItm on fact.InvoiceLineItemEntityKey = ILItm.NX_INV_LINE_ITM_ENTY_KEY
JOIN DIM_NX_COMM_TAX Comm on fact.CommissionableTaxableKey = Comm.NX_COMM_TAX_KEY
"""
)
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

# Load the records to final table
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