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
# MAGIC lazy val GoldFactTableName = dbutils.widgets.get("TableName")

# COMMAND ----------

# Set the path for Silver layer for Nexsure

now = datetime.now() 

dbutils.widgets.text("TableName", "","")
GoldFactTableName = dbutils.widgets.get("TableName")

#sourceSilverPath = "Invoice/Nexsure/DimRateType/2021/05"
sourceSilverPath = "Invoice/Nexsure/FactInvoiceLineItem/" +now.strftime("%Y") + "/" + now.strftime("%m")
#sourceSilverPath = "Invoice/Nexsure/FactInvoiceLineItem/" +now.strftime("%Y") + "/" + "05"
sourceSilverPath = SilverContainerPath + sourceSilverPath

sourceSilverFile = "FactInvoiceLineItem_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
#sourceSilverFile = "FactInvoiceLineItem_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_21.parquet"
#sourceSilverFile = "FactInvoiceLineItem_" + now.strftime("%Y") +"_"+ "05" + "_21.parquet"
sourceSilverFilePath = sourceSilverPath + "/" + sourceSilverFile

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


# Temporary cell - DELETE
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

# COMMAND ----------

# MAGIC %scala
# MAGIC // Temporary cell - DELETE
# MAGIC lazy val GoldFactTableName = "FCT_NX_INV_LINE_ITEM_TRANS"

# COMMAND ----------

# Do not proceed if any of the parameters are missing
if (GoldFactTableName == "" or sourceSilverPath == "" or sourceSilverFile == ""):
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Input parameters are missing"}}})

# COMMAND ----------

# Read source file
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
try:
 
  sourceSilverDF = spark.read.option("badRecordsPath", badRecordsPath).schema("AgencyCommissionBasisKey integer,AppliedRate decimal(19,4),BasisAmt decimal(19,4),BillingCarrierKey integer,BKFactTable string,ClientKey integer,CommissionableTaxableKey integer,CommissionFeeBasedOnKey integer,CreatedByEmployeeKey integer,DateBookedKey integer,DBSourceKey integer,DueAmt decimal(19,4),EffectiveDateKey integer,FeeGLAccountKey integer,FeeServiceProviderKey integer,GLEntityKey integer,GLPeriodKey integer,InsertAuditKey integer,InternalID integer,InvoiceInstallmentKey integer,InvoiceKey integer,InvoiceLineItemEntityKey integer,InvoiceLineItemKey integer,IssuingCarrierKey integer,LineItemLoBKey integer,OrgStructureKey integer,ParentCarrierKey integer,PeopleCommissionBasisKey integer,PolicyKey integer,PremiumFeeBasisKey integer,ProductionAmt decimal(19,4),ProductionCreditPct decimal(7,4),ProductionResponsibilityKey integer,RateTypeKey integer,ReceivableEntityKey integer,RetailAgentCommissionBasisKey integer,UpdateAuditKey integer").parquet(sourceSilverFilePath)
  #display(sourceSilverDF)
except:
  print("Schema mismatch")
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Schema mismatch: " + sourceSilverFilePath}}})

# COMMAND ----------

sourceSilverDF.createOrReplaceTempView("FCT_NX_INV_LINE_ITEM_TRANS")

# COMMAND ----------

pushdown_query = "(select * from [Gold].[DIM_NX_CARRIER]) carrier"
carrierDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(carrierDF)
# Register table so it is accessible via SQL Context
carrierDF.createOrReplaceTempView("DIM_NX_CARRIER")

# COMMAND ----------

pushdown_query = "(select * from [Gold].[DIM_NX_CLIENT]) client"
clientDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(clientDF)
# Register table so it is accessible via SQL Context
clientDF.createOrReplaceTempView("DIM_NX_CLIENT")

# COMMAND ----------

pushdown_query = "(select * from [Gold].[DIM_NX_COMM_TAX]) commtax"
commtaxDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(commtaxDF)
# Register table so it is accessible via SQL Context
commtaxDF.createOrReplaceTempView("DIM_NX_COMM_TAX")

# COMMAND ----------

pushdown_query = "(select * from [Gold].[DIM_NX_INV]) inv"
invDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(invDF)
# Register table so it is accessible via SQL Context
invDF.createOrReplaceTempView("DIM_NX_INV")

# COMMAND ----------

pushdown_query = "(select * from [Gold].[DIM_NX_INV_LINE_ITEM_ENTITY]) invlnitment"
invlnitmentDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(invlnitmentDF)
# Register table so it is accessible via SQL Context
invlnitmentDF.createOrReplaceTempView("DIM_NX_INV_LINE_ITEM_ENTITY")

# COMMAND ----------

pushdown_query = "(select * from [Gold].[DIM_NX_LOB]) lob"
lobDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(lobDF)
# Register table so it is accessible via SQL Context
lobDF.createOrReplaceTempView("DIM_NX_LOB")

# COMMAND ----------

pushdown_query = "(select * from [Gold].[DIM_NX_ORG]) org"
orgDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(orgDF)
# Register table so it is accessible via SQL Context
orgDF.createOrReplaceTempView("DIM_NX_ORG")

# COMMAND ----------

pushdown_query = "(select * from [Gold].[DIM_NX_RATE_TYPE]) ratetype"
ratetypeDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(ratetypeDF)
# Register table so it is accessible via SQL Context
ratetypeDF.createOrReplaceTempView("DIM_NX_RATE_TYPE")

# COMMAND ----------

pushdown_query = "(select * from [Gold].[DIM_NX_RESPONSIBILITY]) resp"
respDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(respDF)
# Register table so it is accessible via SQL Context
respDF.createOrReplaceTempView("DIM_NX_RESPONSIBILITY")

# COMMAND ----------

pushdown_query = "(select * from [Gold].[DIM_NX_EMP]) emp"
empDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(empDF)
# Register table so it is accessible via SQL Context
empDF.createOrReplaceTempView("DIM_NX_EMP")

# COMMAND ----------

pushdown_query = "(select * from [Gold].[DIM_NX_POL]) pol"
polDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(empDF)
# Register table so it is accessible via SQL Context
polDF.createOrReplaceTempView("DIM_NX_POL")

# COMMAND ----------

pushdown_query = "(select * from [Gold].[DIM_NX_DATE]) date"
dtDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(empDF)
# Register table so it is accessible via SQL Context
dtDF.createOrReplaceTempView("DIM_NX_DATE")

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC -- Can be deleted later just added for testing purpose
# MAGIC /*SELECT
# MAGIC InvoiceLineItemKey as INV_LINE_ITEM_TRNS_KEY ,
# MAGIC CommissionFeeBasedOnKey as COMM_FEE_BSD_ON_KEY  ,
# MAGIC GLPeriodKey as GL_PRD_KEY ,
# MAGIC DateBookedKey as DT_BOKD_KEY  ,
# MAGIC EffectiveDateKey as EFF_DT_KEY ,
# MAGIC InvoiceKey as INV_KEY  ,
# MAGIC LineItemLoBKey as LOB_KEY  ,
# MAGIC PolicyKey as POLICY_KEY  ,
# MAGIC ClientKey as CLIENT_KEY  ,
# MAGIC InvoiceLineItemEntityKey as INV_LINE_ITEM_ENTY_KEY ,
# MAGIC RateTypeKey as RATE_TYPE_KEY  ,
# MAGIC CommissionableTaxableKey as COMM_TAX_KEY ,
# MAGIC ProductionResponsibilityKey as PRD_RESP_KEY  ,
# MAGIC CreatedByEmployeeKey as CRETD_BY_EMP_KEY  ,
# MAGIC BasisAmt as BASIS_AMT  ,
# MAGIC DueAmt as DUE_AMT  ,
# MAGIC AppliedRate as APPLIED_RATE  ,
# MAGIC ProductionAmt as PRD_AMT  ,
# MAGIC ProductionCreditPct as PRD_CREDIT_PCT  ,
# MAGIC InternalID as INT_ID  ,
# MAGIC BKFactTable as BK_FACT_TABLE  ,
# MAGIC InsertAuditKey as SRC_AUD_INS_KEY  ,
# MAGIC UpdateAuditKey as SRC_AUD_UPD_KEY,
# MAGIC ParentCarrierKey as PARNT_CARIER_KEY  ,
# MAGIC BillingCarrierKey as BLLNG_CARIER_KEY  ,
# MAGIC IssuingCarrierKey as ISSNG_CARIER_KEY  ,
# MAGIC OrgStructureKey as ORG_KEY  ,
# MAGIC SURR_RESP_ID as SURR_RESP_ID  ,
# MAGIC SURR_RATE_ID as SURR_RATE_ID ,
# MAGIC SURR_ORG_ID as SURR_ORG_ID  ,
# MAGIC SURR_LOB_ID as SURR_LOB_ID  ,
# MAGIC SURR_INV_ID as SURR_INV_ID,
# MAGIC SURR_POL_ID as SURR_POL_ID,
# MAGIC SURR_EMP_ID as SURR_EMP_ID,
# MAGIC SURR_LINE_ITEM_ID as SURR_LINE_ITEM_ID  ,
# MAGIC SURR_COMM_TAX_ID as SURR_COMM_TAX_ID  ,
# MAGIC SURR_CLIENT_ID as SURR_CLIENT_ID  ,
# MAGIC BCarr.SURR_CARIER_ID as SURR_CARIER_ID  ,
# MAGIC SURR_DATE_ID as	SURR_DATE_ID  ,
# MAGIC '{ BatchId }' AS ETL_BATCH_ID,
# MAGIC '{ WorkFlowId }' AS ETL_WRKFLW_ID,
# MAGIC current_timestamp() AS ETL_CREATED_DT,
# MAGIC current_timestamp() AS ETL_UPDATED_DT
# MAGIC FROM FCT_NX_INV_LINE_ITEM_TRANS fact
# MAGIC LEFT JOIN DIM_NX_CARRIER BCarr on BCarr.CARIER_KEY = fact.BillingCarrierKey
# MAGIC LEFT JOIN DIM_NX_CARRIER ICarr on ICarr.CARIER_KEY = fact.IssuingCarrierKey
# MAGIC LEFT JOIN DIM_NX_RESPONSIBILITY Rsp on fact.ProductionResponsibilityKey = Rsp.RESPBLTY_KEY
# MAGIC LEFT JOIN DIM_NX_POL pol on fact.PolicyKey = pol.POLICY_KEY
# MAGIC LEFT JOIN DIM_NX_CLIENT cl on fact.ClientKey = cl.CLIENT_KEY
# MAGIC LEFT JOIN DIM_NX_LOB LOB on fact.LineItemLoBKey = LOB.LOB_KEY
# MAGIC JOIN DIM_NX_DATE dt on fact.DateBookedKey = dt.DT_KEY
# MAGIC JOIN DIM_NX_INV inv on fact.InvoiceKey = inv.INV_KEY
# MAGIC JOIN DIM_NX_EMP emp on fact.CreatedByEmployeeKey = emp.EMP_KEY
# MAGIC JOIN DIM_NX_RATE_TYPE Rtp on fact.RateTypeKey = Rtp.RATE_TYP_KEY
# MAGIC JOIN DIM_NX_ORG Org on fact.OrgStructureKey = Org.ORG_KEY
# MAGIC JOIN DIM_NX_INV_LINE_ITEM_ENTITY ILItm on fact.InvoiceLineItemEntityKey = ILItm.INV_LINE_ITM_ENTY_KEY
# MAGIC JOIN DIM_NX_COMM_TAX Comm on fact.CommissionableTaxableKey = Comm.COMM_TAX_KEY limit 100
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Can be deleted later just added for testing purpose
# MAGIC /*
# MAGIC SELECT COUNT(1)
# MAGIC FROM FCT_NX_INV_LINE_ITEM_TRANS fact
# MAGIC LEFT JOIN DIM_NX_CARRIER BCarr on BCarr.CARIER_KEY = fact.BillingCarrierKey
# MAGIC LEFT JOIN DIM_NX_CARRIER ICarr on ICarr.CARIER_KEY = fact.IssuingCarrierKey
# MAGIC LEFT JOIN DIM_NX_RESPONSIBILITY Rsp on fact.ProductionResponsibilityKey = Rsp.RESPBLTY_KEY
# MAGIC LEFT JOIN DIM_NX_POL pol on fact.PolicyKey = pol.POLICY_KEY
# MAGIC LEFT JOIN DIM_NX_CLIENT cl on fact.ClientKey = cl.CLIENT_KEY
# MAGIC LEFT JOIN DIM_NX_LOB LOB on fact.LineItemLoBKey = LOB.LOB_KEY
# MAGIC JOIN DIM_NX_DATE dt on fact.DateBookedKey = dt.DT_KEY
# MAGIC JOIN DIM_NX_INV inv on fact.InvoiceKey = inv.INV_KEY
# MAGIC JOIN DIM_NX_EMP emp on fact.CreatedByEmployeeKey = emp.EMP_KEY
# MAGIC JOIN DIM_NX_RATE_TYPE Rtp on fact.RateTypeKey = Rtp.RATE_TYP_KEY
# MAGIC JOIN DIM_NX_ORG Org on fact.OrgStructureKey = Org.ORG_KEY
# MAGIC JOIN DIM_NX_INV_LINE_ITEM_ENTITY ILItm on fact.InvoiceLineItemEntityKey = ILItm.INV_LINE_ITM_ENTY_KEY
# MAGIC JOIN DIM_NX_COMM_TAX Comm on fact.CommissionableTaxableKey = Comm.COMM_TAX_KEY
# MAGIC */

# COMMAND ----------

# Get final set of records
finalDataDF = spark.sql(
f""" 
SELECT
InvoiceLineItemKey as INV_LINE_ITEM_TRNS_KEY ,
CommissionFeeBasedOnKey as COMM_FEE_BSD_ON_KEY  ,
GLPeriodKey as GL_PRD_KEY ,
DateBookedKey as DT_BOKD_KEY  ,
EffectiveDateKey as EFF_DT_KEY ,
InvoiceKey as INV_KEY  ,
LineItemLoBKey as LOB_KEY  ,
PolicyKey as POLICY_KEY  ,
ClientKey as CLIENT_KEY  ,
InvoiceLineItemEntityKey as INV_LINE_ITEM_ENTY_KEY ,
RateTypeKey as RATE_TYPE_KEY  ,
CommissionableTaxableKey as COMM_TAX_KEY ,
ProductionResponsibilityKey as PRD_RESP_KEY  ,
CreatedByEmployeeKey as CRETD_BY_EMP_KEY  ,
BasisAmt as BASIS_AMT  ,
DueAmt as DUE_AMT  ,
AppliedRate as APPLIED_RATE  ,
ProductionAmt as PRD_AMT  ,
ProductionCreditPct as PRD_CREDIT_PCT  ,
InternalID as INT_ID  ,
BKFactTable as BK_FACT_TABLE  ,
InsertAuditKey as SRC_AUD_INS_KEY  ,
UpdateAuditKey as SRC_AUD_UPD_KEY,
ParentCarrierKey as PARNT_CARIER_KEY  ,
BillingCarrierKey as BLLNG_CARIER_KEY  ,
IssuingCarrierKey as ISSNG_CARIER_KEY  ,
OrgStructureKey as ORG_KEY  ,
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
coalesce(BCarr.SURR_CARIER_ID, 0) as SURR_CARIER_ID  ,
SURR_DATE_ID as	SURR_DATE_ID  ,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId }' AS ETL_WRKFLW_ID,
current_timestamp() AS ETL_CREATED_DT,
current_timestamp() AS ETL_UPDATED_DT
FROM FCT_NX_INV_LINE_ITEM_TRANS fact
LEFT JOIN DIM_NX_CARRIER BCarr on BCarr.CARIER_KEY = fact.BillingCarrierKey
LEFT JOIN DIM_NX_CARRIER ICarr on ICarr.CARIER_KEY = fact.IssuingCarrierKey
LEFT JOIN DIM_NX_RESPONSIBILITY Rsp on fact.ProductionResponsibilityKey = Rsp.RESPBLTY_KEY
LEFT JOIN DIM_NX_POL pol on fact.PolicyKey = pol.POLICY_KEY
LEFT JOIN DIM_NX_CLIENT cl on fact.ClientKey = cl.CLIENT_KEY
LEFT JOIN DIM_NX_LOB LOB on fact.LineItemLoBKey = LOB.LOB_KEY
JOIN DIM_NX_DATE dt on fact.DateBookedKey = dt.DT_KEY
JOIN DIM_NX_INV inv on fact.InvoiceKey = inv.INV_KEY
JOIN DIM_NX_EMP emp on fact.CreatedByEmployeeKey = emp.EMP_KEY
JOIN DIM_NX_RATE_TYPE Rtp on fact.RateTypeKey = Rtp.RATE_TYP_KEY
JOIN DIM_NX_ORG Org on fact.OrgStructureKey = Org.ORG_KEY
JOIN DIM_NX_INV_LINE_ITEM_ENTITY ILItm on fact.InvoiceLineItemEntityKey = ILItm.INV_LINE_ITM_ENTY_KEY
JOIN DIM_NX_COMM_TAX Comm on fact.CommissionableTaxableKey = Comm.COMM_TAX_KEY
"""
)
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
    (GoldFactTableName,now,sourceRecordCount,targetRecordCount,sourceSilverFilePath,BatchId,WorkFlowId)
  ],["TableName","ETL_CREATED_DT","SourceRecordCount","TargetRecordCount","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID"])

# Write the recon record to SQL DB
reconDF.write.jdbc(url=Url, table=reconTable, mode="append")

# COMMAND ----------

# MAGIC %scala
# MAGIC // Truncate Fact table
# MAGIC //lazy val GoldFactTableName = "gold.FCT_NX_INV_LINE_ITEM_TRANS"
# MAGIC lazy val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
# MAGIC lazy val stmt = connection.createStatement()
# MAGIC lazy val sql = "truncate table " + GoldFactTableName;
# MAGIC stmt.execute(sql)
# MAGIC connection.close()

# COMMAND ----------

GoldFactTableNameComplete = "gold." + GoldFactTableName
finalDataDF.write.jdbc(url=Url, table="[Gold].[FCT_NX_INV_LINE_ITEM_TRANS]", mode="append")