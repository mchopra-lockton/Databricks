# Databricks notebook source
from datetime import datetime

# COMMAND ----------

# MAGIC %run "./Database Config"

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
# MAGIC val GoldDimTableName = dbutils.widgets.get("TableName")
# MAGIC 
# MAGIC val GoldFactTableName = "Gold.FCT_NX_INV_LINE_ITEM_TRANS"
# MAGIC print (GoldDimTableName)
# MAGIC print (GoldFactTableName)

# COMMAND ----------

# Set the path for Silver layer for Nexsure

now = datetime.now() 
#sourceSilverPath = "Reference/Nexsure/DimDate/2021/05"
sourceSilverPath = "Invoice/Nexsure/DimInvoiceInfo/" +now.strftime("%Y") + "/" + now.strftime("%m")
sourceSilverPath = SilverContainerPath + sourceSilverPath

sourceSilverFile = "DimInvoiceInfo_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
#sourceSilverFile = "DimInvoiceInfo_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_21.parquet"
sourceSilverFilePath = sourceSilverPath + "/" + sourceSilverFile

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
#badRecordsPath = "abfss://c360logs@dlsldpdev01v8nkg988.dfs.core.windows.net/Dim_NX_Rate_Type/"
#badRecordsFilePath = "abfss://c360logs@dlsldpdev01v8nkg988.dfs.core.windows.net/Dim_NX_Rate_Type/" + date_time
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
GoldDimTableName = "Dim_NX_Inv"
GoldFactTableName = "FCT_NX_INV_LINE_ITEM_TRANS"
sourceSilverPath = "Invoice/Nexsure/DimInvoiceInfo/" +now.strftime("%Y") + "/05"
sourceSilverPath = SilverContainerPath + sourceSilverPath
sourceSilverFile = "DimInvoiceInfo_2021_05_21.parquet"
sourceSilverFilePath = sourceSilverPath + "/" + sourceSilverFile
badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"
recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"
BatchId = "1afc2b6c-d987-48cc-ae8c-a7f41ea27249"
WorkFlowId ="8fc2895d-de32-4bf4-a531-82f0c6774221"

# COMMAND ----------

# MAGIC %scala
# MAGIC // Temporary cell - DELETE
# MAGIC val GoldDimTableName = "Dim_NX_Inv"

# COMMAND ----------

 # Do not proceed if any of the parameters are missing
if (GoldDimTableName == "" or sourceSilverPath == "" or sourceSilverFile == ""):
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Input parameters are missing"}}})

# COMMAND ----------

# Read source file
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")

try:
 
  sourceSilverDF = spark.read.option("badRecordsPath", badRecordsPath).schema("RebilledInvoiceID int,InvoiceSentToClientFlag boolean,AuditKey int,DBSourceKey int, PolicyMode string,RetAgentReceivableMethod string,PostedStatus string,PostedFlag boolean,InternalMessage string,InvoiceMessage string,RowLastModifiedDate timestamp, InvoiceListBillAllocationID int,InvoiceListBillPlanName string,InvoiceListBillPlanID int,DatePrinted timestamp,BookOfBusinessInvoiceID int,BalanceSheetGroupID int,IncomeExpensesGroupID int,PostingRuleGroupID int,CreatedByEmployeeKey int,OrgStructureKey int,PolicyKey int,ClientKey int,BilledAmountDue  decimal(19,4),CommissionAmountDue decimal(19,4),CalculateAnnualizedFlag boolean,InvoiceStatus string,InvoiceStatusCode int,DateDue timestamp,DateTransactionEffective timestamp,DateProcessed timestamp,DateCreated timestamp,DaysDue int,NumberInstallments int,InstallmentsFlag boolean,InvoiceSourceTypeID int,InvoiceSourceType string,AdvancePaymentFlag boolean,PaymentPlanID int,PaymentPlan string,BillMethodID int,BillMethod string,BillTypeID int,BillType string,InvoiceTypeID int,InvoiceType string,BinderBillInvoiceID int,BinderBillInvoiceKey int,MasterInvoiceID int,MasterInvoiceKey int,ParentInvoiceID int,ParentInvoiceKey int,InvoiceID int,InvoiceKey int").parquet(sourceSilverFilePath)
#display(sourceSilverDF)
except:
  print("Schema mismatch")
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Schema mismatch: " + sourceSilverFilePath}}})

# COMMAND ----------

sourceSilverDF.count()

# COMMAND ----------

# Register table so it is accessible via SQL Context
sourceSilverDF.createOrReplaceTempView("DIM_NX_INV")


# COMMAND ----------

dummyDataDF = spark.sql(
  f"""
SELECT -99999 as INV_KEY,
      -1 as INV_ID,
      -1 as PARNT_INV_KEY,
      -1 as PARNT_INV_ID,
      -1 as MSTR_INV_ID,
      -1 as INV_TYP,
      -1 as BILL_TYP,
      -1 as BILL_MTHD,
      -1 as CRETD_DT,
      -1 as PROCSD_DT,
      -1 as TRANS_EFF_DT,
      -1 as INV_STATUS_CD,
      -1 as INV_STATUS,
      -1 as DB_SRC_KEY,
      -1 as SRC_AUDT_KEY,
     '{ BatchId }' AS ETL_BATCH_ID,
      '{ WorkFlowId}' AS ETL_WRKFLW_ID,
      CURRENT_TIMESTAMP() as ETL_CREATED_DT,
      CURRENT_TIMESTAMP() as ETL_UPDATED_DT
      FROM DIM_NX_INV LIMIT 1
""")

# COMMAND ----------

FinalDataDF = spark.sql(
f"""
SELECT InvoiceKey as INV_KEY,
      InvoiceID as INV_ID,
      ParentInvoiceKey as PARNT_INV_KEY,
      ParentInvoiceID as PARNT_INV_ID,
      MasterInvoiceID as MSTR_INV_ID,
      InvoiceType as INV_TYP,
      BillType as BILL_TYP,
      BillMethod as BILL_MTHD,
      DateCreated as CRETD_DT,
      DateProcessed as PROCSD_DT,
      DateTransactionEffective as TRANS_EFF_DT,
      InvoiceStatusCode as INV_STATUS_CD,
      InvoiceStatus as INV_STATUS,
      DBSourceKey as DB_SRC_KEY,
      AuditKey as SRC_AUDT_KEY,
     '{ BatchId }' AS ETL_BATCH_ID,
      '{ WorkFlowId}' AS ETL_WRKFLW_ID,
      CURRENT_TIMESTAMP() as ETL_CREATED_DT,
      CURRENT_TIMESTAMP() as ETL_UPDATED_DT
      FROM DIM_NX_INV 
""")

# COMMAND ----------

FinalDataDF.count()

# COMMAND ----------

# Do not proceed if there are no records to insert
if (FinalDataDF.count() == 0):
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "There are no records to insert: " + sourceSilverFilePath}}})

# COMMAND ----------

# Create a dataframe for record count
sourceRecordCount = sourceSilverDF.count()
targetRecordCount = FinalDataDF.count()
recordCountDF = spark.createDataFrame([
    (sourceRecordCount,targetRecordCount,)
  ],["SourceRecordCount","TargetRecordCount"])

# Write the record count to ADLS
recordCountDF.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(recordCountFilePath)

# COMMAND ----------

# MAGIC %scala
# MAGIC // Truncate Fact table and Delete data from Dimension table
# MAGIC val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
# MAGIC val stmt = connection.createStatement()
# MAGIC //val sql = "truncate table Gold.FCT_NX_INV_LINE_ITEM_TRANS; delete from Gold.DIM_NX_INV_LINE_ITEM_ENTITY; DBCC CHECKIDENT ('Gold.DIM_NX_INV_LINE_ITEM_ENTITY', RESEED, 0)"
# MAGIC //val sql = "truncate table " + GoldFactTableName + "; delete from " + GoldDimTableNameComplete + "; DBCC CHECKIDENT ('" + GoldDimTableNameComplete + "', RESEED, 0)";
# MAGIC val sql_truncate = "truncate table " + GoldFactTableName
# MAGIC stmt.execute(sql_truncate)
# MAGIC val sql = "exec [Admin].[DropAndCreateFKContraints] @GoldTableName = '" + GoldDimTableName + "'"
# MAGIC stmt.execute(sql)
# MAGIC connection.close()

# COMMAND ----------

GoldDimTableNameComplete = "gold." + GoldDimTableName
dummyDataDF.write.jdbc(url=Url, table=GoldDimTableNameComplete, mode="append")
FinalDataDF.write.jdbc(url=Url, table=GoldDimTableNameComplete, mode="append")