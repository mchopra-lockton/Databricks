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

now= datetime.now()   
#sourceSilverPath = "Reference/Nexsure/DimDate/2021/05"
sourceSilverFolderPath = "Invoice/Benefits/INVOICE/" +now.strftime("%Y") + "/" + now.strftime("%m")
sourceSilverPath = SilverContainerPath + sourceSilverFolderPath
sourceSilverFile = "INVOICE_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
#sourceSilverFile = "DimCLIENT_ + now.strftime("%Y") + "_" + now.strftime("%m") + "_21.parquet"
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
#badRecordsPath = "abfss://c360logs@dlsldpdev01v8nkg988.dfs.core.windows.net/DIM_BP_CARRIER/"
#badRecordsFilePath = "abfss://c360logs@dlsldpdev01v8nkg988.dfs.core.windows.net/DIM_BP_CARRIER/" + date_time
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
GoldFactTableName = "FCT_SAG_INV_LINE_ITEM_TRANS"
badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"
recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"
BatchId = "1afc2b6c-d987-48cc-ae8c-a7f41ea27249"
WorkFlowId ="8fc2895d-de32-4bf4-a531-82f0c6774221"
sourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Invoice/Benefits/INVOICE/" + yymmManual + "/INVOICE_" + yyyymmddManual + ".parquet"

# COMMAND ----------

# MAGIC %scala
# MAGIC // Temporary cell - DELETE
# MAGIC lazy val GoldFactTableName = "FCT_SAG_INV_LINE_ITEM_TRANS"

# COMMAND ----------

 # Do not proceed if any of the parameters are missing
if (GoldFactTableName == "" or sourceSilverPath == "" or sourceSilverFile == ""):
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
    (GoldFactTableName,now,sourceSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceSilverFilePath}}}) 

# COMMAND ----------

# Register table so it is accessible via SQL Context
sourceSilverDF.createOrReplaceTempView("FCT_SAG_INV_LINE_ITEM_TRANS")


# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_BP_LOB]) lob"
clientDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(clientDF)
# Register table so it is accessible via SQL Context
clientDF.createOrReplaceTempView("DIM_BP_LOB")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_BP_CARRIER]) carrier"
clientDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(clientDF)
# Register table so it is accessible via SQL Context
clientDF.createOrReplaceTempView("DIM_BP_CARRIER")

# COMMAND ----------

pushdown_query = "(select BP_CLNT_ID,SURR_CLNT_ID from [dbo].[DIM_BP_CLIENT]) client"
clientDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(clientDF)
# Register table so it is accessible via SQL Context
clientDF.createOrReplaceTempView("DIM_BP_CLIENT")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_BP_PRODUCER_CODE]) producercode"
clientDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(clientDF)
# Register table so it is accessible via SQL Context
clientDF.createOrReplaceTempView("DIM_BP_PRODUCER_CODE")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_BP_POL]) pol"
clientDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(clientDF)
# Register table so it is accessible via SQL Context
clientDF.createOrReplaceTempView("DIM_BP_POL")

# COMMAND ----------

finalDataDF = spark.sql(
f"""
SELECT 
InvoiceKey as SAG_INV_KEY,
AgcyAmt as AGCY_AMT,
AgcyPct as AGCY_PCT,
InvDt as INV_DT,
InvNo as INV_NO,
PayCd as PAY_CD,
PayName as PAY_NAME,
PayType as PAY_TYPE,
PayTypeName as PAY_TYPE_NAME,
PEDt as PE_DT,
PolID as POL_ID,
ProdAmt as PROD_AMT,
ProdCd as PROD_CD,
ProdEmplID as PRODUCER_ID,
ProdName as PROD_NAME,
ProdPct as PROD_PCT,
TransAmt as TRANS_AMT,
TransCd as TRANS_CD,
TransDesc as TRANS_DESC,
TransNo as TRANS_NO,
VoidID as VOID_ID,
coalesce(lob.SURR_LOB_ID,0) as SURR_LOB_ID,
coalesce(c.SURR_CLNT_ID,0) as SURR_CLNT_ID  ,
coalesce(icarr.SURR_CARIER_ID,0) as SURR_ISSNG_CARIER_ID ,
coalesce(bcarr.SURR_CARIER_ID,0) as SURR_BLLNG_CARIER_ID ,
coalesce(c.SURR_ORG_ID,0) AS SURR_ORG_ID,
coalesce(pol.SURR_POL_ID,0) as SURR_POL_ID,
-1 AS SURR_PRODCR_CD_ID,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId}' AS ETL_WRKFLW_ID,
CURRENT_TIMESTAMP() as ETL_CREATED_DT,
CURRENT_TIMESTAMP() as ETL_UPDATED_DT
FROM FCT_SAG_INV_LINE_ITEM_TRANS sag
LEFT JOIN DIM_BP_CLIENT c on sag.ClientBPID = c.BP_CLNT_ID
LEFT JOIN DIM_BP_POL pol on sag.PolBPID = pol.POL_ID
LEFT JOIN DIM_BP_CARRIER icarr on pol.ISUNG_CARIER_ID = icarr.CARIER_ID
LEFT JOIN DIM_BP_CARRIER bcarr on pol.BILNG_CARIER_ID = bcarr.CARIER_ID
LEFT JOIN DIM_BP_LOB lob on pol.POL_LOB_ID = lob.BP_LOB_ID
--LEFT JOIN DIM_BP_ORG org on c.OWNR_OFC_ID = org.BRNCH_NUM
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
print(GoldFactTableNameComplete)

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