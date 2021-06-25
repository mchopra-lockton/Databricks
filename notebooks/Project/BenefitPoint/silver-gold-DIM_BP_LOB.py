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

now= datetime.now()   

sourceSilverFilePath = SilverContainerPath + "Policy/Benefits/vw_PLAN_TYPE_AllTypes/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "vw_PLAN_TYPE_AllTypes_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"

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
GoldDimTableName = "DIM_BP_LOB"
badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"
recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"
BatchId = "1afc2b6c-d987-48cc-ae8c-a7f41ea27249"
WorkFlowId ="8fc2895d-de32-4bf4-a531-82f0c6774221"
sourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Policy/Benefits/vw_PLAN_TYPE_AllTypes/" + yymmManual + "/vw_PLAN_TYPE_AllTypes_" + yyyymmddManual + ".parquet"

# COMMAND ----------

# MAGIC %scala
# MAGIC // Temporary cell - DELETE
# MAGIC lazy val GoldDimTableName = "DIM_BP_LOB"

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
    (GoldFactTableName,now,sourceSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceSilverFilePath}}})

# COMMAND ----------

# Register table so it is accessible via SQL Context
sourceSilverDF.createOrReplaceTempView("DIM_BP_LOB")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[BP_NX_REF_MASTER_LOB_MAPPING]) lobmapping"
clientDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(clientDF)
# Register table so it is accessible via SQL Context
clientDF.createOrReplaceTempView("BP_NX_REF_MASTER_LOB_MAPPING")

# COMMAND ----------

dummyDataDF = spark.sql(
  f"""
SELECT
-99999 BP_LOB_ID,
'None' LOB,
'None' LOB_SHRT_DESC,
'None' MSTR_LOB,
'None' MSTR_DEPT,
'None' LOC_DESC,
-99999 SORT_ORDR,
'0' ALLOW_EE_INCLUDE_IND,
'0' ALLOW_TIERED_RATE_IND,
'0' RATE_COMPARABLE_IND,
'0' ADHOC_PRODUCT_IND,
'None' SRC_AUD_INSRT_KEY,
'None' SRC_AUD_UPD_KEY,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId}' AS ETL_WRKFLW_ID,
CURRENT_TIMESTAMP() as ETL_CREATED_DT,
CURRENT_TIMESTAMP() as ETL_UPDATED_DT
FROM DIM_BP_LOB LIMIT 1
""")

# COMMAND ----------

finalDataDF = spark.sql(
f"""
SELECT
PLAN_TYPE_ID As BP_LOB_ID,
-1 As LOB,
PLAN_TYPE_SHRT_DESC	As LOB_SHRT_DESC,
MASTER_LINE_OF_BUSINESS As MSTR_LOB,
-1 As MSTR_DEPT,
LOC_DESC As LOC_DESC,
SORT_ORDER As SORT_ORDR,
ALLOW_EE_INCLUDE_IND As	ALLOW_EE_INCLUDE_IND,
ALLOW_TIERED_RATE_IND As ALLOW_TIERED_RATE_IND,
RATE_COMPARABLE_IND	As RATE_COMPARABLE_IND,
ADHOC_PRODUCT_IND As ADHOC_PRODUCT_IND,
-1 As SRC_AUD_INSRT_KEY,
-1 As SRC_AUD_UPD_KEY,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId}' AS ETL_WRKFLW_ID,
CURRENT_TIMESTAMP() as ETL_CREATED_DT,
CURRENT_TIMESTAMP() as ETL_UPDATED_DT
from DIM_BP_LOB lob
left join BP_NX_REF_MASTER_LOB_MAPPING mc on lob.PLAN_TYPE_DESC = mc.LINE_OF_BUSINESS_GRP
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