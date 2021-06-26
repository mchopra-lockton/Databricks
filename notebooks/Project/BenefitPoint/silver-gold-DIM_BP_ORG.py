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

#Broker Office file
sourceSilverFilePath = SilverContainerPath + "OrgStructure/Benefits/vw_BROKER_OFFICE_AllRecs/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "vw_BROKER_OFFICE_AllRecs_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"

#Broker Region file
regionSourceSilverFilePath = SilverContainerPath + "OrgStructure/Benefits/vw_BROKER_REGION_AllRecs/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "vw_BROKER_REGION_AllRecs_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"

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
print(regionSourceSilverFilePath)

# COMMAND ----------

# Temporary cell - DELETE
now = datetime.now() 
GoldDimTableName = "DIM_BP_ORG"
badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"
recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"
BatchId = "1afc2b6c-d987-48cc-ae8c-a7f41ea27249"
WorkFlowId ="8fc2895d-de32-4bf4-a531-82f0c6774221"
sourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/OrgStructure/Benefits/vw_BROKER_OFFICE_AllRecs/" + yymmManual + "/vw_BROKER_OFFICE_AllRecs_" + yyyymmddManual + ".parquet"
regionSourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/OrgStructure/Benefits/vw_BROKER_REGION_AllRecs/" + yymmManual + "/vw_BROKER_REGION_AllRecs_" + yyyymmddManual + ".parquet"
print(sourceSilverFilePath)
print(regionSourceSilverFilePath)

# COMMAND ----------

# MAGIC %scala
# MAGIC // Temporary cell - DELETE
# MAGIC  val GoldDimTableName = "DIM_BP_ORG"

# COMMAND ----------

# Do not proceed if any of the parameters are missing
if (GoldDimTableName == "" or sourceSilverFilePath == "" or regionSourceSilverFilePath == ""):
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Input parameters are missing"}}})

# COMMAND ----------

# Read  office source file
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

# Read region source file
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
try:
 
  regionSourceSilverDF = spark.read.parquet(regionSourceSilverFilePath)
  #display(regionSourceSilverDF)
except:
  # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,regionSourceSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + regionSourceSilverFilePath}}}) 

# COMMAND ----------

sourceSilverDF.createOrReplaceTempView("DIM_BP_OFFICE")
regionSourceSilverDF.createOrReplaceTempView("DIM_BP_REGION")

# COMMAND ----------

# Create a dataframe of Org Structure Mapping table
pushdown_query = "(select OFFICE_BRANCHID,SERIES,BUSINESS_UNIT,BUSINESS_TYPE,REPRTNG_OFC_NAME,BUSS_TYP_GRP from [dbo].[BP_NX_REF_ORG_STR_MAPPING]) orgstrmap"
orgstrmapDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(clientDF)
# Register table so it is accessible via SQL Context
orgstrmapDF.createOrReplaceTempView("BP_NX_REF_ORG_STR_MAPPING")

# COMMAND ----------

dummyDataDF = spark.sql(
f""" 
SELECT
-99999 as BP_ORG_ID,
---99999 as BRNCH_IDNTITY,
-99999 as BRNCH_NUM,
-99999  as BRNCH_NAME,
-1 as BRNCH_CONTCT,
-1 as ADDR_LINE_1,
-1 as ADDR_LINE_2,
-1 as CITY,
-1 as STATE,
-1 as ZIP,
-1 as AREA_CD,
-1 as PHONE_NUM,
-1 as REGIONL_OFC_NAME,
-1 as SERIES_ID,
-1 as BUSS_UNIT,
-1 as BUSS_TYP,
-1 as BUSS_TYP_GRP,
'1800-01-01' as SRC_ROW_BEGN_DT,
'1800-01-01' as SRC_ROW_END_DT,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId }' AS ETL_WRKFLW_ID,
current_timestamp() AS ETL_CREATED_DT,
current_timestamp() AS ETL_UPDATED_DT
FROM DIM_BP_OFFICE o limit 1
"""
)
display(dummyDataDF)

# COMMAND ----------

# Get final set of records
finalDataDF = spark.sql(
f""" 
SELECT
-99999 as BP_ORG_ID,
--BROKER_OFFICE_IDENTITY as BRNCH_IDNTITY,
OFFICE_ID as BRNCH_NUM,
OFFICE_NAME as BRNCH_NAME,
OFFICE_CONTACT as BRNCH_CONTCT,
STREET1 as ADDR_LINE_1,
STREET2 as ADDR_LINE_2,
CITY as CITY,
STATE as STATE,
POSTAL_CODE as ZIP,
AREA_CODE as AREA_CD,
PHONE_NUMBER as PHONE_NUM,
coalesce(REPRTNG_OFC_NAME,0) as REGIONL_OFC_NAME,
coalesce(SERIES,0) as SERIES_ID,
coalesce(BUSINESS_UNIT,0) as BUSS_UNIT,
coalesce(BUSINESS_TYPE,0) as BUSS_TYP,
coalesce(BUSS_TYP_GRP,0) as BUSS_TYP_GRP,
o.RowBeginDate as SRC_ROW_BEGN_DT,
o.RowEndDate as SRC_ROW_END_DT,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId }' AS ETL_WRKFLW_ID,
current_timestamp() AS ETL_CREATED_DT,
current_timestamp() AS ETL_UPDATED_DT
FROM DIM_BP_OFFICE o 
--JOIN DIM_BP_REGION r on o.REGION_ID = r.REGION_ID
LEFT JOIN BP_NX_REF_ORG_STR_MAPPING orgstrmap ON o.OFFICE_ID = orgstrmap.OFFICE_BRANCHID
"""
)
#display(finalDataDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select SERIES from BP_NX_REF_ORG_STR_MAPPING

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