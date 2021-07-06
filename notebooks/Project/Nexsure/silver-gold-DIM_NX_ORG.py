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
# MAGIC var GoldDimTableName = dbutils.widgets.get("TableName")
# MAGIC 
# MAGIC // USE WHEN RUN IN DEBUG MODE
# MAGIC if (RunInDebugMode != "No") {
# MAGIC   GoldDimTableName = "Dim_NX_ORG"
# MAGIC }

# COMMAND ----------

# Set the path for Silver layer for Nexsu

now = datetime.now() 
#sourceSilverPath = "OrgStructure/Nexsure/DimOrgStructure/2021/05"
sourceSilverFolderPath = "OrgStructure/Nexsure/DimOrgStructure/" +now.strftime("%Y") + "/" + now.strftime("%m")
sourceSilverPath = SilverContainerPath + sourceSilverFolderPath

sourceSilverFile = "DimOrgStructure_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
#sourceSilverFile = "DimOrgStructure_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_21.parquet"
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
#badRecordsPath = "abfss://c360logs@dlsldpdev01v8nkg988.dfs.core.windows.net/Dim_NX_ORG/"
#badRecordsFilePath = "abfss://c360logs@dlsldpdev01v8nkg988.dfs.core.windows.net/Dim_NX_ORG/" + date_time
recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"

#Set the file path to log error
#badRecordsPath = badRecordsRootPath + "/" + sourceTable + "/"

print ("Param -\'Variables':")
print (sourceSilverFilePath)
print (badRecordsFilePath)
print (recordCountFilePath)

# COMMAND ----------

# USE WHEN RUN IN DEBUG MODE
if (RunInDebugMode != 'No'):
  now = datetime.now() 
  GoldDimTableName = "DIM_NX_ORG"
  badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"
  recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"
  BatchId = "1afc2b6c-d987-48cc-ae8c-a7f41ea27249"
  WorkFlowId ="8fc2895d-de32-4bf4-a531-82f0c6774221"
  sourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/OrgStructure/Nexsure/DimOrgStructure/" + yymmManual + "/DimOrgStructure_" + yyyymmddManual + ".parquet"

# COMMAND ----------

 # Read source file
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
try:
 
   sourceSilverDF = spark.read.parquet(sourceSilverFilePath)
   display(sourceSilverDF)
except:
  # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  #dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceSilverFilePath}}})  

# COMMAND ----------

# Register table so it is accessible via SQL Context
sourceSilverDF.createOrReplaceTempView("DIM_NX_ORG")
#display(sourceSilverDF)

# COMMAND ----------

# Create a dataframe of Org Structure Mapping table
pushdown_query = "(select OFFICE_BRANCHID,SERIES,BUSINESS_UNIT,BUSINESS_TYPE,REPRTNG_OFC_NAME,BUSS_TYP_GRP from [dbo].[BP_NX_REF_ORG_STR_MAPPING]) orgstrmap"
orgstrmapDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(clientDF)
# Register table so it is accessible via SQL Context
orgstrmapDF.createOrReplaceTempView("BP_NX_REF_ORG_STR_MAPPING")

# COMMAND ----------

# Create a dataframe of Master Dept (Mapping BenefitsDept) table
# pushdown_query = "(select * from [dbo].[BP_NX_REF_PS_DEPT_MAPPING]) psdepmap"
pushdown_query = "(SELECT DEPT_ID,BUSINESS_UNIT,DEPT_ALIAS,DEPT_MASTER_ALIAS FROM (SELECT *, row_number() over(PARTITION BY  DEPT_ID,BUSINESS_UNIT ORDER BY Eff_Dt desc) dept_rank FROM [dbo].[BP_NX_REF_PS_DEPT_MAPPING]) AS DeptMap WHERE dept_rank=1) psdepmap"
psdeptmapDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(clientDF)
# Register table so it is accessible via SQL Context
psdeptmapDF.createOrReplaceTempView("BP_NX_REF_PS_DEPT_MAPPING")

# COMMAND ----------

dummyDataDF = spark.sql(
f""" 
SELECT
-99999 AS NX_ORG_KEY,
-1 As DEPT_NO,
-1 As BRNCH_NO,
-1 As ORG_NO,
-1 As DEPT_NAME,
-1 As BRNCH_NAME,
-1 As REGION_NAME,
-1 As ORG_NAME,
-1 As ENTY_LVL_NAME,
-1 As SERIES,
-1 As BUSS_UNIT,
-1 As BUSS_TYP,
-1 As REGIONL_OFC_NAME,
-1 As BUSS_TYP_GRP,
-1 As DEPT_ALIAS,
-1 As DEPT_MSTR_ALIAS,
-1 as DB_SRC_KEY,
-1 as SRC_AUDT_KEY,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId }' AS ETL_WRKFLW_ID,
current_timestamp() AS ETL_CREATED_DT,
current_timestamp() AS ETL_UPDATED_DT
FROM DIM_NX_ORG e LIMIT 1
"""
)
#display(dummyDataDF)

# COMMAND ----------

# Get final set of records
finalDataDF = spark.sql(
f""" 
SELECT
OrgStructureKey AS NX_ORG_KEY,
DepartmentNum As DEPT_NO,
DepartmentName As DEPT_NAME,
BranchNum As BRNCH_NO,
BranchName As BRNCH_NAME,
RegionName As REGION_NAME,
OrgNumber As ORG_NO,
OrgName	As ORG_NAME,
EntityLevelName	As ENTY_LVL_NAME,
coalesce(orgstrmap.SERIES,0) As SERIES, 
coalesce(orgstrmap.BUSINESS_UNIT,0) As BUSS_UNIT,
coalesce(orgstrmap.BUSINESS_TYPE,0) As BUSS_TYP, 
coalesce(orgstrmap.REPRTNG_OFC_NAME,0) As REGIONL_OFC_NAME,
coalesce(orgstrmap.BUSS_TYP_GRP,0) As BUSS_TYP_GRP,
coalesce(psdepmap.DEPT_ALIAS,0) DEPT_ALIAS,
coalesce(psdepmap.DEPT_MASTER_ALIAS,0) As DEPT_MSTR_ALIAS,
DBSourceKey As DB_SRC_KEY,
AuditKey As SRC_AUDT_KEY,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId }' AS ETL_WRKFLW_ID,
current_timestamp() AS ETL_CREATED_DT,
current_timestamp() AS ETL_UPDATED_DT
FROM DIM_NX_ORG e
LEFT JOIN BP_NX_REF_ORG_STR_MAPPING orgstrmap ON e.BranchNum = orgstrmap.OFFICE_BRANCHID
LEFT JOIN BP_NX_REF_PS_DEPT_MAPPING psdepmap ON SUBSTRING(e.DepartmentName,1,4) = psdepmap.DEPT_ID AND orgstrmap.BUSINESS_UNIT = psdepmap.BUSINESS_UNIT
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