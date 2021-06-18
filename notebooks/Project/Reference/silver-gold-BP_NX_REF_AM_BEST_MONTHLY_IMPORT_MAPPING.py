# Databricks notebook source
from datetime import datetime

# COMMAND ----------

# MAGIC %run "/Project/Database Config"

# COMMAND ----------

# Setup a connection to ADLS 
spark.conf.set(
  ADLSConnectionURI,
  ADLSConnectionKey
)

# COMMAND ----------

# Cleanup the widgets
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC dbutils.widgets.text("TableName", "","")
# MAGIC lazy val GoldDimTableName = dbutils.widgets.get("TableName")

# COMMAND ----------

# Get the parameters from ADF
now = datetime.now() # current date and time

dbutils.widgets.text("TableName", "","")
GoldDimTableName = dbutils.widgets.get("TableName")

# Set the path for Silver layer for Nexsure
sourceSilverFolderPath = "Carrier/MDS2/AMBestMonthlyImport/" +now.strftime("%Y") + "/" + now.strftime("%m")
sourceSilverPath = SilverContainerPath + sourceSilverFolderPath

sourceSilverFile = "AMBestMonthlyImport_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
sourceSilverFilePath = sourceSilverPath + "/" + sourceSilverFile

dbutils.widgets.text("BatchId", "","")
BatchId = dbutils.widgets.get("BatchId")
dbutils.widgets.text("WorkFlowId", "","")
WorkFlowId = dbutils.widgets.get("WorkFlowId")

#Set the file path for logging 
badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"

date_time = now.strftime("%Y%m%dT%H%M%S")
badRecordsFilePath = badRecordsPath + date_time + "/" + "ErrorRecords"
#badRecordsPath = "abfss://c360logs@dlsldpdev01v8nkg988.dfs.core.windows.net/Dim_NX_Carrier/"
#badRecordsFilePath = "abfss://c360logs@dlsldpdev01v8nkg988.dfs.core.windows.net/Dim_NX_Carrier/" + date_time
recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"

print ("Param -\'Variables':")
print (sourceSilverFilePath)
#print (SelectQuery)
print (badRecordsPath)
print (badRecordsFilePath)
print (recordCountFilePath)

# COMMAND ----------

# Temporary cell to run manually - DELETE
if (GoldDimTableName == "" or sourceSilverPath == "" or sourceSilverFile == ""):
  now = datetime.now() 
  GoldDimTableName = "BP_NX_REF_AM_BEST_MONTHLY_IMPORT_MAPPING"
  sourceSilverPath = "Carrier/MDS2/AMBestMonthlyImport/" +now.strftime("%Y") + "/05"
  sourceSilverPath = SilverContainerPath + sourceSilverPath
  sourceSilverFile = "AMBestMonthlyImport_2021_06_15.parquet"
  sourceSilverFilePath = sourceSilverPath + "/" + sourceSilverFile
  badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"
  recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"
  BatchId = "1afc2b6c-d987-48cc-ae8c-a7f41ea27249"
  WorkFlowId ="8fc2895d-de32-4bf4-a531-82f0c6774221"
sourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Carrier/MDS2/AMBestMonthlyImport/2021/06/AMBestMonthlyImport_2021_06_15.parquet"

# COMMAND ----------

# MAGIC %scala
# MAGIC // Temporary cell to run manually - DELETE
# MAGIC if (GoldDimTableName == "") {
# MAGIC   lazy val GoldDimTableName = "BP_NX_REF_AM_BEST_MONTHLY_IMPORT_MAPPING"
# MAGIC }  

# COMMAND ----------

 # Do not proceed if any of the parameters are missing
if (GoldDimTableName == "" or sourceSilverPath == "" or sourceSilverFile == ""):
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Input parameters are missing"}}})

# COMMAND ----------

# Read source file
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")

try:
  sourceSilverDF = spark.read.parquet(sourceSilverFilePath)
#  display(sourceSilverDF)
#  sourceSilverDF.printSchema
except:
  # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceSilverFilePath}}})  

# COMMAND ----------

# Register table so it is accessible via SQL Context
sourceSilverDF.createOrReplaceTempView("BP_NX_REF_AM_BEST_MONTHLY_IMPORT_MAPPING")

# COMMAND ----------

# Get final set of records
finalDataDF = spark.sql(
f""" 
SELECT
e.AMB_NUMBER As AMB_NUM 
,e.NAIC_COMPANY_NUMBER As NAIC_CMPNY_NUM 
,e.BESTS_FINANCIAL_STRENGTH_RATING_ALPHA As BST_FIN_STRNGTH_RTNG_ALPH 
,e.BESTS_FINANCIAL_STRENGTH_RATING_MODIFIERS As BST_FIN_STRNGTH_RTNG_MODFRS 
,e.BESTS_FINANCIAL_STRENGTH_RATING_AFFILIATION_CODE As BST_FIN_STRNGTH_RTNG_AFFLATION_CD 
,e.BESTS_FINANCIAL_STRENGTH_NUMERIC As BST_FIN_STRNGTH_NUM  
,e.FINANCIAL_SIZE_ALPHA As FIN_SIZE_ALPH  
,e.FINANCIAL_SIZE_NUMERIC As FIN_SIZE_NUM  
,e.BESTS_FINANCIAL_STRENGTH_RATING_EFFECTIVE_DATE As BST_FIN_STRNGTH_RTNG_EFF_DT   
,e.BUSINESS_COMPOSITE_CODE As BUSINES_CMPSITE_CD    
,e.RESPONSIBLE_RATING_DIVISION As RESP_RTNG_DIV  
,e.STATE_PROVINCE_OF_DOMICILE As ST_PROV_OF_DOMCLE  
,e.AMB_COMPANY_NAME As AMB_CMPNY_NAME  
,e.AMB_ULTIMATE_PARENT_NUMBER As AMB_ULTMTE_PARNT_NUM  
,e.AMB_ULTIMATE_PARENT_NAME As AMB_ULTMTE_PARNT_NAME  
,e.COUNTRY_OF_DOMICILE As CNTRY_OF_DOMCLE  
,e.AMB_RATING_UNIT_COMPANY_NUMBER As AMB_RTNG_UNIT_CMPNY_NUM  
,e.AMB_RATING_UNIT_COMPANY_NAME As AMB_RTNG_UNIT_CMPNY_NAME  
,e.BESTS_FINANCIAL_STRENGTH_RATING_OUTLOOK As BST_FIN_STRNGTH_RTNG_OUTLK  
,e.BESTS_FINANCIAL_STRENGTH_RATING_IMPLICATION As BST_FIN_STRNGTH_RTING_IMPLCTN  
,e.BESTS_FINANCIAL_STRENGTH_RATING_ACTION As BST_FIN_STRNGTH_RTING_ACTION  
,e.AMB_RATING_UNIT_INDICATOR As AMB_RTING_UNIT_INDICTR  
,e.FEIN_NUMBER As FEIN_NUM   
,e.AMB_FINANCIAL_GROUP_NUMBER As AMB_FIN_GRP_NUM 
,e.AMB_FINANCIAL_GROUP_NAME As AMB_FIN_GRP_NAME  
,e.LEGAL_ENTITY_IDENTIFIER As LEGAL_ENTITY_IDNTFIER 
,e.PARENT_NUMBER As PARNT_NUM 
,e.PARENT_NAME As PARNT_NAME 
,'{ BatchId }' AS ETL_BATCH_ID
,'{ WorkFlowId }' AS ETL_WRKFLW_ID
,current_timestamp() AS ETL_CREATED_DT
,current_timestamp() AS ETL_UPDATED_DT
from BP_NX_REF_AM_BEST_MONTHLY_IMPORT_MAPPING e
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
    (GoldDimTableName,now,sourceRecordCount,targetRecordCount,sourceSilverFilePath,BatchId,WorkFlowId)
  ],["TableName","ETL_CREATED_DT","SourceRecordCount","TargetRecordCount","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID"])

# Write the recon record to SQL DB
reconDF.write.jdbc(url=Url, table=reconTable, mode="append")

# COMMAND ----------

# MAGIC %scala
# MAGIC // Truncate table and Delete data from Dimension table
# MAGIC lazy val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
# MAGIC lazy val stmt = connection.createStatement()
# MAGIC lazy val sql = "exec " + finalTableSchema + ".[DropAndCreateFKContraints] @GoldTableName = '" + GoldDimTableName + "' , @ReseedTo = " + 1
# MAGIC stmt.execute(sql)
# MAGIC connection.close()

# COMMAND ----------

GoldDimTableNameComplete = finalTableSchema + "." + GoldDimTableName
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

