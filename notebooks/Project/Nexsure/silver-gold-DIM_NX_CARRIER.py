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
sourceSilverFolderPath = "Client/Nexsure/DimEntity/" +now.strftime("%Y") + "/" + now.strftime("%m")
sourceSilverPath = SilverContainerPath + sourceSilverFolderPath

sourceSilverFile = "DimEntity_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
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
if (GoldFactTableName == "" or sourceSilverPath == "" or sourceSilverFile == ""):
  now = datetime.now() 
  GoldDimTableName = "Dim_NX_Carrier"
  GoldFactTableName = "FCT_NX_INV_LINE_ITEM_TRANS"
  sourceSilverPath = "Client/Nexsure/DimEntity/" +now.strftime("%Y") + "/05"
  sourceSilverPath = SilverContainerPath + sourceSilverPath
  sourceSilverFile = "DimEntity_2021_05_25.parquet"
  sourceSilverFilePath = sourceSilverPath + "/" + sourceSilverFile
  badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"
  recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"
  BatchId = "1afc2b6c-d987-48cc-ae8c-a7f41ea27249"
  WorkFlowId ="8fc2895d-de32-4bf4-a531-82f0c6774221"
  sourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Client/Nexsure/DimEntity/2021/06/DimEntity_2021_06_04.parquet"

# COMMAND ----------

# MAGIC %scala
# MAGIC // Temporary cell to run manually - DELETE
# MAGIC if (GoldFactTableName == "") {
# MAGIC   lazy val GoldDimTableName = "Dim_NX_Carrier"
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
sourceSilverDF.createOrReplaceTempView("DIM_NX_CARRIER")

# COMMAND ----------

dummyDataDF = spark.sql(
f""" 
SELECT
 -99999 AS NX_CARIER_KEY
,-1 As NX_CARIER_ID
,-1 As CARIER_CLAS
,-1 As CARIER_NAME
,-1 As CARIER_TYP
,-1 As ADDR_LINE_1
,-1 As ADDR_LINE_2
,-1 As CITY
,-1 As STATE
,-1 As ZIP
,-1 As COUNTRY
,-1 As PRIM_CONTCT_NAME
,-1 As PRIM_CONTCT_EMAIL
,-1 As PRIM_CONTCT_PHN
,-1 As URL
,-1 As ACTV_FLG
,-1 As CARIER_BEGIN_DT
,-1 As NX_LST_MOD_DT
,-1 As NAIC_CMPNY_NUM
,-1 As AMB_NUM
,-1 As AMB_CMPNY_NAME
,-1 As AMB_PARNT_NUM
,-1 As AMB_PARNT_NAME
,-1 As AMB_ULTMT_PARNT_NUM
,-1 As AMB_ULTMT_PARNT_NAME
,-1 As DESCRIPTION
,-1 As STRT_DT
,-1 As END_DT
,-1 As DB_SRC_KEY
,-1 As SRC_AUDT_KEY
,'{ BatchId }' AS ETL_BATCH_ID
,'{ WorkFlowId }' AS ETL_WRKFLW_ID
,current_timestamp() AS ETL_CREATED_DT
,current_timestamp() AS ETL_UPDATED_DT
from DIM_NX_CARRIER e LIMIT 1
"""
)

# COMMAND ----------

# Get final set of records
finalDataDF = spark.sql(
f""" 
SELECT
 e.EntityKey As NX_CARIER_KEY
,e.EntityID As NX_CARIER_ID
,e.EntityClass As CARIER_CLAS
,e.EntityName As CARIER_NAME
,e.EntityType As CARIER_TYP
,e.EntityAddressLine1 As ADDR_LINE_1
,e.EntityAddressLine2 As ADDR_LINE_2
,e.EntityAddressCity As CITY
,e.EntityAddressState As STATE
,e.EntityAddressZip As ZIP
,e.EntityAddressCountry As COUNTRY
,e.PrimaryContactName As PRIM_CONTCT_NAME
,e.PrimaryContactEmail As PRIM_CONTCT_EMAIL
,e.PrimaryContactPhone As PRIM_CONTCT_PHN
,e.EntityURL As URL
,e.EntityActiveFlag As ACTV_FLG
,e.ClientBeginDate As CARIER_BEGIN_DT
,e.LastModified As NX_LST_MOD_DT
,-1 As NAIC_CMPNY_NUM -- Replace this with AMB column
,-1 As AMB_NUM -- Replace this with AMB column
,-1 As AMB_CMPNY_NAME -- Replace this with AMB column
,-1 As AMB_PARNT_NUM -- Replace this with AMB column
,-1 As AMB_PARNT_NAME -- Replace this with AMB column
,-1 As AMB_ULTMT_PARNT_NUM -- Replace this with AMB column
,-1 As AMB_ULTMT_PARNT_NAME -- Replace this with AMB column
,e.EntityDescription As DESCRIPTION
,e.RowStartDate As STRT_DT
,e.RowEndDate As END_DT
,e.DBSourceKey As DB_SRC_KEY
,e.AuditKey As SRC_AUDT_KEY
,'{ BatchId }' AS ETL_BATCH_ID
,'{ WorkFlowId }' AS ETL_WRKFLW_ID
,current_timestamp() AS ETL_CREATED_DT
,current_timestamp() AS ETL_UPDATED_DT
from DIM_NX_CARRIER e
where (e.EntityClass = 'Carrier' or e.EntityKey = -1)
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

# COMMAND ----------

