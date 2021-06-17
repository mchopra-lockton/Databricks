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
# MAGIC print (GoldDimTableName)

# COMMAND ----------

# Set the path for Silver layer for Nexsure

now = datetime.now() 
#sourceSilverPath = "Invoice/Nexsure/DimRateType/2021/06"
sourceSilverFolderPath = "Client/Nexsure/DimEntity/" +now.strftime("%Y") + "/" + now.strftime("%m")
#sourceSilverPath = "Client/Nexsure/DimEntity/" +now.strftime("%Y") + "/" + "06"
sourceSilverPath = SilverContainerPath + sourceSilverFolderPath

sourceSilverFile = "DimEntity_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
#sourceSilverFile = "DimEntity_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_25.parquet"
#sourceSilverFile = "DimEntity_" + now.strftime("%Y") + "_" + "06" + "_04.parquet"
sourceSilverFilePath = sourceSilverPath + "/" + sourceSilverFile

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
if (GoldDimTableName == "" or sourceSilverPath == "" or sourceSilverFile == ""):
  now = datetime.now() 
  GoldDimTableName = "DIM_NX_CLIENT"
  GoldFactTableName = "FCT_NX_INV_LINE_ITEM_TRANS"
  sourceSilverPath = "Client/Nexsure/DimEntity/" +now.strftime("%Y") + "/06"
  sourceSilverPath = SilverContainerPath + sourceSilverPath
  sourceSilverFile = "DimEntity_2021_06_04.parquet"
  sourceSilverFilePath = sourceSilverPath + "/" + sourceSilverFile
  badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"
  recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"
  BatchId = "1afc2b6c-d987-48cc-ae8c-a7f41ea27249"
  WorkFlowId ="8fc2895d-de32-4bf4-a531-82f0c6774221"
  sourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Client/Nexsure/DimEntity/2021/06/DimEntity_2021_06_04.parquet"

# COMMAND ----------

# MAGIC %scala
# MAGIC // Temporary cell to run manually - DELETE
# MAGIC if (GoldDimTableName == "" ) {
# MAGIC  lazy val GoldDimTableName = "Dim_NX_CLIENT"
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

sourceSilverDF.createOrReplaceTempView("DIM_NX_CLIENT")

# COMMAND ----------

dummyDataDF = spark.sql(
f""" 
SELECT
-99999 as NX_CLIENT_KEY
,-1 as MSTR_CLIENT_KEY
,-1 as NX_CLIENT_ID
,-1 As CLIENT_NAME
,-1 As CLIENT_TYP
,-1 As FEIN
,-1 As ADDR_LINE_1
,-1 As ADDR_LINE_2
,-1 As CITY
,-1 As STATE
,-1 As ZIP
,-1 As COUNTRY
,-1 As PRIM_NAICS_CD
,-1 As PRIM_NAICS_DESC
,-1 As PRIM_PNC_EFF_DT
,-1 As PNC_RELNSHIP
,-1 As PRIM_PNC_PRODCR
,-1 As PRIM_PNC_SERV_LEAD
,-1 As CLNT_SNCE_FRM
,-1 As CLNT_SNCE_TO
,-1 As PRIM_CONTCT_NAME
,-1 As PRIM_CONTCT_EMAIL
,-1 As PRIM_CONTCT_PHN
,-1 As NX_LST_MOD_DT
,-1 As URL
,-1 As ACTV_FLG
,-1 As CARIER_CLAS
,-1 As DB_SRC_KEY
,-1 As SRC_AUDT_KEY
,'{ BatchId }' AS ETL_BATCH_ID
,'{ WorkFlowId }' AS ETL_WRKFLW_ID
,current_timestamp() AS ETL_CREATED_DT
,current_timestamp() AS ETL_UPDATED_DT
FROM DIM_NX_CLIENT e LIMIT 1
"""
)
display(dummyDataDF)

# COMMAND ----------

# Get final set of records
finalDataDF = spark.sql(
f""" 
SELECT
EntityKey as NX_CLIENT_KEY,
1 As MSTR_CLIENT_KEY,
EntityID  as NX_CLIENT_ID,
EntityName as CLIENT_NAME,
EntityType as CLIENT_TYP,
FEIN as FEIN,
EntityAddressLine1 as ADDR_LINE_1,
EntityAddressLine2 as ADDR_LINE_2,
EntityAddressCity as CITY,
EntityAddressState as STATE,
EntityAddressZip as ZIP,
EntityAddressCountry as COUNTRY,
PrimaryNaicsSic As PRIM_NAICS_CD,
'None' As PRIM_NAICS_DESC,
'1800-01-01' As PRIM_PNC_EFF_DT,
-1 As PNC_RELNSHIP,
-1 As PRIM_PNC_PRODCR,
-1 As PRIM_PNC_SERV_LEAD,
-1 As CLNT_SNCE_FRM,
-1 As CLNT_SNCE_TO,
PrimaryContactName as PRIM_CONTCT_NAME,
PrimaryContactEmail as PRIM_CONTCT_EMAIL,
PrimaryContactPhone as PRIM_CONTCT_PHN,
LastModified as NX_LST_MOD_DT,
EntityURL as URL,
EntityActiveFlag as ACTV_FLG,
EntityClass as CARIER_CLAS,
DBSourceKey as DB_SRC_KEY,
AuditKey as SRC_AUDT_KEY,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId }' AS ETL_WRKFLW_ID,
current_timestamp() AS ETL_CREATED_DT,
current_timestamp() AS ETL_UPDATED_DT
FROM DIM_NX_CLIENT
WHERE (EntityClass ='Client' or EntityKey = -1)
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