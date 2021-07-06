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
# MAGIC var GoldDimTableName = dbutils.widgets.get("TableName")
# MAGIC 
# MAGIC // USE WHEN RUN IN DEBUG MODE
# MAGIC if (RunInDebugMode != "No") {
# MAGIC   GoldDimTableName = "DIM_NX_CLIENT_CONTACT"
# MAGIC }

# COMMAND ----------

# Get the parameters from ADF
now = datetime.now() # current date and time

dbutils.widgets.text("TableName", "","")
GoldDimTableName = dbutils.widgets.get("TableName")

# Set the path for Silver layer for Nexsure
sourceSilverFolderPath = "Client/Nexsure/DimContact/" +now.strftime("%Y") + "/" + now.strftime("%m")
sourceSilverPath = SilverContainerPath + sourceSilverFolderPath

sourceSilverFile = "DimContact_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
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

# USE WHEN RUN IN DEBUG MODE
if (RunInDebugMode != 'No'):
  now = datetime.now() 
  GoldDimTableName = "DIM_NX_CLIENT_CONTACT"
  GoldFactTableName = "FCT_NX_INV_LINE_ITEM_TRANS"
  badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"
  recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"
  BatchId = "1afc2b6c-d987-48cc-ae8c-a7f41ea27249"
  WorkFlowId ="8fc2895d-de32-4bf4-a531-82f0c6774221"
  sourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Client/Nexsure/DimContact/" + yymmManual + "/DimContact_" + yyyymmddManual + ".parquet"
  print(sourceSilverFilePath)


# COMMAND ----------

# Read source file
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
try:
   sourceClientDF = spark.read.parquet(sourceSilverFilePath)
   #creating dataframe for client only from entity
   sourceClientOnlyDF=sourceClientDF.filter((sourceClientDF.EntityClass=='Client'))  
except:
    # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceSilverClientFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  #dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceSilverFilePath}}})  

# COMMAND ----------

sourceClientOnlyDF.count()

# COMMAND ----------

# Register table so it is accessible via SQL Context
sourceClientOnlyDF.createOrReplaceTempView("DIM_NX_CLIENT_CONTACT")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_NX_CLIENT]) client"
clientDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
display(clientDF)
# Register table so it is accessible via SQL Context
clientDF.createOrReplaceTempView("DIM_NX_CLIENT")

# COMMAND ----------

dummyDataDF = spark.sql(
f""" 
SELECT
-99999 As NX_CONTACT_KEY
,-1 As NX_CONTACT_ID
,0 As SURR_CLIENT_ID
,-1 As CLASS
,-1 As PRIM_IND
,-1 As FIRST_NAME
,-1 As LAST_NAME
,-1 As FULL_NAME
,-1 As EMAIL
,-1 As TITLE
,-1 As CONTACT_ROLE
,-1 As MARITAL_STAT
,-1 As GENDER
,-1 As EMP_IND
,-1 As DB_SRC_KEY
,-1 As SRC_AUDT_KEY
,'{ BatchId }' AS ETL_BATCH_ID
,'{ WorkFlowId }' AS ETL_WRKFLW_ID
,current_timestamp() AS ETL_CREATED_DT
,current_timestamp() AS ETL_UPDATED_DT
from DIM_NX_CLIENT_CONTACT e LIMIT 1
"""
)

# COMMAND ----------

# Get final set of records
finalDataDF = spark.sql(
f""" 
SELECT
c.ContactKey As NX_CONTACT_KEY
,c.ContactID As NX_CONTACT_ID
,coalesce (cl.SURR_CLIENT_ID,0) As SURR_CLIENT_ID
,c.EntityClass As CLASS
,c.PrimaryInd As PRIM_IND
,c.FirstLastName As FIRST_NAME
,c.LastFirstName As LAST_NAME
,c.FullName As FULL_NAME
,c.Title As TITLE
,c.Role As CONTACT_ROLE
,c.MaritalStatus As MARITAL_STAT
,c.Sex As GENDER
,c.Email1 As EMAIL
,c.EmployeeInd As EMP_IND
,c.DBSourceKey As DB_SRC_KEY
,c.AuditKey As SRC_AUDT_KEY
,'{ BatchId }' AS ETL_BATCH_ID
,'{ WorkFlowId }' AS ETL_WRKFLW_ID
,current_timestamp() AS ETL_CREATED_DT
,current_timestamp() AS ETL_UPDATED_DT
from DIM_NX_CLIENT_CONTACT c
LEFT JOIN DIM_NX_CLIENT cl on c.EntityKey = cl.NX_CLIENT_KEY
"""
)

# COMMAND ----------

finalDataDF.count()

# COMMAND ----------

# Do not proceed if there are no records to insert
if (finalDataDF.count() == 0):
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "There are no records to insert: " + sourceSilverFilePath}}})

# COMMAND ----------

# Create a dataframe for record count
sourceRecordCount = sourceClientOnlyDF.count()
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