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
# MAGIC   GoldDimTableName = "MSTR_CLIENT_REF"
# MAGIC }

# COMMAND ----------

# Get the parameters from ADF
now = datetime.now() # current date and time

dbutils.widgets.text("TableName", "","")
GoldDimTableName = dbutils.widgets.get("TableName")

# Set the path for Silver layer for Client match
sourceSilverFilePath = SilverContainerPath + "Client/LocktonBookOfBusiness/MSTR_CLIENT_REF/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "LDP_360_ClientMatch_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"

dbutils.widgets.text("BatchId", "","")
BatchId = dbutils.widgets.get("BatchId")
dbutils.widgets.text("WorkFlowId", "","")
WorkFlowId = dbutils.widgets.get("WorkFlowId")

#Set the file path for logging 
badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"

date_time = now.strftime("%Y%m%dT%H%M%S")
badRecordsFilePath = badRecordsPath + date_time + "/" + "ErrorRecords"
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
  GoldDimTableName = "MSTR_CLIENT_REF"
  badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"
  recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"
  BatchId = "1afc2b6c-d987-48cc-ae8c-a7f41ea27249"
  WorkFlowId ="8fc2895d-de32-4bf4-a531-82f0c6774221"
  sourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Client/LocktonBookOfBusiness/MSTR_CLIENT_REF/" + yymmManual + "/LDP_360_ClientMatch_" + yyyymmddManual + ".parquet"

#  sourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Client/LocktonBookOfBusiness/DIM_CLIENT_MATCH/2021/07/02/Matching_Report.parquet"
  sourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Client/LocktonBookOfBusiness/MSTR_CLIENT_REF/2021/07/Client_Matching_Report_2021_07_05.parquet"

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
  #dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceSilverFilePath}}})  

# COMMAND ----------

# TEMPORARY
# from datetime import timedelta
# now = datetime.today() - timedelta(days = 1)
# sourceSilverFilePathCSV = SilverContainerPath + "Client/LocktonBookOfBusiness/DIM_CLIENT_MATCH/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "LDP_360_ClientMatch_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".csv"
# sourceSilverFilePath = SilverContainerPath + "Client/LocktonBookOfBusiness/DIM_CLIENT_MATCH/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "LDP_360_ClientMatch_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
# sourceSilverFilePath = SilverContainerPath + "Client/LocktonBookOfBusiness/DIM_CLIENT_MATCH/2021/07/02/Matching_Report.parquet"
# df2 = spark.read.option("header",True).csv(sourceSilverFilePathCSV)
# df2.write.mode("overwrite").parquet(sourceSilverFilePath)
# sourceSilverDF = spark.read.parquet(sourceSilverFilePath)
# display(df2)

# COMMAND ----------

# Register table so it is accessible via SQL Context
sourceSilverDF.createOrReplaceTempView("MSTR_CLIENT_REF")

# COMMAND ----------

pushdown_query = "(select BP_CLNT_ID,SURR_CLNT_ID from [dbo].[DIM_BP_CLIENT]) client"
clientDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
display(clientDF)
# Register table so it is accessible via SQL Context
clientDF.createOrReplaceTempView("DIM_BP_CLIENT")

# COMMAND ----------

pushdown_query = "(select NX_CLIENT_ID,SURR_CLIENT_ID from [dbo].[DIM_NX_CLIENT]) client"
clientDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
display(clientDF)
# Register table so it is accessible via SQL Context
clientDF.createOrReplaceTempView("DIM_NX_CLIENT")

# COMMAND ----------

# Get final set of records
finalDataDF = spark.sql(
f""" 
SELECT
Final_UPID AS MSTR_CLIENT_ID,
Source_Name AS CLIENT_SRC,
--CASE WHEN Source_Name = 'BenefitPoint' THEN CLIENT_ID ELSE EntityID END AS CLIENT_ID,
COALESCE(cn.SURR_CLIENT_ID,cb.SURR_CLNT_ID,0) AS CLIENT_ID,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId }' AS ETL_WRKFLW_ID,
current_timestamp() AS ETL_CREATED_DT,
current_timestamp() AS ETL_UPDATED_DT
FROM MSTR_CLIENT_REF r
LEFT JOIN DIM_NX_CLIENT cn on cn.NX_CLIENT_ID  = r.entityId
LEFT JOIN DIM_BP_CLIENT cb ON cb.BP_CLNT_ID = r.client_ID
"""
)
display(finalDataDF)
# finalDataDF .repartition(1).write.format("csv").option("header", "true").save("abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Client/LocktonBookOfBusiness/DIM_CLIENT_MATCH/2021/07/test.csv")

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
    (GoldDimTableName,now,sourceRecordCount,targetRecordCount,sourceSilverFilePath,BatchId,WorkFlowId)
  ],["TableName","ETL_CREATED_DT","SourceRecordCount","TargetRecordCount","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID"])

# Write the recon record to SQL DB
reconDF.write.jdbc(url=Url, table=reconTable, mode="append")

# COMMAND ----------

# MAGIC %scala
# MAGIC lazy val GoldDimTableNameComplete = finalTableSchema + "." + GoldDimTableName
# MAGIC // Truncate table and Delete data from Dimension table
# MAGIC lazy val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
# MAGIC lazy val stmt = connection.createStatement()
# MAGIC //lazy val sql = "exec " + finalTableSchema + ".[DropAndCreateFKContraints] @GoldTableName = '" + GoldDimTableName + "' , @ReseedTo = " + 1
# MAGIC lazy val sql = "truncate table " + GoldDimTableNameComplete;
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

