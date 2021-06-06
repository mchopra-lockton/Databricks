# Databricks notebook source
from datetime import datetime

# COMMAND ----------

# MAGIC %run "/Shared/Database Config"

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

# COMMAND ----------

# Set the path for Silver layer for Nexsu

now = datetime.now() 
#sourceSilverPath = "Person/Nexsure/DimEmployee/2021/05"
sourceSilverFolderPath = "Person/Nexsure/DimEmployee/" +now.strftime("%Y") + "/" + now.strftime("%m")
sourceSilverPath = SilverContainerPath + sourceSilverFolderPath

sourceSilverFile = "DimEmployee_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
#sourceSilverFile = "DimEmployee_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_21.parquet"
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
#badRecordsPath = "abfss://c360logs@dlsldpdev01v8nkg988.dfs.core.windows.net/Dim_NX_EMP/"
#badRecordsFilePath = "abfss://c360logs@dlsldpdev01v8nkg988.dfs.core.windows.net/Dim_NX_EMP/" + date_time
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
# now = datetime.now() 
# GoldDimTableName = "Dim_NX_Emp"
# GoldFactTableName = "FCT_NX_INV_LINE_ITEM_TRANS"
# sourceSilverPath = "Person/Nexsure/DimEmployee/" +now.strftime("%Y") + "/05"
# sourceSilverPath = SilverContainerPath + sourceSilverPath
# sourceSilverFile = "DimEmployee_2021_05_21.parquet"
# sourceSilverFilePath = sourceSilverPath + "/" + sourceSilverFile
# badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"
# recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"
# BatchId = "1afc2b6c-d987-48cc-ae8c-a7f41ea27249"
# WorkFlowId ="8fc2895d-de32-4bf4-a531-82f0c6774221"
sourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Person/Nexsure/DimEmployee/2021/05/DimEmployee_2021_05_21.parquet"

# COMMAND ----------

# MAGIC %scala
# MAGIC // Temporary cell - DELETE
# MAGIC // lazy val GoldDimTableName = "Dim_NX_Emp"

# COMMAND ----------

 # Do not proceed if any of the parameters are missing
if (GoldDimTableName == "" or sourceSilverPath == "" or sourceSilverFile == ""):
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Input parameters are missing"}}})

# COMMAND ----------

sourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Person/Nexsure/DimEmployee/2021/05/DimEmployee_2021_05_21.parquet" 
# Read source file
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
try:
 
   sourceSilverDF = spark.read.option("badRecordsPath", badRecordsPath).schema("EmployeeKey INT,EmployeeFullName string,EmployeeSSN string,DriversLicenseNumber string,DBSourceKey int,AuditKey INT").parquet(sourceSilverFilePath)
   #display(sourceSilverDF)
except:
  print("Schema mismatch")
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Schema mismatch: " + sourceSilverFilePath}}})

# COMMAND ----------

# Register table so it is accessible via SQL Context
sourceSilverDF.createOrReplaceTempView("DIM_NX_EMP")

# COMMAND ----------

dummyDataDF = spark.sql(
  f""" 
SELECT
-99999	As EMP_KEY,
-1 As FULL_NAME,
-1 As SSN,
-1 As DRVR_LCNSE_NO,
-1 As DB_SRC_KEY,
-1 As SRC_AUDT_KEY,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId }' AS ETL_WRKFLW_ID,
current_timestamp() AS ETL_CREATED_DT,
current_timestamp() AS ETL_UPDATED_DT
FROM DIM_NX_EMP LIMIT 1
"""
)

# COMMAND ----------

# Get final set of records
finalDataDF = spark.sql(
f""" 
SELECT
EmployeeKey	As EMP_KEY,
EmployeeFullName As FULL_NAME,
EmployeeSSN As SSN,
DriversLicenseNumber As DRVR_LCNSE_NO,
DBSourceKey As DB_SRC_KEY,
AuditKey As SRC_AUDT_KEY,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId }' AS ETL_WRKFLW_ID,
current_timestamp() AS ETL_CREATED_DT,
current_timestamp() AS ETL_UPDATED_DT
FROM DIM_NX_EMP
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
# MAGIC lazy val GoldFactTableName = "Gold.FCT_NX_INV_LINE_ITEM_TRANS"
# MAGIC lazy val sql_truncate = "truncate table " + GoldFactTableName
# MAGIC stmt.execute(sql_truncate)
# MAGIC lazy val sql = "exec [Admin].[DropAndCreateFKContraints] @GoldTableName = '" + GoldDimTableName + "'"
# MAGIC stmt.execute(sql)
# MAGIC connection.close()

# COMMAND ----------

GoldDimTableNameComplete = "gold." + GoldDimTableName
dummyDataDF.write.jdbc(url=Url, table=GoldDimTableNameComplete, mode="append")
finalDataDF.write.jdbc(url=Url, table=GoldDimTableNameComplete, mode="append")

# COMMAND ----------

# Write the final parquet file to Gold zone
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInWrite=CORRECTED")
sourceGoldFilePath = GoldContainerPath + sourceSilverFolderPath + "/" + sourceSilverFile
finalDataDF.write.mode("overwrite").parquet(sourceGoldFilePath)