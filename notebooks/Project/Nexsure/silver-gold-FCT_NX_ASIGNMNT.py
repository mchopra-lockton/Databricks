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

now = datetime.now()

dbutils.widgets.text("TableName", "","")
GoldFactTableName = dbutils.widgets.get("TableName")

#sourceSilverPath = "Invoice/Nexsure/DimRateType/2021/05"
#FactInvoiceLineItem table
sourceSilverFolderPath = "Person/Nexsure/FactAssignment/" +now.strftime("%Y") + "/" + now.strftime("%m")
sourceSilverPath = SilverContainerPath + sourceSilverFolderPath
sourceSilverFile = "FactAssignment_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
sourceSilverFilePath = sourceSilverPath + "/" + sourceSilverFile

#FactPolicyInfo table
#factPInfoSourceSilverFolderPath = "Policy/Nexsure/FactPolicyInfo/" +now.strftime("%Y") + "/" + now.strftime("%m")
#factPInfoSourceSilverPath = SilverContainerPath + factPInfoSourceSilverFolderPath
#factPInfoSourceSilverFile = "FactPolicyInfo_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
#factPInfoSourceSilverFilePath = factPInfoSourceSilverPath + "/" + factPInfoSourceSilverFile

dbutils.widgets.text("BatchId", "","")
BatchId = dbutils.widgets.get("BatchId")

dbutils.widgets.text("WorkFlowId", "","")
WorkFlowId = dbutils.widgets.get("WorkFlowId")

#Set the file path to log error
badRecordsPath = badRecordsRootPath + GoldFactTableName + "/"


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


# Temporary cell - DELETE
# now = datetime.now() 
GoldFactTableName = "FCT_NX_ASIGNMNT"
# sourceSilverPath = "Invoice/Nexsure/FactInvoiceLineItem/" +now.strftime("%Y") + "/05"
# sourceSilverPath = SilverContainerPath + sourceSilverPath
# sourceSilverFile = "FactInvoiceLineItem_2021_05_21.parquet"
# sourceSilverFilePath = sourceSilverPath + "/" + sourceSilverFile
# badRecordsPath = badRecordsRootPath + GoldFactTableName + "/"
# recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"
# BatchId = "1afc2b6c-d987-48cc-ae8c-a7f41ea27249"
# WorkFlowId ="8fc2895d-de32-4bf4-a531-82f0c6774221"
sourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Person/Nexsure/FactAssignment/2021/06/FactAssignment_2021_06_10.parquet"
#factPInfoSourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Policy/Nexsure/FactPolicyInfo/2021/06/FactPolicyInfo_2021_06_04.parquet"

# COMMAND ----------

# MAGIC %scala
# MAGIC // Temporary cell - DELETE
# MAGIC lazy val GoldFactTableName = "FCT_NX_ASIGNMNT"

# COMMAND ----------

# Do not proceed if any of the parameters are missing
if (GoldFactTableName == "" or sourceSilverPath == "" or sourceSilverFile == ""):
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Input parameters are missing"}}})

# COMMAND ----------

# Read source file
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
try:
 
  sourceSilverDF = spark.read.parquet(sourceSilverFilePath)
except:
  # Log the error message
  errorDF = spark.createDataFrame([
    (GoldFactTableName,now,sourceSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceSilverFilePath}}})  

# COMMAND ----------

sourceSilverDF.createOrReplaceTempView("FCT_NX_ASIGNMNT")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_NX_CLIENT]) client where NX_CLIENT_KEY <> -1"
clientDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(clientDF)
# Register table so it is accessible via SQL Context
clientDF.createOrReplaceTempView("DIM_NX_CLIENT")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_NX_POL]) pol where NX_POLICY_KEY <> -1"
commtaxDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(commtaxDF)
# Register table so it is accessible via SQL Context
commtaxDF.createOrReplaceTempView("DIM_NX_POL")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_NX_EMP]) emp where NX_EMP_KEY <> -1"
invDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(invDF)
# Register table so it is accessible via SQL Context
invDF.createOrReplaceTempView("DIM_NX_EMP")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_NX_ORG]) org where NX_ORG_KEY <> -1"
orgDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(orgDF)
# Register table so it is accessible via SQL Context
orgDF.createOrReplaceTempView("DIM_NX_ORG")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_NX_RESPONSIBILITY]) resp where NX_RESPBLTY_KEY <> -1"
respDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
# display(respDF)
# Register table so it is accessible via SQL Context
respDF.createOrReplaceTempView("DIM_NX_RESPONSIBILITY")

# COMMAND ----------

# Get final set of records
finalDataDF = spark.sql(
f""" 
SELECT 
AssignmentID as NX_ASIGNMNT_ID ,
AssignmentKey as NX_ASIGNMNT_KEY,
AssignmentStartDate as START_DATE ,
AssignmentEndDate as END_DATE,
CurrentAssignmentFlag as CUR_ASIGNMNT_FLG,
PrimaryInd as PRI_ASIGNMNT_FLG ,
ClientPolicyInd as CLNT_POL_INDICTR,
DBSourceKey as DB_SRC_KEY,
InsertAuditKey as SRC_AUDT_INS_KEY,
UpdateAuditKey as SRC_AUDT_UPD_KEY,
coalesce(SURR_RESP_ID,0) as RESPNSBLTY_ID,
SURR_ORG_ID as ORG_ID  ,
coalesce(SURR_POL_ID,0) as POL_ID,
SURR_EMP_ID as EMP_ID,  
coalesce(SURR_CLIENT_ID,0) as CLNT_ID,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId }' AS ETL_WORKFLOW_ID,
current_timestamp() AS ETL_CREATED_DT,
current_timestamp() AS ETL_UPDATED_DT
FROM FCT_NX_ASIGNMNT fact
JOIN DIM_NX_RESPONSIBILITY Rsp on fact.ResponsibilityKey = Rsp.NX_RESPBLTY_KEY
JOIN DIM_NX_POL pol on fact.PolicyKey = pol.NX_POLICY_KEY
JOIN DIM_NX_CLIENT cl on fact.ClientKey = cl.NX_CLIENT_KEY
JOIN DIM_NX_EMP emp on fact.AssignmentEmployeeKey = emp.NX_EMP_KEY
JOIN DIM_NX_ORG Org on fact.OrgStructureKey = Org.NX_ORG_KEY
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

# Load the records to final table
GoldFactTableNameComplete = finalTableSchema + "." + GoldFactTableName
finalDataDF.write.jdbc(url=Url, table=GoldFactTableNameComplete, mode="append")

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