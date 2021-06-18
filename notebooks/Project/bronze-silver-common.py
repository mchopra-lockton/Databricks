# Databricks notebook source
from pyspark.sql.functions import *
import re 
from datetime import datetime
now = datetime.now() # current date and time

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

# Retrieve values from ADF parameters and Set the path 

dbutils.widgets.text("ProjectFolderName", "","")
sourcePath = dbutils.widgets.get("ProjectFolderName")
dbutils.widgets.text("ProjectFileName", "","")
sourceFile = dbutils.widgets.get("ProjectFileName")

dbutils.widgets.text("TableName", "","")
sourceTable = dbutils.widgets.get("TableName")

# Set the path for Bronze layer 
sourceBronzePath = BronzeContainerPath + sourcePath
sourceBronzeFilePath = sourceBronzePath + "/" + sourceFile

# Set the path for Silver layer 
sourceSilverPath = SilverContainerPath + sourcePath
sourceSilverFilePath = sourceSilverPath + "/" + sourceFile

dbutils.widgets.text("BatchId", "","")
BatchId = dbutils.widgets.get("BatchId")
dbutils.widgets.text("WorkFlowId", "","")
WorkFlowId = dbutils.widgets.get("WorkFlowId")

#Set the file path to log error
badRecordsPath = badRecordsRootPath + "/" + sourceTable + "/"

print ("Param -\'Variables':")
print (sourceBronzeFilePath)
print (sourceSilverFilePath)
print (badRecordsPath)

# COMMAND ----------

# Read source file
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
try:
  sourceBronzeDF = spark.read.option('badRecordsPath',badRecordsPath).parquet(sourceBronzeFilePath)  
#sourceBronzeDF = spark.read.parquet(sourceBronzeFilePath)
except:
  # Log the error message
  errorDF = spark.createDataFrame([
    (sourceTable,now,sourceBronzeFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceBronzeFilePath}}})

# COMMAND ----------

# Fix the column headers 

sourceBronzeDF = sourceBronzeDF.toDF(*(re.sub(r'[#&()\-\s\'\[\]=]+', '', c) for c in sourceBronzeDF.columns))

# COMMAND ----------

# Fix the data for all columns with datatype as string
dt = sourceBronzeDF.dtypes
columnList = [item[0] for item in dt if item[1].startswith('string')]
for col_name in sourceBronzeDF.columns:
  if col_name in columnList:
      sourceBronzeDF = sourceBronzeDF.withColumn(col_name,regexp_replace(col_name, "[…• ¢£®°³¼½¾æð•]",""))
      sourceBronzeDF = sourceBronzeDF.withColumn(col_name,translate(col_name,"‘’–—´ºàÁÂÃÄÅÇÈÉêëíîïÑÓôÖØßùúÜý","''--'oaaaaaaceeeeiiinooooBuuuy"))      

# COMMAND ----------

# # Fix the data for all columns with datatype as string
# dt = sourceBronzeDF.dtypes
# columnList = [item[0] for item in dt if item[1].startswith('string')]
# for col_name in sourceBronzeDF.columns:
#   if col_name in columnList:
#     sourceBronzeDF = sourceBronzeDF.withColumn(col_name, regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(col_name, '[…• ¢£®°³¼½¾æð]',''),'[‘’´]',"'"),'[–—]','-'),'[ºÓôÖØ]','o'),'[àÁÂÃÄÅ]','a'),'Ç','c'),'[ÈÉêë]','e'),'[íîï]','i'),'Ñ','n'),'ß','B'),'[ùúÜ]','u'),'ý','y'))
# #display(sourceBronzeDF.head(10))

# COMMAND ----------

# Write the parquet file to Silver zone
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInWrite=CORRECTED")
sourceBronzeDF.write.mode("overwrite").parquet(sourceSilverFilePath)

# COMMAND ----------

