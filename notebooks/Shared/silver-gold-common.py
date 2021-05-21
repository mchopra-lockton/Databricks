# Databricks notebook source
# MAGIC %run "./Database Config"

# COMMAND ----------

spark.conf.set(
  ADLSConnectionURI,
  ADLSConnectionKey
)

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# Set the Select SQL statement
dbutils.widgets.text("SelectQuery", "","")
SelectQuery = dbutils.widgets.get("SelectQuery")
dbutils.widgets.text("InsertQuery", "","")
InsertQuery = dbutils.widgets.get("InsertQuery")

dbutils.widgets.text("TableName", "","")
sourceTable = dbutils.widgets.get("TableName")

# Set the path for Silver layer for Nexsure
dbutils.widgets.text("ProjectFolderName", "","")
sourceSilverPath = dbutils.widgets.get("ProjectFolderName")
sourceSilverPath = SilverContainerPath + sourceSilverPath
dbutils.widgets.text("ProjectFileName", "","")
sourceSilverFile = dbutils.widgets.get("ProjectFileName")
sourceSilverFilePath = sourceSilverPath + "/" + sourceSilverFile

#Set the file path to log error
badRecordsPath = badRecordsRootPath + "/" + sourceTable + "/"

print ("Param -\'Variables':")
print (sourceSilverFilePath)
print (SelectQuery)
print (badRecordsPath)

# COMMAND ----------

# Read source file
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
sourceSilverFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Invoice/Nexsure/FactInvoiceLineItem/2021/05/FactInvoiceLineItem_2021_05_20.parquet" 
sourceSilverDF = spark.read.parquet(sourceSilverFilePath)
#Checking if the file path exists
#try:
#  sourceSilverDF = spark.read.option('badRecordsPath',badRecordsPath).parquet(sourceSilverFilePath)
#except:
#  print("File not found")
#  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "File not found: " + sourceSilverDF}}})

# COMMAND ----------

# Register table so it is accessible via SQL Context
sourceSilverDF.createOrReplaceTempView("tableName")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tableName limit 100

# COMMAND ----------

#SelectQuery = "select AuditKey,max(rowEndDate) as MaxRowEndDate from tableName group By AuditKey limit 5"
SelectQuery = "SELECT c.* FROM tableName c LIMIT 10"
df = spark.sql(SelectQuery)
display(df.head(10))


# COMMAND ----------

sourceSilverDF.createOrReplaceTempView("tableName")

# COMMAND ----------

clientDF.write.option("truncate", "true").jdbc(url=Url, table="[Gold].[DimClient]", mode="overwrite")

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC CREATE TEMPORARY VIEW FactInvoiceLineItem
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url Url,
# MAGIC   dbtable "Gold.FactInvoiceLineItem",
# MAGIC 
# MAGIC )
# MAGIC 
# MAGIC INSERT INTO TABLE FactInvoiceLineItem
# MAGIC SELECT * FROM tableName LIMIT 10

# COMMAND ----------

print(Url)

# COMMAND ----------

