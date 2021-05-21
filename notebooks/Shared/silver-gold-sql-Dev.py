# Databricks notebook source
# MAGIC %run "./Database Config"

# COMMAND ----------

spark.conf.set(
  ADLSConnectionURI,
  ADLSConnectionKey
)

# COMMAND ----------

# Set the path for Silver layer for Nexsure
dbutils.widgets.text("NexsureSilverPath", "","")
sourceNexsureSilverPath = dbutils.widgets.get("NexsureSilverPath")
sourceNexsureSilverPath = SilverContainerPath + sourceNexsureSilverPath
dbutils.widgets.text("NexsureSilverFile", "","")
sourceNexsureSilverFile = dbutils.widgets.get("NexsureSilverFile")
sourceNexsureSilverFilePath = sourceNexsureSilverPath + sourceNexsureSilverFile

# Set the path for Silver layer for BP
dbutils.widgets.text("BPSilverPath", "","")
sourceBPSilverPath = dbutils.widgets.get("BPSilverPath")
sourceBPSilverPath = SilverContainerPath + sourceBPSilverPath
dbutils.widgets.text("BPSilverFile", "","")
sourceBPSilverFile = dbutils.widgets.get("BPSilverFile")
sourceBPSilverFilePath = sourceBPSilverPath + sourceBPSilverFile

# Set the path for Silver layer for BP Address
dbutils.widgets.text("BPAddrSilverPath", "","")
sourceBPAddrSilverPath = dbutils.widgets.get("BPAddrSilverPath")
sourceBPAddrSilverPath = SilverContainerPath + sourceBPAddrSilverPath
dbutils.widgets.text("BPAddrSilverFile", "","")
sourceBPAddrSilverFile = dbutils.widgets.get("BPAddrSilverFile")
sourceBPAddrSilverFilePath = sourceBPAddrSilverPath + sourceBPAddrSilverFile

# Set the path for Gold layer for Client data
dbutils.widgets.text("GoldPath", "","")
targetPath = dbutils.widgets.get("GoldPath")
targetPath = GoldContainerPath + targetPath
dbutils.widgets.text("GoldFile", "","")
targetFile = dbutils.widgets.get("GoldFile")
targetFilePath = targetPath + targetFile

print ("Param -\'Variables':")
print (sourceNexsureSilverFilePath)
print (sourceBPSilverFilePath)
print (sourceBPAddrSilverFilePath)
print (targetFilePath)

# COMMAND ----------

# Read Nexsure data DimEntity
# Use the previously established DBFS mount point to read the data. Create a data frame to read data.
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
sourceNexsureSilverPath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Business/Nexsure/DimEntity/2021/05/"
sourceNexsureSilverFile = "DimEntity_2021_05_07.parquet"
sourceNexsureSilverFilePath = sourceNexsureSilverPath + sourceNexsureSilverFile
sourceNexsureSilverDF = spark.read.parquet(sourceNexsureSilverFilePath)
sourceNexsureSilverDF = spark.read.parquet(sourceNexsureSilverFilePath)

display(sourceNexsureSilverDF)


# COMMAND ----------

# Read BenefitPoint data DimClient
# Use the previously established DBFS mount point to read the data. Create a data frame to read data.
sourceBPSilverPath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Carrier/Benefits/CLIENT/2021/05/"
sourceBPSilverFile = "CLIENT_2021_05_07.parquet"
sourceBPSilverFilePath = sourceBPSilverPath + sourceBPSilverFile
sourceBPSilverDF = spark.read.parquet(sourceBPSilverFilePath)

# display(sourceBPSilverDF)

# COMMAND ----------

# Read BenefitPoint data DimClient
# Use the previously established DBFS mount point to read the data. Create a data frame to read data.
sourceBPAddrSilverPath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Client/Benefits/CLIENT_ADDRESS/2021/05/"
sourceBPAddrSilverFile = "CLIENT_ADDRESS_2021_05_07.parquet"
sourceBPAddrSilverFilePath = sourceBPAddrSilverPath + sourceBPAddrSilverFile
sourceBPAddrSilverDF = spark.read.parquet(sourceBPAddrSilverFilePath)

# display(sourceBPSilverDF)

# COMMAND ----------

sourceNexsureSilverDF.count()
sourceBPSilverDF.count()
sourceBPAddrSilverDF.count()

# COMMAND ----------

# Register table so it is accessible via SQL Context
sourceNexsureSilverDF.createOrReplaceTempView("NexsureClientSilverTable")
sourceBPSilverDF.createOrReplaceTempView("BPClientSilverTable")
sourceBPAddrSilverDF.createOrReplaceTempView("BPClientAddrSilverTable")


# COMMAND ----------

display(sourceBPAddrSilverDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM BPClientAddrSilverTable LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tmpViewBPClient;
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tmpViewBPClient
# MAGIC         AS
# MAGIC       SELECT  CL.CLIENT_ID,
# MAGIC         CL.NAME,
# MAGIC         CA.STATE,
# MAGIC         CA.STREET1,
# MAGIC         CA.STREET2,
# MAGIC         CA.CITY,
# MAGIC         CA.POSTAL_CODE
# MAGIC FROM BPClientSilverTable CL
# MAGIC INNER JOIN BPClientAddrSilverTable CA ON CL.CLIENT_ID = CA.CLIENT_ID
# MAGIC     

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from tmpViewBPClient Limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tmpClient;
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tmpViewClient
# MAGIC         AS
# MAGIC       SELECT  NXCL.EntityID as ClientSourceID,
# MAGIC         NXCL.EntityName as ClientName,
# MAGIC         NXCL.EntityAddressState as ClientState,
# MAGIC         NXCL.EntityAddressLine1 as AddressLine1,
# MAGIC         NXCL.EntityAddressLine2 as AddressLine2,
# MAGIC         NXCL.EntityAddressCity as ClientCity,
# MAGIC         NXCL.EntityAddressZip as ClientZip,
# MAGIC         'Nexsure' as SourceTable
# MAGIC     FROM NexsureClientSilverTable NXCL
# MAGIC     UNION
# MAGIC       SELECT  BPCL.CLIENT_ID as ClientSourceID,
# MAGIC         BPCL.NAME as ClientName,
# MAGIC         BPCL.STATE as ClientState,
# MAGIC         BPCL.STREET1 as AddressLine1,
# MAGIC         BPCL.STREET2 as AddressLine2,
# MAGIC         BPCL.CITY as ClientCity,
# MAGIC         BPCL.POSTAL_CODE as ClientZip,
# MAGIC         'BenefitPoint' as SourceTable
# MAGIC     FROM tmpViewBPClient BPCL    
# MAGIC     
# MAGIC     

# COMMAND ----------

clientDF = spark.sql(F"""SELECT  NXCL.EntityID as ClientSourceID,
        NXCL.EntityName as ClientName,
        NXCL.EntityAddressState as ClientState,
        NXCL.EntityAddressLine1 as AddressLine1,
        NXCL.EntityAddressLine2 as AddressLine2,
        NXCL.EntityAddressCity as ClientCity,
        NXCL.EntityAddressZip as ClientZip,
        'Nexsure' as SourceTable
    FROM NexsureClientSilverTable NXCL
    UNION
      SELECT  BPCL.CLIENT_ID as ClientSourceID,
        BPCL.NAME as ClientName,
        BPCL.STATE as ClientState,
        BPCL.STREET1 as AddressLine1,
        BPCL.STREET2 as AddressLine2,
        BPCL.CITY as ClientCity,
        BPCL.POSTAL_CODE as ClientZip,
        'BenefitPoint' as SourceTable
    FROM tmpViewBPClient BPCL    
    """)
    

# COMMAND ----------

targetPath = "abfss://c360gold@dlsldpdev01v8nkg988.dfs.core.windows.net/Client/"
targetFile = "CLIENT_2021_05_07.parquet"
targetFilePath = targetPath + targetFile
clientDF.write.mode("overwrite").parquet(targetFilePath)

targetfileDF = spark.read.parquet(targetFilePath)
display(targetfileDF)

# COMMAND ----------

targetfileDF.count()

# COMMAND ----------

targetfileDF.createOrReplaceTempView("ClientTable")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM ClientTable WHERE SourceTable = 'Nexsure';

# COMMAND ----------

clientDF.write.option("truncate", "true").jdbc(url=Url, table="[Gold].[DimClient]", mode="overwrite")

# COMMAND ----------

