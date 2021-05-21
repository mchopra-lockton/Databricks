# Databricks notebook source
# MAGIC %run "./Database Config"

# COMMAND ----------

spark.conf.set(
  ADLSConnectionURI,
  ADLSConnectionKey
)

# COMMAND ----------

# Read Nexsure data DimEntity
# Use the previously established DBFS mount point to read the data. Create a data frame to read data.
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
sourceNexsureBronzePath = BronzeContainerPath + "Business/Nexsure/DimEntity/2021/05/"
sourceNexsureBronzeFile = "DimEntity_2021_05_07.parquet"
sourceNexsureBronzeFilePath = sourceNexsureBronzePath + sourceNexsureBronzeFile
sourceNexsureBronzeDF = spark.read.parquet(sourceNexsureBronzeFilePath)

display(sourceNexsureBronzeDF)


# COMMAND ----------

# Read BenefitPoint data DimClient
# Use the previously established DBFS mount point to read the data. Create a data frame to read data.
#sourceBPSilverPath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Carrier/Benefits/CLIENT/2021/05/"
#sourceBPSilverFile = "CLIENT_2021_05_07.parquet"
#sourceBPSilverFilePath = sourceBPSilverPath + sourceBPSilverFile
sourceBPSilverDF = spark.read.parquet(sourceBPSilverFilePath)

# display(sourceBPSilverDF)

# COMMAND ----------

# Read BenefitPoint data DimClient
# Use the previously established DBFS mount point to read the data. Create a data frame to read data.
# sourceBPAddrSilverPath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Client/Benefits/CLIENT_ADDRESS/2021/05/"
# sourceBPAddrSilverFile = "CLIENT_ADDRESS_2021_05_07.parquet"
# sourceBPAddrSilverFilePath = sourceBPAddrSilverPath + sourceBPAddrSilverFile
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

# Register table so it is accessible via SQL Context
sourceBPSilverDF.count()


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

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM tmpViewClient

# COMMAND ----------

# targetPath = "abfss://c360gold@dlsldpdev01v8nkg988.dfs.core.windows.net/Client/"
# targetFile = "CLIENT_2021_05_07.parquet"
# targetFilePath = targetPath + targetFile
clientDF.write.mode("overwrite").parquet(targetFilePath)

targetfileDF = spark.read.parquet(targetFilePath)
display(targetfileDF)

# COMMAND ----------

targetfileDF.count()

# COMMAND ----------

targetfileDF.createOrReplaceTempView("ClientTable")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM ClientTable WHERE SourceTable = 'BenefitPoint';

# COMMAND ----------

clientDF.write.option("truncate", "true").jdbc(url=Url, table="[Gold].[DimClient]", mode="overwrite")