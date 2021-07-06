# Databricks notebook source
# Common parameters
IronContainerPath = "abfss://c360iron@dlsldpdev01v8nkg988.dfs.core.windows.net/"
BronzeContainerPath = "abfss://bronze@dlsldpdev01v8nkg988.dfs.core.windows.net/"
SilverContainerPath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/"
GoldContainerPath = "abfss://c360gold@dlsldpdev01v8nkg988.dfs.core.windows.net/"
BronzeQCContainerPath = "abfss://c360bronzeqc@dlsldpdev01v8nkg988.dfs.core.windows.net/"
reconTable = "dbo.Recon"
finalTableSchema = "dbo"
yymmManual = "2021/06"
yyyymmddManual = "2021_06_24"

#Bad Record File Configuration
badRecordsRootPath = "abfss://c360logs@dlsldpdev01v8nkg988.dfs.core.windows.net/"

print(IronContainerPath, "\n", BronzeContainerPath,"\n", SilverContainerPath, "\n", GoldContainerPath, "\n", badRecordsRootPath)

# COMMAND ----------

# Get ADLS Connection string from Key Vault
ADLSConnectionURI = "fs.azure.account.key.dlsldpdev01v8nkg988.dfs.core.windows.net"
ADLSConnectionKey = dbutils.secrets.get(scope = "c360-databricks-secret", key = "adlkey") 

# COMMAND ----------

# Get SQL credentials string from Key Vault
Hostname = dbutils.secrets.get(scope = "c360-databricks-secret", key = "SQLHostName")
Database = dbutils.secrets.get(scope = "c360-databricks-secret", key = "SQLGoldDatabaseName")
Port = 1433
UN = 'lockadmin'
PW = dbutils.secrets.get(scope = "c360-databricks-secret", key = "sqladminpw") 
Url = "jdbc:sqlserver://{0}:{1};database={2};user={3};password= {4}".format(Hostname, Port, Database, UN, PW)
connectionProperties = {
  "user" : UN,
  "password" : PW,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# MAGIC %scala
# MAGIC // Get SQL credentials string from Key Vault
# MAGIC import java.util.Properties
# MAGIC import java.sql.DriverManager
# MAGIC lazy val Hostname = dbutils.secrets.get(scope = "c360-databricks-secret", key = "SQLHostName")
# MAGIC lazy val Database = dbutils.secrets.get(scope = "c360-databricks-secret", key = "SQLGoldDatabaseName")
# MAGIC lazy val finalTableSchema = "dbo"
# MAGIC lazy val jdbcUsername = "lockadmin"
# MAGIC lazy val jdbcPassword = dbutils.secrets.get(scope = "c360-databricks-secret", key = "sqladminpw") 
# MAGIC lazy val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# MAGIC lazy val jdbcUrl = s"jdbc:sqlserver://$Hostname:1433;database=$Database;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;";

# COMMAND ----------

# Data Quality checks
replaceFromCharColumn = "[…• ¢£®°³¼½¾æð•]"
replaceFromCharData = "‘’–—´ºàÁÂÃÄÅÇÈÉêëíîïÑÓôÖØßùúÜý"
replaceToCharData = "''--'oaaaaaaceeeeiiinooooBuuuy"


# COMMAND ----------

dbutils.widgets.text("DebugMode", "","")
RunInDebugMode = dbutils.widgets.get("DebugMode")
print(RunInDebugMode)

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.widgets.text("DebugMode", "","")
# MAGIC val RunInDebugMode = dbutils.widgets.get("DebugMode")

# COMMAND ----------

