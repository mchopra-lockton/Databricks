# Databricks notebook source
IronContainerPath = "abfss://c360iron@dlsldpdev01v8nkg988.dfs.core.windows.net/"
BronzeContainerPath = "abfss://bronze@dlsldpdev01v8nkg988.dfs.core.windows.net/"
SilverContainerPath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/"
GoldContainerPath = "abfss://c360gold@dlsldpdev01v8nkg988.dfs.core.windows.net/"
reconTable = "dbo.Recon"
finalTableSchema = "dbo"

#Bad Record File Configuration
badRecordsRootPath = "abfss://c360logs@dlsldpdev01v8nkg988.dfs.core.windows.net/"

ADLSConnectionURI = "fs.azure.account.key.dlsldpdev01v8nkg988.dfs.core.windows.net"
ADLSConnectionKey = "Fy+q1y58bEH4LmNXs7+VK6ekRER9aA6V54e/8WJUEtBTRKZ7/7ZXUnHq9VkrV29GD1esh1JtF4+/7d9gPvDlPw=="
DBConnectionString = "Server=tcp:sqlsv-ldp-dev-01.database.windows.net,1433;Initial Catalog=sqldb-pmr-CAP360-dev;Persist Security Info=False;User ID=lockadmin;Password=KuVcI71rd50t$5fj;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"
print(IronContainerPath, "\n", BronzeContainerPath,"\n", SilverContainerPath, "\n", GoldContainerPath, "\n", badRecordsRootPath)

# COMMAND ----------

Hostname = "sqlsv-ldp-dev-01.database.windows.net"
Database = "sqldb-GoldZone-CAP360-dev"
Port = 1433
UN = 'lockadmin'
PW = 'KuVcI71rd50t$5fj'
Url = "jdbc:sqlserver://{0}:{1};database={2};user={3};password= {4}".format(Hostname, Port, Database, UN, PW)
connectionProperties = {
  "user" : UN,
  "password" : PW,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
#print(Url)

# COMMAND ----------

# MAGIC %scala
# MAGIC import java.util.Properties
# MAGIC import java.sql.DriverManager
# MAGIC lazy val finalTableSchema = "dbo"
# MAGIC lazy val jdbcUsername = "lockadmin"
# MAGIC lazy val jdbcPassword = "KuVcI71rd50t$5fj"
# MAGIC lazy val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# MAGIC // Create the JDBC URL without passing in the user and password parameters.
# MAGIC lazy val jdbcUrl = s"jdbc:sqlserver://sqlsv-ldp-dev-01.database.windows.net:1433;database=sqldb-GoldZone-CAP360-dev;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;";
# MAGIC // Create a Properties() object to hold the parameters.
# MAGIC // lazy val connectionProperties = new Properties()
# MAGIC // connectionProperties.put("user", s"${jdbcUsername}")
# MAGIC // connectionProperties.put("password", s"${jdbcPassword}")
# MAGIC // connectionProperties.setProperty("Driver", driverClass)