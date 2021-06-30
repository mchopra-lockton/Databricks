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
# MAGIC dbutils.widgets.text("TableName", "","")
# MAGIC lazy val GoldDimTableName = dbutils.widgets.get("TableName")
# MAGIC print (GoldDimTableName)

# COMMAND ----------

# Get parameters from ADF
now = datetime.now() 
dbutils.widgets.text("TableName", "","")
GoldDimTableName = dbutils.widgets.get("TableName")

dbutils.widgets.text("BatchId", "","")
BatchId = dbutils.widgets.get("BatchId")

dbutils.widgets.text("WorkFlowId", "","")
WorkFlowId = dbutils.widgets.get("WorkFlowId")

#Set the file path to log error
badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"

date_time = now.strftime("%Y%m%dT%H%M%S")
badRecordsFilePath = badRecordsPath + date_time + "/" + "ErrorRecords"
recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"

#Set the file path to log error
#badRecordsPath = badRecordsRootPath + "/" + sourceTable + "/"

print ("Param -\'Variables':")
#print (sourceSilverFilePath)
print (badRecordsFilePath)
print (recordCountFilePath)

# COMMAND ----------

# Set the path for Silver layer 
now = datetime.now() 

# BP Client data 
sourceBPClientSilverFilePath = SilverContainerPath + "Client/Benefits/vw_CLIENT_AllClients/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "vw_CLIENT_AllClients_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
#ClientAddress
sourceBPCLientAddressSilverFilePath = SilverContainerPath + "Client/Benefits/vw_CLIENT_ADDRESS_AllRecs/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "vw_CLIENT_ADDRESS_AllRecs_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
#Broker
sourceBPBrokerSilverFilePath = SilverContainerPath + "Person/Benefits/vw_BROKER_AllRecs/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "vw_BROKER_AllRecs_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
#Brokeroffice
sourceBPBrokerOfficeSilverFilePath = SilverContainerPath + "OrgStructure/Benefits/vw_BROKER_OFFICE_AllRecs/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "vw_BROKER_OFFICE_AllRecs_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
#Clientcontact
sourceBPClientContactSilverFilePath = SilverContainerPath + "Client/Benefits/vw_CLIENT_CONTACT_AllRecs/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "vw_CLIENT_CONTACT_AllRecs_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
#BPcontact
sourceBPContactSilverFilePath = SilverContainerPath + "Person/Benefits/vw_CONTACT_AllRecs/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "vw_CONTACT_AllRecs_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
#ClientAccountTeam
sourceBPCLientAccountTeamSilverFilePath = SilverContainerPath + "Client/Benefits/vw_CLIENT_ACCOUNT_TEAM_AllRecs/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "vw_CLIENT_ACCOUNT_TEAM_AllRecs_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
#Custom Field Value
sourceBPCustomFieldSilverFilePath = SilverContainerPath + "Reference/Benefits/CUSTOM_FIELD_VALUE/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "CUSTOM_FIELD_VALUE_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
#clientvalieassoc
sourceBPClientvalueassocSilverFilePath = SilverContainerPath + "Client/Benefits/CF_CLIENT_VALUE_ASSOC/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "CF_CLIENT_VALUE_ASSOC_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
#Client_service_lead
sourceBPServiceSilverFilePath = SilverContainerPath + "Reference/Benefits/CUSTOM_FIELD_VALUE/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "CUSTOM_FIELD_VALUE_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
#MasterLink data
sourceMDS2_MasterLinkNoLMSilverFilePath = SilverContainerPath + "Client/MDS2/MasterLinkNoLM/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "MasterLinkNoLM_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
#DUNS Reffernce Data
sourceMDS2_DUNS_ReferenceSilverFilePath = SilverContainerPath + "Client/MDS2/DUNS_Reference/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "DUNS_Reference_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
#PitchBook CompanyData
sourceMDS2_Pitchbook_PB_ClientToCompanySilverFilePath = SilverContainerPath + "Client/Pitchbook/PB_ClientToCompany/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "PB_ClientToCompany_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
#PitchBookClienttoSilver data
sourceMDS2_PB_Reference_CompanyActiveInvestorRelationSilverFilePath = SilverContainerPath + "Client/Pitchbook/PB_Reference_CompanyActiveInvestorRelation/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "PB_Reference_CompanyActiveInvestorRelation_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
#exclude table
sourceBPClientExcludeSilverFilePath = SilverContainerPath + "Business/MDS2/ClientExclude/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "ClientExclude_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
#CF Option value
sourceBPClientOptionValueFilePath = SilverContainerPath + "Reference/Benefits/CF_OPTION_VALUE/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "CF_OPTION_VALUE_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
#Customvalue
sourceBPClientCustomValueeFilePath = SilverContainerPath + "Reference/Benefits/CUSTOM_FIELD/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "CUSTOM_FIELD_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"

# COMMAND ----------

# Temporary cell to run manually - DELETE
now = datetime.now() 
date_time = now.strftime("%Y%m%dT%H%M%S")
GoldDimTableName = "DIM_BP_CLIENT"
badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"
recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"
BatchId = "1afc2b6c-d987-48cc-ae8c-a7f41ea27249"
WorkFlowId ="8fc2895d-de32-4bf4-a531-82f0c6774221"

# BP Client data 
sourceBPClientSilverFilePath = SilverContainerPath + "Client/Benefits/vw_CLIENT_AllClients/" + yymmManual + "/vw_CLIENT_AllClients_" + yyyymmddManual + ".parquet"
#ClientAddress
sourceBPCLientAddressSilverFilePath = SilverContainerPath + "Client/Benefits/vw_CLIENT_ADDRESS_AllRecs/" + yymmManual + "/vw_CLIENT_ADDRESS_AllRecs_" + yyyymmddManual + ".parquet"
#Broker
sourceBPBrokerSilverFilePath = SilverContainerPath + "Person/Benefits/vw_BROKER_AllRecs/" + yymmManual + "/vw_BROKER_AllRecs_" + yyyymmddManual + ".parquet"
#Brokeroffice
sourceBPBrokerOfficeSilverFilePath = SilverContainerPath + "OrgStructure/Benefits/vw_BROKER_OFFICE_AllRecs/" + yymmManual + "/vw_BROKER_OFFICE_AllRecs_" + yyyymmddManual + ".parquet"
#Clientcontact
sourceBPClientContactSilverFilePath = SilverContainerPath + "Client/Benefits/vw_CLIENT_CONTACT_AllRecs/" + yymmManual + "/vw_CLIENT_CONTACT_AllRecs_" + yyyymmddManual + ".parquet"
#BPcontact
sourceBPContactSilverFilePath = SilverContainerPath + "Person/Benefits/vw_CONTACT_AllRecs/" + yymmManual + "/vw_CONTACT_AllRecs_" + yyyymmddManual + ".parquet"
#ClientAccountTeam
sourceBPCLientAccountTeamSilverFilePath = SilverContainerPath + "Client/Benefits/vw_CLIENT_ACCOUNT_TEAM_AllRecs/" + yymmManual + "/vw_CLIENT_ACCOUNT_TEAM_AllRecs_" + yyyymmddManual + ".parquet"
#Custom Field Value
sourceBPCustomFieldSilverFilePath = SilverContainerPath + "Reference/Benefits/CUSTOM_FIELD_VALUE/" + yymmManual + "/CUSTOM_FIELD_VALUE_" + yyyymmddManual + ".parquet"
#clientvalieassoc
sourceBPClientvalueassocSilverFilePath = SilverContainerPath + "Client/Benefits/CF_CLIENT_VALUE_ASSOC/" + yymmManual + "/CF_CLIENT_VALUE_ASSOC_" + yyyymmddManual + ".parquet"
#Client_service_lead
sourceBPServiceSilverFilePath = SilverContainerPath + "Reference/Benefits/CUSTOM_FIELD_VALUE/" + yymmManual + "/CUSTOM_FIELD_VALUE_" + yyyymmddManual + ".parquet"
#MasterLink data
sourceMDS2_MasterLinkNoLMSilverFilePath = SilverContainerPath + "Client/MDS2/MasterLinkNoLM/" + yymmManual + "/MasterLinkNoLM_" + yyyymmddManual + ".parquet"
#DUNS Reffernce Data
sourceMDS2_DUNS_ReferenceSilverFilePath = SilverContainerPath + "Client/MDS2/DUNS_Reference/" + yymmManual + "/DUNS_Reference_" + yyyymmddManual + ".parquet"
#PitchBook CompanyData
sourceMDS2_Pitchbook_PB_ClientToCompanySilverFilePath = SilverContainerPath + "Client/Pitchbook/PB_ClientToCompany/" + yymmManual + "/PB_ClientToCompany_" + yyyymmddManual + ".parquet"
#PitchBookClienttoSilver data
sourceMDS2_PB_Reference_CompanyActiveInvestorRelationSilverFilePath = SilverContainerPath + "Client/Pitchbook/PB_Reference_CompanyActiveInvestorRelation/" + yymmManual + "/PB_Reference_CompanyActiveInvestorRelation_" + yyyymmddManual + ".parquet"
#exclude table
sourceBPClientExcludeSilverFilePath = SilverContainerPath + "Business/MDS2/ClientExclude/" + yymmManual + "/ClientExclude_" + yyyymmddManual + ".parquet"
#CF Option value
sourceBPClientOptionValueFilePath = SilverContainerPath + "Reference/Benefits/CF_OPTION_VALUE/" + yymmManual + "/CF_OPTION_VALUE_" + yyyymmddManual + ".parquet"
#Customvalue
sourceBPClientCustomValueeFilePath = SilverContainerPath + "Reference/Benefits/CUSTOM_FIELD/" + yymmManual + "/CUSTOM_FIELD_" + yyyymmddManual + ".parquet"

# HARD CODED FILE NAMES - CURRENT FILES NOT AVAILABLE
sourceMDS2_Pitchbook_PB_ClientToCompanySilverFilePath= "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Client/Pitchbook/PB_ClientToCompany/2021/06/PB_ClientToCompany_2021_06_22.parquet"
sourceMDS2_PB_Reference_CompanyActiveInvestorRelationSilverFilePath ="abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Client/Pitchbook/PB_Reference_CompanyActiveInvestorRelation/2021/06/PB_Reference_CompanyActiveInvestorRelation_2021_06_22.parquet"

# COMMAND ----------

# MAGIC %scala
# MAGIC // Temporary cell to run manually - DELETE
# MAGIC lazy val GoldDimTableName = "Dim_BP_CLIENT"  

# COMMAND ----------

# Do not proceed if any of the parameters are missing
if (GoldDimTableName == "" or sourceBPClientSilverFilePath == ""):
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Input parameters are missing"}}})

# COMMAND ----------

spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")

try:
   sourceBPClientSilverDF = spark.read.parquet(sourceBPClientSilverFilePath)
      
except:
    # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceBPClientSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceBPClientSilverFilePath}}})  

# COMMAND ----------

try:
  sourceBPCLientAddressSilverDF = spark.read.parquet(sourceBPCLientAddressSilverFilePath)
  sourceBPCLientAddressSilverDF.createOrReplaceTempView("SilverBPAddressTemp")
  sourceBPCLientAddressSilverFinalDF = spark.sql(f""" select * from (select *, row_number() over(PARTITION BY  CLIENT_ID order by 
rowEndDate desc) client_rank from  SilverBPAddressTemp where ADDRESS_TYPE_DESC='Main' ) where client_rank=1 """)
      
except:
    # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceBPCLientAddressSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceBPCLientAddressSilverFilePath}}})  

# COMMAND ----------

try:
   sourceBPBrokerSilverDF = spark.read.parquet(sourceBPBrokerSilverFilePath)
      
except:
    # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceBPBrokerSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceBPBrokerSilverFilePath}}})  

# COMMAND ----------

try:
   sourceBPBrokerOfficeSilverDF = spark.read.parquet(sourceBPBrokerOfficeSilverFilePath)
      
except:
    # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceBPBrokerOfficeSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceBPBrokerOfficeSilverFilePath}}})  

# COMMAND ----------

try:
  sourceBPClientContactSilverDF = spark.read.parquet(sourceBPClientContactSilverFilePath)
  sourceBPClientContactSilverDF.createOrReplaceTempView("SilverBPClientContactTemp")
  sourceBPClientContactSilverFinalDF = spark.sql(f""" select * from (select *, row_number() over(PARTITION BY  client_id order by 
RowBeginDate desc) client_rank 
from SilverBPClientContactTemp  where rowenddate = '9999-12-31')t  where client_rank=1 """)
      
except:
    # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceBPClientContactSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceBPClientContactSilverFilePath}}}) 

# COMMAND ----------

try:
  sourceBPContactSilverDF = spark.read.parquet(sourceBPContactSilverFilePath)
  sourceBPContactSilverDF.createOrReplaceTempView("SilverBPContactTemp")
  sourceBPContactSilverFinalDF = spark.sql(f""" select * from (select *, row_number() over(PARTITION BY  contact_id order by 
RowBeginDate desc) client_rank 
from SilverBPContactTemp  where rowenddate = '9999-12-31')t  where client_rank=1 """)
except:
    # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceBPContactSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceBPContactSilverFilePath}}})

# COMMAND ----------

try:
   sourceBPCLientAccountTeamSilverDF = spark.read.parquet(sourceBPCLientAccountTeamSilverFilePath)
      
except:
    # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceBPCLientAccountTeamSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceBPCLientAccountTeamSilverFilePath}}})

# COMMAND ----------

try:
   sourceBPClientvalueassocSilverDF = spark.read.parquet(sourceBPClientvalueassocSilverFilePath)
      
except:
    # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceBPClientvalueassocSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceBPClientvalueassocSilverFilePath}}})

# COMMAND ----------

try:
   sourceBPCustomFieldSilverDF = spark.read.parquet(sourceBPCustomFieldSilverFilePath)
      
except:
    # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceBPCustomFieldSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceBPCustomFieldSilverFilePath}}})

# COMMAND ----------

try:
   sourceBPServiceSilverDF = spark.read.parquet(sourceBPServiceSilverFilePath)
      
except:
    # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceBPServiceSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceBPServiceSilverFilePath}}})

# COMMAND ----------

try:
   sourceMDS2_MasterLinkNoLMSilverDF = spark.read.parquet(sourceMDS2_MasterLinkNoLMSilverFilePath)
      
except:
    # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceMDS2_MasterLinkNoLMSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceMDS2_MasterLinkNoLMSilverFilePath}}})

# COMMAND ----------

try:
   sourceMDS2_DUNS_ReferenceSilverDF = spark.read.parquet(sourceMDS2_DUNS_ReferenceSilverFilePath)
      
except:
    # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceMDS2_DUNS_ReferenceSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceMDS2_DUNS_ReferenceSilverFilePath}}})

# COMMAND ----------

try:
   sourceMDS2_Pitchbook_PB_ClientToCompanySilverDF = spark.read.parquet(sourceMDS2_Pitchbook_PB_ClientToCompanySilverFilePath)
      
except:
    # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceMDS2_Pitchbook_PB_ClientToCompanySilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceMDS2_Pitchbook_PB_ClientToCompanySilverFilePath}}})

# COMMAND ----------

try:
    
  sourceMDS2_PB_Reference_CompanyActiveInvestorRelationSilverDF=spark.read.parquet(sourceMDS2_PB_Reference_CompanyActiveInvestorRelationSilverFilePath)
    
  sourceMDS2_PB_Reference_CompanyActiveInvestorRelationSilverDF.createOrReplaceTempView("SilverBPPBActiveInvestorTEMP")
    
  sourceMDS2_PB_Reference_CompanyActiveInvestorRelationSilverFinalDF = spark.sql(f""" select * from (select *, row_number() over(PARTITION BY CompanyID order by InvestorID asc) client_rank from SilverBPPBActiveInvestorTEMP  where Holding <> 'Minority') where client_rank=1; """)
      
except:
    # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceMDS2_PB_Reference_CompanyActiveInvestorRelationSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceMDS2_PB_Reference_CompanyActiveInvestorRelationSilverFilePath}}})

# COMMAND ----------

try:
   sourceBPClientExcludeSilverDF = spark.read.parquet(sourceBPClientExcludeSilverFilePath)
      
except:
    # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceBPClientExcludeSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceBPClientExcludeSilverFilePath}}})

# COMMAND ----------

try:
   sourceBPClientOptionValueDF = spark.read.parquet(sourceBPClientOptionValueFilePath)
      
except:
    # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceBPClientOptionValueFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceBPClientOptionValueFilePath}}})

# COMMAND ----------

try:
   sourceBPClientCustomValueeDF = spark.read.parquet(sourceBPClientCustomValueeFilePath)
      
except:
    # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceBPClientCustomValueeFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceBPClientCustomValueeFilePath}}})

# COMMAND ----------

try:
   sourceBPContactSilverDF = spark.read.parquet(sourceBPContactSilverFilePath)
      
except:
    # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceBPContactSilverFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceBPContactSilverFilePath}}})

# COMMAND ----------

#creating max NAICS code
sourceBPClientSilverDF.createOrReplaceTempView("SilverBPClientTemp")
sourceBPBrokerSilverDF.createOrReplaceTempView("SilverBPBrokerTemp")
sourceBPBrokerOfficeSilverDF.createOrReplaceTempView("SilverBPBRokerOfficeTEMP")
sourceBPCLientAccountTeamSilverDF.createOrReplaceTempView("SilverBPClientAccountTeamsTEMP")
sourceBPClientvalueassocSilverDF.createOrReplaceTempView("SilverBPClientValueAssocTEMP")
sourceBPCustomFieldSilverDF.createOrReplaceTempView("SilverBPCustomFieldTEMP")
sourceBPServiceSilverDF.createOrReplaceTempView("SilverBPServiceLeadTEMP")
sourceMDS2_MasterLinkNoLMSilverDF.createOrReplaceTempView("SilverBPMasterLinkTEMP")
sourceMDS2_DUNS_ReferenceSilverDF.createOrReplaceTempView("SilverBPDUNSRefTEMP")
sourceMDS2_Pitchbook_PB_ClientToCompanySilverDF.createOrReplaceTempView("SilverBPPithbooktoCompTEMP")
sourceBPClientExcludeSilverDF.createOrReplaceTempView("SilverBPClientExclude")
sourceBPClientOptionValueDF.createOrReplaceTempView("SilverBPCFOption")
sourceBPClientCustomValueeDF.createOrReplaceTempView("SilverBPCustomValue")
sourceBPContactSilverDF.createOrReplaceTempView("SilverBPSilverContactTemp")
sourceMDS2_PB_Reference_CompanyActiveInvestorRelationSilverFinalDF.createOrReplaceTempView("SilverBPPBActiveInvestorFinal")
sourceBPCLientAddressSilverFinalDF.createOrReplaceTempView("SilverBPAddressFinal")
sourceBPClientContactSilverFinalDF.createOrReplaceTempView("SilverBPClientContactFinal")
sourceBPContactSilverFinalDF.createOrReplaceTempView("SilverBPContactFinal")

# COMMAND ----------

SilverBPClientCustomfiledDF = spark.sql(
f"""
SELECT c2.CLIENT_ID, cfv.CUSTOM_FIELD_VALUE_ID,cfv.CUSTOM_FIELD_ID
       from SilverBPClientTemp c2
       inner JOIN (select * from SilverBPClientValueAssocTEMP where rowenddate = '9999-12-31') cfva  on c2.CLIENT_ID = cfva.CLIENT_ID
       INNER JOIN (select * from SilverBPCustomFieldTEMP where rowenddate = '9999-12-31' and CUSTOM_FIELD_ID = '100134') cfv on cfva.CUSTOM_FIELD_VALUE_ID = cfv.CUSTOM_FIELD_VALUE_ID
       INNER JOIN (select * from SilverBPCFOption where rowenddate = '9999-12-31' and DESCRIPTION!='Select') cfo on cfv.CF_OPTION_VALUE_ID = cfo.CF_OPTION_VALUE_ID
       INNER JOIN (select * from SilverBPCustomValue where rowenddate = '9999-12-31' and CUSTOM_FIELD_ID = '100134') cf on cfv.CUSTOM_FIELD_ID = cf.CUSTOM_FIELD_ID
"""
)

SilverBPClientCustomfiledDF.createOrReplaceTempView("SilverBPClientCustomfiled")

# COMMAND ----------

pushdown_query = "(select * from [dbo].[DIM_BP_ORG]) ORG"
carrierDF = spark.read.jdbc(url=Url, table=pushdown_query, properties=connectionProperties)
display(carrierDF)
# Register table so it is accessible via SQL Context
carrierDF.createOrReplaceTempView("DIM_BP_ORG")

# COMMAND ----------

dummyDataDF = spark.sql(
f""" 
SELECT
-99999 as BP_CLNT_ID,
'None' as IS_PROSPECT  ,
'None' as CLNT_FUND_TYPE_DESC ,
'1800-12-31' as CLNT_SINCE  ,
'1800-12-31' as SRC_ROW_BEGIN_DT  ,
'1800-12-31' as SRC_ROW_END_DT   
,'{ BatchId }' AS ETL_BATCH_ID
,'{ WorkFlowId }' AS ETL_WRKFLW_ID
,current_timestamp() AS ETL_CREATED_DT
,current_timestamp() AS ETL_UPDATED_DT
FROM SilverBPClientTemp e LIMIT 1
"""
)

# COMMAND ----------

finalDataDF = spark.sql(
f""" 
SELECT
a.CLIENT_ID as BP_CLNT_ID   ,
a.ACCOUNT_NUM as ACCOUNT_NUM  ,
a.NAME as NAME  ,
a.WEBSITE as WEBSITE  ,
a.ACTIVE_IND as ACTIV_IND  ,
b.STREET1 as ADDR_LINE_1 ,
b.STREET2 AS  ADDR_LINE_2 ,
b.CITY as CITY  ,
b.POSTAL_CODE as ZIP  ,
b.STATE as STATE  ,
b.COUNTRY as COUNTRY  ,
a.NAICS_CODE as PRI_NAICS_CD  ,
a.SIC_CODE as PRI_SIC_CD  ,
f1.FIRST_NAME as FIRST_NAME  ,
f1.LAST_NAME as LAST_NAME  ,
f1.EMAIL_ADDR as EMAIL  ,
'' as IS_PROSPECT  ,
org.SURR_ORG_ID as SURR_ORG_ID  ,
a.OWNER_DEPARTMENT_ID as BRKRAGE_DEPT_ID  ,
x1.CUSTOM_FIELD_ID as AGENCY_CD  ,
x1.CUSTOM_FIELD_VALUE_ID as BRANCH_CD  ,
a.DOING_BUSINESS_AS as DOING_BUS_AS  ,
a.CLIENT_FUNDING_TYPE_DESC as CLNT_FUND_TYPE_DESC   ,
a.BUSINESS_TYPE_DESC as BUS_TYPE  ,
bSales.LAST_NAME + ', ' + bSales.FIRST_NAME as PRI_SALES_LEAD  ,
bAdmin.LAST_NAME + ', ' + bAdmin.FIRST_NAME as ADMIN  ,
bContact.LAST_NAME + ', ' + bContact.FIRST_NAME as PRI_CONTACT  ,
a.BROKER_OF_RECORD as TAM_CLNT_ID  ,
a.CREATE_DATE as PRI_BP_EFF_DT  ,
a.RowBeginDate as CLNT_SINCE   ,
'' as BP_LAST_MOD_DT  ,
a.RowBeginDate as SRC_ROW_BEGIN_DT   ,
a.RowEndDate as SRC_ROW_END_DT   ,
'{ BatchId }' AS ETL_BATCH_ID,
'{ WorkFlowId }' AS ETL_WRKFLW_ID,
current_timestamp() AS ETL_CREATED_DT,
current_timestamp() AS ETL_UPDATED_DT,
Public_Private_Indicator as PUBLIC_PRIV_IND  ,
Non_Profit_Indicator as NON_PRFIT  ,
'' as ANNL_REVNUE  ,
'' as NUM_OF_EMP  ,
NYSE_Ticker as NYSE_TICKER  ,
ASE_Ticker as ASE_TICKER  ,
NMS_Ticker as NMS_TICKER  ,
NAS_Ticker as NAS_TICKER  ,
OTC_Ticker as OTC_TICKER  ,
Global_Ultimate_D_U_N_S_Number as GLOB_ULTMT_DUNS_NUM  ,
Global_Ultimate_Business_Name as GLOBL_ULTMT_BUSS_NAME  ,
Domestic_Ultimate_D_U_N_S_Number as DOMSTIC_ULTMT_DUNS_NUM  ,
Domestic_Ultimate_Business_Name as DOMSTIC_ULTMT_BUSS_NAME  ,
Parent_D_U_N_S_Number as PRNT_DUNS_NUM  ,
Headquarter_Parent_Business_Name as HEADQRTER_PRNT_BUSS_NAME  ,
'' as BUSS_DESC  ,
'' as PTCHBK_MATCH  ,
ClientSourceID as PTCHBK_CLNTSRCID  ,
PBID_ToUse as PTCHBK_PBID_TOUSE  ,
InvestorID as PTCHBK_INVESTR_ID  ,
InvestorName as PTCHBK_INVESTR_NAME  ,
InvestorStatus as PTCHBK_INVESTR_STATUS ,
InvestorType as PTCHBK_INVESTR_TYPE  ,
Holding as PTCHBK_HOLDNG  ,
InvestorSince as PTCHBK_INVSTOR_SINCE  ,
'' as DB_MATCH  ,
D_U_N_S_Number as DUNS_NUM  ,
'' as PRI_NAICS_DESC  ,
'' as CONFDL_ACCT 
from SilverBPClientTemp a
LEFT JOIN SilverBPAddressFinal b on a.client_id = b.client_id 
LEFT JOIN SilverBPClientExclude z on a.client_id = z.SourceId
LEFT JOIN SilverBPBRokerOfficeTEMP bo on a.OWNER_OFFICE_ID = bo.OFFICE_ID
LEFT JOIN SilverBPBrokerTemp bSales on a.SALES_LEAD_BROKER_ID = bSales.BROKER_ID 
LEFT JOIN SilverBPBrokerTemp bService on a.SERVICE_LEAD_BROKER_ID = bService.BROKER_ID
LEFT JOIN (select * from SilverBPClientAccountTeamsTEMP   where rowenddate = '9999-12-31') catAdmin  on a.CLIENT_ID = catAdmin.CLIENT_ID  and catAdmin.ACCOUNT_TEAM_OWNER_IND = 1
LEFT JOIN SilverBPBrokerTemp bAdmin on catAdmin.BROKER_ID = bAdmin.BROKER_ID
LEFT JOIN (select * from SilverBPClientAccountTeamsTEMP where rowenddate = '9999-12-31')catRenewal on a.CLIENT_ID = catRenewal.CLIENT_ID and catRenewal.RENEWAL_OWNER_IND = 1
LEFT JOIN SilverBPBrokerTemp bContact on catRenewal.BROKER_ID = bContact.BROKER_ID
LEFT JOIN SilverBPClientContactFinal f on a.CLIENT_ID = f.CLIENT_ID
LEFT JOIN SilverBPContactFinal f1  on f.contact_id= f1.CONTACT_ID
LEFT JOIN SilverBPClientCustomfiled x1 on x1.CLIENT_ID = a.client_id
LEFT JOIN (select * from SilverBPMasterLinkTEMP where LOB='Benefits' ) i on a.client_ID = i.SourceID
LEFT JOIN SilverBPDUNSRefTEMP j on i.DUNSCurrent = j.D_U_N_S_Number
LEFT JOIN SilverBPPithbooktoCompTEMP k on  i.PrimaryKey = k.PrimaryKey
LEFT JOIN SilverBPPBActiveInvestorFinal l on k.PBID_ToUse = l.CompanyID
LEFT JOIN DIM_BP_ORG org on a.OWNER_OFFICE_ID = org.BRNCH_NUM
"""
)
#display(finalDataDF)

# COMMAND ----------

finalDataDF.count()

# COMMAND ----------

# Do not proceed if there are no records to insert
if (finalDataDF.count() == 0):
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "There are no records to insert: " + sourceSilverFilePath}}})

# COMMAND ----------

# Create a dataframe for record count
sourceRecordCount = sourceBPClientSilverDF.count()
targetRecordCount = finalDataDF.count()
#errorRecordCount = errorDataDF.count()
reconDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceRecordCount,targetRecordCount,sourceBPClientSilverFilePath,BatchId,WorkFlowId)
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

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.NAME,
# MAGIC a.CLIENT_ID,
# MAGIC a.TAX_PAYER_ID,
# MAGIC 'Benefits',
# MAGIC a.WEBSITE,
# MAGIC a.ACCOUNT_NUM,
# MAGIC a.ACTIVE_IND,
# MAGIC CASE  WHEN  z.Client_Name_SourceSystem is null then 'N' else 'Y' END as BP_Confidential,
# MAGIC b.STREET1,
# MAGIC b.STREET2,
# MAGIC b.CITY,
# MAGIC b.POSTAL_CODE,
# MAGIC b.STATE,
# MAGIC b.COUNTRY,
# MAGIC a.NAICS_CODE,
# MAGIC a.SIC_CODE,
# MAGIC f1.FIRST_NAME,
# MAGIC f1.LAST_NAME,
# MAGIC f1.EMAIL_ADDR,
# MAGIC a.CLIENT_STATUS_DESC,
# MAGIC bo.OFFICE_NAME,
# MAGIC a.OWNER_DEPARTMENT_ID,
# MAGIC x1.CUSTOM_FIELD_ID, 
# MAGIC x1.CUSTOM_FIELD_VALUE_ID,
# MAGIC a.DOING_BUSINESS_AS,
# MAGIC a.CLIENT_FUNDING_TYPE_DESC,
# MAGIC a.BUSINESS_TYPE_DESC,
# MAGIC bSales.LAST_NAME + ', ' + bSales.FIRST_NAME as Primary_Sales_Lead,
# MAGIC bService.LAST_NAME + ', ' + bService.FIRST_NAME as Primary_Service_Lead,
# MAGIC bAdmin.LAST_NAME + ', ' + bAdmin.FIRST_NAME as Administrator,
# MAGIC bContact.LAST_NAME + ', ' + bContact.FIRST_NAME as Primary_Contact,
# MAGIC a.BROKER_OF_RECORD,
# MAGIC a.CREATE_DATE,
# MAGIC a.RowBeginDate  as Client_Since,
# MAGIC Public_Private_Indicator,
# MAGIC Non_Profit_Indicator,
# MAGIC NYSE_Ticker,
# MAGIC ASE_Ticker,
# MAGIC NMS_Ticker,
# MAGIC NAS_Ticker,
# MAGIC OTC_Ticker,
# MAGIC Global_Ultimate_D_U_N_S_Number,
# MAGIC Global_Ultimate_Business_Name,
# MAGIC Domestic_Ultimate_D_U_N_S_Number,
# MAGIC Domestic_Ultimate_Business_Name,
# MAGIC Parent_D_U_N_S_Number,
# MAGIC Headquarter_Parent_Business_Name,
# MAGIC PBID_ToUse,
# MAGIC  PEStatus_ToUse,
# MAGIC  D_U_N_S_Number,
# MAGIC clientsourceid ,
# MAGIC pbid_touse ,
# MAGIC InvestorID ,
# MAGIC InvestorName, 
# MAGIC InvestorStatus, 
# MAGIC InvestorType ,
# MAGIC Holding ,
# MAGIC InvestorSince,
# MAGIC  ''  as DB_Match,
# MAGIC  D_U_N_S_Number,
# MAGIC a.CALCULATED_AS_OF_DATE ,
# MAGIC '' as LMDB_ID,
# MAGIC '' as BenefitPoint_ID,
# MAGIC  ''  as ImageRight_ID
# MAGIC  from SilverBPClientTemp a
# MAGIC left join  SilverBPAddressFinal b
# MAGIC on a.client_id = b.client_id 
# MAGIC left join SilverBPClientExclude z
# MAGIC on a.client_id = z.SourceId
# MAGIC left JOIN SilverBPBRokerOfficeTEMP bo on a.OWNER_OFFICE_ID = bo.OFFICE_ID
# MAGIC left join SilverBPBrokerTemp bSales on a.SALES_LEAD_BROKER_ID = bSales.BROKER_ID 
# MAGIC left join SilverBPBrokerTemp bService on a.SERVICE_LEAD_BROKER_ID = bService.BROKER_ID
# MAGIC left join (select * from SilverBPClientAccountTeamsTEMP   where rowenddate = '9999-12-31') catAdmin  on a.CLIENT_ID = catAdmin.CLIENT_ID  and catAdmin.ACCOUNT_TEAM_OWNER_IND = 1
# MAGIC left join SilverBPBrokerTemp bAdmin on catAdmin.BROKER_ID = bAdmin.BROKER_ID
# MAGIC LEFT JOIN (select * from SilverBPClientAccountTeamsTEMP where rowenddate = '9999-12-31')catRenewal on a.CLIENT_ID = catRenewal.CLIENT_ID and catRenewal.RENEWAL_OWNER_IND = 1
# MAGIC LEFT JOIN SilverBPBrokerTemp bContact on catRenewal.BROKER_ID = bContact.BROKER_ID
# MAGIC left join SilverBPClientContactFinal f on a.CLIENT_ID = f.CLIENT_ID
# MAGIC left join SilverBPContactFinal f1  on f.contact_id= f1.CONTACT_ID
# MAGIC left join SilverBPClientCustomfiled x1 on x1.CLIENT_ID = a.client_id
# MAGIC left join (select * from SilverBPMasterLinkTEMP where LOB='Benefits' ) i on a.client_ID = i.SourceID
# MAGIC left join SilverBPDUNSRefTEMP j on i.DUNSCurrent = j.D_U_N_S_Number
# MAGIC left join SilverBPPithbooktoCompTEMP k on  i.PrimaryKey = k.PrimaryKey
# MAGIC left join SilverBPPBActiveInvestorFinal l on k.PBID_ToUse = l.CompanyID