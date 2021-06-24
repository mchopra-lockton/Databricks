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
# DimEntity file
sourceSilverClientFilePath = SilverContainerPath + "Client/Nexsure/DimEntity/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "DimEntity_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
# DimClientName file
sourceSilverClientNameFilePath = SilverContainerPath + "Client/Nexsure/DimClientName/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "DimClientName_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
# DimClientNaicsSic file
sourceSilverClientNaicsSicFilePath = SilverContainerPath + "Client/Nexsure/DimClientNaicsSic/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "DimClientNaicsSic_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
# FactAssignment file
sourceSilverFactAssignmentFilePath = SilverContainerPath + "Person/Nexsure/FactAssignment/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "FactAssignment_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
# MasterLinkNoLM file
sourceSilverMasterLinkFilePath = SilverContainerPath + "Client/MDS2/MasterLinkNoLM/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "MasterLinkNoLM_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
# DUNS_Reference file
sourceSilverDUNSReferenceFilePath = SilverContainerPath + "Client/MDS2/DUNS_Reference/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "DUNS_Reference_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
# PB_ClientToCompany file
sourceSilverPBClientFilePath = SilverContainerPath + "Client/Pitchbook/PB_ClientToCompany/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "PB_ClientToCompany_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
# PB_Reference_CompanyActiveInvestorRelation file
sourceSilverPBCompanyFilePath = SilverContainerPath + "Client/Pitchbook/PB_Reference_CompanyActiveInvestorRelation/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "PB_Reference_CompanyActiveInvestorRelation_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"
sourceSilverClientExcludeFilePath = SilverContainerPath + "Business/MDS2/ClientExclude/" +now.strftime("%Y") + "/" + now.strftime("%m") + "/" + "ClientExclude_" + now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + ".parquet"

# COMMAND ----------

# Temporary cell to run manually - DELETE
now = datetime.now() 
date_time = now.strftime("%Y%m%dT%H%M%S")
GoldDimTableName = "DIM_NX_CLIENT"
badRecordsPath = badRecordsRootPath + GoldDimTableName + "/"
recordCountFilePath = badRecordsPath + date_time + "/" + "RecordCount"
BatchId = "1afc2b6c-d987-48cc-ae8c-a7f41ea27249"
WorkFlowId ="8fc2895d-de32-4bf4-a531-82f0c6774221"
sourceSilverClientFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Client/Nexsure/DimEntity/" + yymmManual + "/DimEntity_" + yyyymmddManual + ".parquet"
sourceSilverClientNameFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Client/Nexsure/DimClientName/" + yymmManual + "/DimClientName_" + yyyymmddManual + ".parquet"
sourceSilverClientNaicsSicFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Client/Nexsure/DimClientNaicsSic/" + yymmManual + "/DimClientNaicsSic_" + yyyymmddManual + ".parquet"
sourceSilverFactAssignmentFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Person/Nexsure/FactAssignment/" + yymmManual + "/FactAssignment_" + yyyymmddManual + ".parquet"
sourceSilverMasterLinkFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Client/MDS2/MasterLinkNoLM/" + yymmManual + "/MasterLinkNoLM_" + yyyymmddManual + ".parquet"
sourceSilverDUNSReferenceFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Client/MDS2/DUNS_Reference/" + yymmManual + "/DUNS_Reference_" + yyyymmddManual + ".parquet"
sourceSilverPBClientFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Client/Pitchbook/PB_ClientToCompany/" + yymmManual + "/PB_ClientToCompany_" + yyyymmddManual + ".parquet"
sourceSilverPBCompanyFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Client/Pitchbook/PB_Reference_CompanyActiveInvestorRelation/" + yymmManual + "/PB_Reference_CompanyActiveInvestorRelation_" + yyyymmddManual + ".parquet"
sourceSilverClientExcludeFilePath = "abfss://c360silver@dlsldpdev01v8nkg988.dfs.core.windows.net/Business/MDS2/ClientExclude/" + yymmManual + "/ClientExclude_" + yyyymmddManual + ".parquet"

# COMMAND ----------

# MAGIC %scala
# MAGIC // Temporary cell to run manually - DELETE
# MAGIC lazy val GoldDimTableName = "Dim_NX_CLIENT"  

# COMMAND ----------

# Do not proceed if any of the parameters are missing
if (GoldDimTableName == "" or sourceSilverClientFilePath == ""):
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Input parameters are missing"}}})

# COMMAND ----------

# Read source file
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
try:
   sourceClientDF = spark.read.parquet(sourceSilverClientFilePath)
   #creating dataframe for client only from entity
   sourceClientOnlyDF=sourceClientDF.filter((sourceClientDF.EntityClassID=='6'))  
except:
    # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceSilverClientFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "Error reading the file: " + sourceSilverFilePath}}})  

# COMMAND ----------

# Read other source file ClientName
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
try:
  sourceClientNameDF = spark.read.parquet(sourceSilverClientNameFilePath)
except:
    # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceSilverClientNameFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  

# COMMAND ----------

# Read other source file NexsureNicsSic
try:
  sourceNAICSDF = spark.read.parquet(sourceSilverClientNaicsSicFilePath)  
  # Get max NAICS code
  sourceNAICSDF.createOrReplaceTempView("vwNAICSCode")
  sourceNAICSFinalDF = spark.sql(
  f""" 
  select * from 
  ( select *, row_number() over(PARTITION BY  ClientID order by ClientNaicsSickey desc) client_rank from  vwNAICSCode ) 
  where client_rank=1
  """
  )  
except:
    # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceSilverClientNaicsSicFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  

# COMMAND ----------

# Read other source file FactAssigment
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
try:
  sourceFactAssignmentDF = spark.read.parquet(sourceSilverFactAssignmentFilePath)  
# Get max Assignment 
  sourceFactAssignmentDF.createOrReplaceTempView("vwFactAssignment")
  sourceFactAssignmentFinalDF = spark.sql(
  f""" 
  select * from 
  (select PBR.*, row_number() over(PARTITION BY  ClientKey order by AssignmentStartDate desc) client_rank 
  from vwFactAssignment PBR) t 
  where client_rank=1
  """
  )  
except:
    # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceSilverFactAssignmentFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  

# COMMAND ----------

# Read other source file MasterLink data
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
try:
  sourceMasterLinkAllDF = spark.read.parquet(sourceSilverMasterLinkFilePath)
  # Get max Client Rank for PBCompanyActiveInvestor
  sourceMasterLinkAllDF.createOrReplaceTempView("vwMasterLinkAll")
  sourceMasterLinkDF = spark.sql(
  f""" 
  select * from vwMasterLinkAll where SourceSystem = 'Nexsure' 
  """
  )    
except:
    # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceSilverMasterLinkFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  

# COMMAND ----------

# Read other source file DUNS Reference Data
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
try:
  sourceDUNSReferenceDF = spark.read.parquet(sourceSilverDUNSReferenceFilePath)
except:
    # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceSilverDUNSReferenceFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  

# COMMAND ----------

# Read other source file PitchBook CompanyData
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
try:
  sourcePBClientToCompanyDF = spark.read.parquet(sourceSilverPBClientFilePath)
except:
    # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceSilverPBClientFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  

# COMMAND ----------

# Read other source file PitchBookClienttoSilver data
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
try:
  sourcePBCompanyInvestorDF = spark.read.parquet(sourceSilverPBCompanyFilePath)
  # Get max Client Rank for PBCompanyActiveInvestor
  sourcePBCompanyInvestorDF.createOrReplaceTempView("vwPBCompanyActiveInvestor")
  sourcePBCompanyInvestorFinalDF = spark.sql(
  f""" 
  select * from 
  (select *, row_number() over(PARTITION BY  CompanyID order by InvestorID asc) client_rank from vwPBCompanyActiveInvestor where Holding <> 'Minority') 
  where client_rank=1
  """
  )  
except:
    # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceSilverPBCompanyFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  

# COMMAND ----------

# Read other source file ClientExclude Confidential
spark.sql("set spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED")
try:
  sourceClientExcludeDF = spark.read.parquet(sourceSilverPBClientFilePath)
except:
    # Log the error message
  errorDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceSilverClientExcludeFilePath,BatchId,WorkFlowId,"Error reading the file")
  ],["TableName","ETL_CREATED_DT","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID","Message"])
  # Write the recon record to SQL DB
  errorDF.write.jdbc(url=Url, table=reconTable, mode="append")  

# COMMAND ----------

# Create temporary view for client only from entity
sourceClientOnlyDF.createOrReplaceTempView("vwClientOnly")
# Create temporary view for MasterLink
sourceMasterLinkDF.createOrReplaceTempView("vwMasterLink")
# Create temporary view for DUNS Reference
sourceDUNSReferenceDF.createOrReplaceTempView("vwDUNSReference")
# Create temporary view for NAICSCode
sourceNAICSFinalDF.createOrReplaceTempView("vwNAICSCodeFinal")
# Create temporary view for PBClientToCompany
sourcePBClientToCompanyDF.createOrReplaceTempView("vwPBClientToCompany")
# Create temporary view for PBCompanyActiveInvestor
sourcePBCompanyInvestorFinalDF.createOrReplaceTempView("vwPBCompanyActiveInvestorFinal")
# Create temporary view for FactAssignment
sourceFactAssignmentFinalDF.createOrReplaceTempView("vwFactAssignmentFinal")
# Create temporary view for ClientExclude Confidential
sourceClientExcludeDF.createOrReplaceTempView("vwClientExclude")

# COMMAND ----------

dummyDataDF = spark.sql(
f""" 
SELECT
-99999 as NX_CLIENT_KEY
,-1 as MSTR_CLIENT_KEY
,-1 as NX_CLIENT_ID
,-1 As CLIENT_NAME
,-1 As CLIENT_TYP
,-1 As FEIN
,-1 As ADDR_LINE_1
,-1 As ADDR_LINE_2
,-1 As CITY
,-1 As STATE
,-1 As ZIP
,-1 As COUNTRY
,-1 As PRIM_NAICS_CD
,-1 As PRIM_NAICS_DESC
,-1 As PRIM_PNC_EFF_DT
,-1 As PNC_RELNSHIP
,-1 As PRIM_PNC_PRODCR
,-1 As PRIM_PNC_SERV_LEAD
,-1 As CLNT_SNCE_FRM
,-1 As CLNT_SNCE_TO
,-1 As PRIM_CONTCT_NAME
,-1 As PRIM_CONTCT_EMAIL
,-1 As PRIM_CONTCT_PHN
,-1 As NX_LST_MOD_DT
,-1 As URL
,-1 As ACTV_FLG
,-1 As CARIER_CLAS
,-1 As DB_SRC_KEY
,-1 As SRC_AUDT_KEY
,'{ BatchId }' AS ETL_BATCH_ID
,'{ WorkFlowId }' AS ETL_WRKFLW_ID
,current_timestamp() AS ETL_CREATED_DT
,current_timestamp() AS ETL_UPDATED_DT
FROM vwClientOnly e LIMIT 1
"""
)

# COMMAND ----------

finalDataDF = spark.sql(
f""" 
SELECT
EntityKey AS  NX_CLIENT_KEY 
,1 AS  MSTR_CLIENT_KEY 
,EntityID AS  NX_CLIENT_ID 
,EntityName AS  CLIENT_NAME 
,EntityType AS  CLIENT_TYP 
,FEIN AS  FEIN 
,EntityAddressLine1 AS  ADDR_LINE_1 
,EntityAddressLine2 AS  ADDR_LINE_2 
,EntityAddressCity AS  CITY 
,EntityAddressState AS  STATE 
,EntityAddressZip AS  ZIP 
,EntityAddressCountry AS  COUNTRY 
,coalesce(NaicsCode,0) AS  PRIM_NAICS_CD 
,coalesce(NaicsCodeDesc,'None') AS  PRIM_NAICS_DESC 
,'' AS  PRIM_PNC_EFF_DT 
,'' AS  PNC_RELNSHIP 
,'' AS  PRIM_PNC_PRODCR 
,'' AS  PRIM_PNC_SERV_LEAD 
,ClientBeginDate AS  CLNT_SNCE_FRM 
,'' AS  CLNT_SNCE_TO 
,PrimaryContactName AS  PRIM_CONTCT_NAME 
,PrimaryContactEmail AS  PRIM_CONTCT_EMAIL 
,PrimaryContactPhone AS  PRIM_CONTCT_PHN 
,LastModified AS  NX_LST_MOD_DT 
,EntityClass AS  CARIER_CLAS 
,EntityURL AS  URL 
,EntityActiveFlag AS  ACTV_FLG 
,client.DBSourceKey AS  DB_SRC_KEY 
,client.AuditKey AS  SRC_AUDT_KEY 
,'' AS  ACCT_NAME 
,'P&C' AS  SRC 
,'' AS  ACCT_INFORMATION 
,'' AS  SEC_NAICS_CODE 
,'' AS  SEC_NAICS_DESC 
,'' AS  PRIM_SIC_CODE 
,'' AS  CNTCT_FIRST_NAME 
,'' AS  CNTCT_LST_NAME 
,'' AS  CNTCT_EMAIL 
,'' AS  IS_PROSPCT 
,'' AS  OFFICE 
,'' AS  DEPARTMENT 
,'' AS  AGNCY_CCD 
,'' AS  BRNCH_CD 
,'' AS  DOING_BUSS_AS 
,'' AS  CLNT_FNDG_TYP_DESC 
,'' AS  BUSS_TYP 
,'' AS  PRIM_SALES_LEAD 
,'' AS  PRIM_SERV_LEAD 
,'' AS  ADMINISTRATOR 
,'' AS  TAM_CLNT_ID 
,'' AS  GICS_CD 
,'' AS  GICS_DESC 
,'' AS  PRIM_BP_EFF_DT 
,'' AS  PRACT_GRPS 
,Public_Private_Indicator AS  PUBLIC_PRIV_IND 
,Non_Profit_Indicator AS  NON_PRFIT 
,Sales_Volume_US_Dollars_ AS  ANNL_REVNUE 
,Employees_Total AS  NUM_OF_EMP 
,'' AS  ANN_PAYROLL 
,NYSE_Ticker AS  NYSE_TICKER 
,ASE_Ticker AS  ASE_TICKER 
,NMS_Ticker AS  NMS_TICKER 
,NAS_Ticker AS  NAS_TICKER 
,OTC_Ticker AS  OTC_TICKER 
,Global_Ultimate_D_U_N_S_Number AS  GLOB_ULTMT_DUNS_NUM 
,Global_Ultimate_Business_Name AS  GLOBL_ULTMT_BUSS_NAME 
,Domestic_Ultimate_D_U_N_S_Number AS  DOMSTIC_ULTMT_DUNS_NUM 
,Domestic_Ultimate_Business_Name AS  DOMSTIC_ULTMT_BUSS_NAME 
,Parent_D_U_N_S_Number AS  PRNT_DUNS_NUM 
,Headquarter_Parent_Business_Name AS  HEADQRTER_PRNT_BUSS_NAME 
,Primary_NAICS_1_1_Description AS  BUSS_DESC 
,'' AS  PTCHBK_MATCH 
,ClientSourceID AS  PTCHBK_CLNTSRCID 
,PBID_ToUse AS  PTCHBK_PBID_TOUSE 
,InvestorID  AS  PTCHBK_INVESTR_ID 
,InvestorName  AS  PTCHBK_INVESTR_NAME 
,InvestorStatus  AS  PTCHBK_INVESTR_STATUS 
,InvestorType  AS  PTCHBK_INVESTR_TYPE 
,Holding  AS  PTCHBK_HOLDNG 
,InvestorSince AS  PTCHBK_INVSTOR_SINCE 
,'' AS  DB_MATCH 
,D_U_N_S_Number AS  DUNS_NUM 
,'' AS  IS_ECP 
,'' AS  DT_ECP_FORM_LST_SNT_TO_CLNT 
,'' AS  DT_ECP_FORM_LST_RENWD 
,'' AS  CRETD_BY 
,'' AS  LAST_MOD_BY 
,'' AS  LMDB_ID 
,'' AS  PEOPLESOFT_ID 
,'' AS  BENEFITPOINT_ID 
,'' AS  IMAGERIGHT_ID 
,'' AS  DB_SYNC_RESTRICTED 
, CASE WHEN Client_Name_SourceSystem is null then 'N' else 'Y' END as CONFDL_ACCT
,'{ BatchId }' AS ETL_BATCH_ID
,'{ WorkFlowId }' AS ETL_WRKFLW_ID
,current_timestamp() AS ETL_CREATED_DT
,current_timestamp() AS ETL_UPDATED_DT
FROM vwClientOnly client
LEFT JOIN vwFactAssignmentFinal assign on client.EntityKey = assign.ClientKey
LEFT JOIN vwNAICSCodeFinal naics on client.EntityKey = naics.ClientKey
LEFT JOIN vwMasterLink masterlink on client.EntityID = masterlink.SourceID
LEFT JOIN vwDUNSReference duns on masterlink.DUNSCurrent = duns.D_U_N_S_Number
LEFT JOIN vwPBClientToCompany pbclient on masterlink.PrimaryKey = pbclient.PrimaryKey
LEFT JOIN vwPBCompanyActiveInvestorFinal pbcompany on pbclient.PBID_ToUse = pbcompany.CompanyID
LEFT JOIN vwClientExclude clexcl on client.EntityId = clexcl.SourceID
"""
)
finalDataDF.count()

# COMMAND ----------

# Do not proceed if there are no records to insert
if (finalDataDF.count() == 0):
  dbutils.notebook.exit({"exceptVariables": {"errorCode": {"value": "There are no records to insert: " + sourceSilverFilePath}}})

# COMMAND ----------

# Create a dataframe for record count
sourceRecordCount = sourceClientOnlyDF.count()
targetRecordCount = finalDataDF.count()
#errorRecordCount = errorDataDF.count()
reconDF = spark.createDataFrame([
    (GoldDimTableName,now,sourceRecordCount,targetRecordCount,sourceSilverClientFilePath,BatchId,WorkFlowId)
  ],["TableName","ETL_CREATED_DT","SourceRecordCount","TargetRecordCount","Filename","ETL_BATCH_ID","ETL_WRKFLW_ID"])

# Write the recon record to SQL DB
reconDF.write.jdbc(url=Url, table=reconTable, mode="append")

# COMMAND ----------

# MAGIC %scala
# MAGIC // Truncate Fact table and Delete data from Dimension table
# MAGIC lazy val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
# MAGIC lazy val stmt = connection.createStatement()
# MAGIC //lazy val sql_truncate = "truncate table " + finalTableSchema + "." + "FCT_NX_INV_LINE_ITEM_TRANS"
# MAGIC //stmt.execute(sql_truncate)
# MAGIC lazy val sql = "exec " + finalTableSchema + ".[DropAndCreateFKContraints] @GoldTableName = '" + GoldDimTableName + "'"
# MAGIC stmt.execute(sql)
# MAGIC connection.close()

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