# Databricks notebook source
# MAGIC %sh
# MAGIC #install dbignite python package from Github
# MAGIC pip install git+https://github.com/databrickslabs/dbignite

# COMMAND ----------

from dbignite.data_model import *
#directory location where FHIR bundle files exist. Sample datasets can downloaded from https://synthetichealth.github.io/synthea/ 
fhir_file_location=
files=dbutils.fs.ls(fhir_file_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ![logo](https://www.hl7.org/fhir/assets/images/fhir-logo-www.png)

# COMMAND ----------

#FhirBundle Data Model
fhir_model=FhirBundles(fhir_file_location)
#Omop Data Model
cdm_model=OmopCdm("hls_synthetic_claims")

#Transform from FHIR to OMOP
fhir2omop_transformer=FhirBundlesToCdm(spark)
fhir2omop_transformer.transform(fhir_model,cdm_model)

# COMMAND ----------

# MAGIC %md
# MAGIC ![logo](https://ohdsi.github.io/TheBookOfOhdsi/images/CommonDataModel/cdmDiagram.png)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from hls_synthetic_claims.condition limit 100;

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table if not exists hls_synthetic_claims.feature_engineering_fhir
# MAGIC as
# MAGIC select person_id, max(diabetes_ind) as diabetes_ind, max(hypertension_ind) as hypertension_ind, max(drug_ind) as drug_ind
# MAGIC From
# MAGIC (
# MAGIC select  person_id, condition_status, condition_code,
# MAGIC   case when condition_code in (44054006, 427089005, 127013003, 422034002) then 1 else 0 end as diabetes_ind,
# MAGIC   case when condition_code in (59621000) then 1 else 0 end as hypertension_ind,
# MAGIC   case when condition_code in (55680006, 361055000, 449868002) then 1 else 0 end as drug_ind
# MAGIC from hls_synthetic_claims.condition
# MAGIC where condition_code in (44054006, 427089005, 127013003, 422034002,  59621000, 55680006, 361055000, 449868002) 
# MAGIC ) foo
# MAGIC group by person_id
# MAGIC ;
# MAGIC select * from hls_synthetic_claims.feature_engineering_fhir limit 100 ;
