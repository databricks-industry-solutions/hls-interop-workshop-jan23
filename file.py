# Databricks notebook source
# MAGIC %sh
# MAGIC mkdir -p /dbfs/user/hive/warehouse/hls_cms_source.db/raw_files/synthea_mass
# MAGIC wget  https://synthetichealth.github.io/synthea-sample-data/downloads/synthea_sample_data_ccda_sep2019.zip -O ./synthea_sample_data_ccda_sep2019.zip 
# MAGIC unzip ./synthea_sample_data_ccda_sep2019.zip -d /dbfs/user/hive/warehouse/hls_cms_source.db/raw_files/synthea_mass/
# MAGIC #Jason Walonoski, Mark Kramer, Joseph Nichols, Andre Quina, Chris Moesel, Dylan Hall, Carlton Duffett, Kudakwashe Dube, Thomas Gallagher, Scott McLachlan, #Synthea: An approach, method, and software mechanism for generating synthetic patients and the synthetic electronic health care record, Journal of the #American Medical Informatics Association, Volume 25, Issue 3, March 2018, Pages 230–238, https://doi.org/10.1093/jamia/ocx079

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Batch Load Files

# COMMAND ----------

spark.conf.set("spark.sql.caseSensitive", "true")
df = (
  spark.read.format('xml')
   .option("rowTag", "ClinicalDocument")
  .load("dbfs:/user/hive/warehouse/hls_cms_source.db/raw_files/synthea_mass/ccda/")
)

df.write.format("parquet").mode("overwrite").saveAsTable("hls_synthetic_claims.ccda_claims_synthea_mass")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Normalized Tables for Common Use Cases

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists hls_synthetic_claims.claims_synthea_mass_medications;
# MAGIC create table hls_synthetic_claims.claims_synthea_mass_medications
# MAGIC as
# MAGIC  select  distinct recordTarget.patientRole.addr.city,
# MAGIC  recordTarget.patientRole.patient.name.given as first_name,
# MAGIC   recordTarget.patientRole.patient.name.family as last_name,
# MAGIC  entry_array.substanceAdministration.consumable.manufacturedProduct.manufacturedMaterial.code._displayName as medication_name
# MAGIC   from 
# MAGIC (
# MAGIC   select *, explode(component_array.section.entry) as entry_array 
# MAGIC   from 
# MAGIC      (select *, explode(component.structuredBody.component) as component_array from hls_synthetic_claims.ccda_claims_synthea_mass) foo
# MAGIC   where component_array.section.title='Medications'
# MAGIC ) medications

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hls_synthetic_claims.claims_synthea_mass_medications

# COMMAND ----------

# MAGIC %sql 
# MAGIC select medication_name, count(1)
# MAGIC from hls_synthetic_claims.claims_synthea_mass_medications
# MAGIC group by medication_name
