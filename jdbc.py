# Databricks notebook source
import time 
from datetime import date

#
# Transfer a source table over JDBC to a target Table in Databricks
#  (1) CoE is a small team... must build to SCALE 
#  (2) Self-service oriented
#
def transferTable(connection, tablePair):
  timer_start = time.time()
  df = (
      spark.read.format("jdbc")
        .option("driver", connection.driver)
        .option("url", connection.url)
        .option("dbtable", tablePair.source.tablename)
        .load() 
  )
  df.write.mode("overwrite").saveAsTable(tablePair.target.tablename)
  timer_end = time.time() 
  return { 
    'row_count': df.count(), 
    'time_seconds': f"{timer_end - timer_start:0.4f}", 
    'source_tablename': tablePair.source.tablename, 
    'databricks_tablename': tablePair.target.tablename,
    'jdbc_source':  str(url[:95] + "..."),
    'refresh_date': str(date.today())
  }


# COMMAND ----------

#helper function
class TableMessage():
  def __init__(self, tablename): 
    self.tablename=tablename


# COMMAND ----------

#helper function
class TransferTableMessage(): 
  def __init__(self, source, target):
    self.source=source
    self.target=target

# COMMAND ----------

#helper function
#TODO Enter your JDBC Url and JDBC Driver here. Default is "Spark" driver
#url = 
#driver = 
class JDBCConnectionMessage():
  def __init__(self, driver="com.simba.spark.jdbc.Driver", user=None, password=None, urlParams=None):
      self.url=url
      self.driver=driver
      self.user=user
      self.password=password


# COMMAND ----------

#lightweight implementation of democratized data 
#TODO Enter your source and target on the command below
table = TransferTableMessage(source=TableMessage("provider_db.nppes_provider"), target=TableMessage("hls_provider_db_source.nppes_provider"))
connection = JDBCConnectionMessage()
result = transferTable(connection, table)

#considerations to scale... 
# 1. Database specific parameters e.g. SQL Server ISOLATION_LEVEL = READ UNCOMMITTED (do not block source system properties)
# 2. Optimize individual table pulls with parallelism 


# COMMAND ----------

spark.createDataFrame([result]).write.mode("append").saveAsTable("hls_ingest_audit.jdbc_ingests")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hls_ingest_audit.jdbc_ingests
# MAGIC where source_tablename='provider_db.nppes_provider';
# MAGIC --(1) Data Quality/Trust with transparency
# MAGIC --(2) Data Quality Extensions - AI/ML, Great Expectations
