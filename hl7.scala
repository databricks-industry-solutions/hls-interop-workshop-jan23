// Databricks notebook source
// MAGIC %md
// MAGIC ## Generate HL7 Stream

// COMMAND ----------

/* For the workshop we used synthetic generator code not publicly available yet
%scala
import com.databricks.labs.hl7gen._
import org.scalacheck._
import org.apache.spark.sql.types._

DemoGen.sample.replace("\r", "\n")
*/ 

// COMMAND ----------

/* Instead, can download files diretly to director below from existing synthetic datasets. Eg. https://synthea.mitre.org/downloads
%scala
(1 to 1000).map( _ => DemoGen.sample)
              .toDF()
              .write
              .option("header", "false")
              .mode("append")
              .csv("dbfs:/dbfs/user/hive/warehouse/hls_cms_source.db/raw_files/hl7/")
*/


// COMMAND ----------

// MAGIC %md
// MAGIC ## Read Stream of HL7v2 Files

// COMMAND ----------

val rawFileSchema = StructType(Array(StructField("line", StringType, true))) 
//Extract records in near real time from cloud directory
val hl7EventStream = (
  spark
    .readStream
    .schema(rawFileSchema)            // Sets the schema to be raw text
    .option("maxFilesPerTrigger", 10)  // Treat a sequence of files as a stream by picking one file at a time
    .option("lineSep", "\n")          // carriage returns indicate an HL7 segment seperator. Only treat newlines as record seperators
    .format("text")
    .load("dbfs:/dbfs/user/hive/warehouse/hls_cms_source.db/raw_files/hl7/")
  )


// COMMAND ----------

// MAGIC %md
// MAGIC ## Parse HL7v2 for Relevant Attributes

// COMMAND ----------

import com.databricks.labs.smolder.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession

//HL7 Parsed Output Schema
object targetHL7Schema {
  val schema = StructType(Array(
      StructField("patient_id", StringType, true)
      ,StructField("admit_location", StringType, true)
      ,StructField("admit_diagnosis", StringType, true)
  )) 
  //Here defining a simple transformation using Smoler to filter and parse HL7 messages
  // 1. Filter for interested admission records {ADT_A01: new admission, ADT_A02: transfer, ADT_A05 = pre-admit}
  // 2. Select patient_id, admit_location, admit_diagnosis
  // 3. Return this new dataframe
  def transform(df: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
      Option(df) match{
        case Some(df) => df
          .select(parse_hl7_message(col("line")).alias("parsed_hl7"))
          .withColumn("patient_id", segment_field("PID", 0, col("parsed_hl7.segments")))
          .withColumn("admit_location", segment_field("EVN", 6, col("parsed_hl7.segments")))
          .withColumn("admit_diagnosis", subfield(segment_field("DG1", 2, col("parsed_hl7.segments")), 0))
          .select(col("patient_id"), col("admit_location"), col("admit_diagnosis"))
        case _ => spark.createDataFrame(spark.sparkContext.emptyRDD[Row], targetHL7Schema.schema)
      }
  }
}


// COMMAND ----------

val parsedHl7Stream = (
  targetHL7Schema
    .transform(hl7EventStream, spark) //Returns HL7 Patient ID, Location, and Admit Diagnosis
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "dbfs:/dbfs/user/hive/warehouse/hls_cms_source.db/raw_files/_hl7_checkpoint") 
    .toTable("hls_synthetic_claims.parsed_hl7")
)

// COMMAND ----------

// MAGIC %sql 
// MAGIC select admit_diagnosis, count(1)
// MAGIC from hls_synthetic_claims.parsed_hl7
// MAGIC group by admit_diagnosis
// MAGIC order by count(1) desc
// MAGIC limit 20
