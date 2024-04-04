package graph

import io.prophecy.libs._
import config.Context
import udfs.UDFs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Write_Data_at_CAG_level {
  def apply(context: Context, in: DataFrame): Unit = {
    val spark = context.spark
    val Config = context.config
    val withFileDF = in.withColumn("fileName", col("data_path"))
    withFileDF.breakAndWriteDataFrameForOutputFile(List("carrier", "account", "group", "ndc11", "gpi14", "status_cd", "eff_dt", "term_dt", "inactive_dt", "msc", "drug_name", "rx_otc", "desi", "roa_cd", "dosage_form_cd", "prod_strength", "repack_cd", "prod_short_desc", "gpi14_desc", "gpi8_desc", "newline", "run_eff_dt", "data_path"), "fileName", "csv", Some("\\x01"))
  }

}
