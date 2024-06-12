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
    withFileDF.breakAndWriteDataFrameForOutputFile(List(
        "formulary_name",
        "formulary_cd",
        "carrier",
        "account",
        "group",
        "last_exp_dt",
        "ndc11",
        "formulary_tier",
        "formulary_status",
        "pa_reqd_ind",
        "specialty_ind",
        "step_therapy_ind",
        "formulary_tier_desc",
        "formulary_status_desc",
        "pa_type_cd",
        "step_therapy_type_cd",
        "step_therapy_group_name",
        "step_therapy_step_number",
        "newline",
        "run_eff_dt",
        "customer_name",
        "data_path"
    ), "fileName", "csv", Some("\\x01"))
  }

}
