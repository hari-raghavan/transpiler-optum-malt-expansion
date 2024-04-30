package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Data_Cleansing {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("file_load_cntl_id").cast(DecimalType(16, 0)).as("file_load_cntl_id"),
      coalesce(trim(col("component_ids")),          col("component_ids"))
        .as("component_ids"),
      col("as_of_date"),
      coalesce(trim(col("rxclaim_env_name")), col("rxclaim_env_name"))
        .as("rxclaim_env_name"),
      coalesce(trim(col("carrier")), col("carrier")).as("carrier"),
      coalesce(trim(col("account")), col("account")).as("account"),
      coalesce(trim(col("group")),   col("group")).as("group"),
      col("component_type_cd").cast(StringType).as("component_type_cd"),
      coalesce(trim(col("file_name_w")),   col("file_name_w")).as("file_name_w"),
      coalesce(trim(col("published_ind")), col("published_ind"))
        .as("published_ind"),
      col("alt_run_id").cast(DecimalType(16,  0)).as("alt_run_id"),
      coalesce(trim(col("report_file_name")), col("report_file_name"))
        .as("report_file_name"),
      coalesce(col("newline"), lit("""
""")).as("newline")
    )

}
