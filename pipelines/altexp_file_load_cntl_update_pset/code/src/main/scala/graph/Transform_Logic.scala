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

object Transform_Logic {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("file_load_cntl_id").cast(DecimalType(16, 0)).as("file_load_cntl_id"),
      col("component_ids"),
      col("as_of_date"),
      col("rxclaim_env_name"),
      col("carrier"),
      col("account"),
      col("group"),
      col("component_type_cd").cast(StringType).as("component_type_cd"),
      col("file_name_w"),
      col("published_ind"),
      col("alt_run_id").cast(DecimalType(16, 0)).as("alt_run_id"),
      col("report_file_name"),
      coalesce(col("newline"), lit("""
""")).as("newline")
    )

}
