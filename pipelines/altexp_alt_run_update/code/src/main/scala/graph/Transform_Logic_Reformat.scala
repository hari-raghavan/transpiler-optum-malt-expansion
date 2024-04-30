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

object Transform_Logic_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      coalesce(col("alt_run_id").cast(DecimalType(16, 0)), lit(-1))
        .as("alt_run_id"),
      col("output_profile_id").cast(DecimalType(10, 0)).as("output_profile_id"),
      col("run_start_ts"),
      col("run_complete_ts"),
      col("alt_run_status_cd").cast(StringType).as("alt_run_status_cd"),
      col("published_ind"),
      col("rec_crt_ts"),
      col("rec_crt_user_id"),
      col("rec_last_upd_ts"),
      col("rec_last_upd_user_id"),
      col("run_eff_dt"),
      col("as_of_dt"),
      coalesce(col("newline"), lit("""
""")).as("newline")
    )

}
