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

object Join_With_Input_Surrogate_Key_File__Join {

  def apply(context: Context, left: DataFrame, right: DataFrame): DataFrame =
    left
      .as("left")
      .join(right.as("right"),
            (col("left.output_profile_id") === col("right.output_profile_id"))
              .and(col("left.newline") === col("right.newline")),
            "inner"
      )
      .select(
        coalesce(col("right.alt_run_id").cast(DecimalType(16, 0)), lit(-1))
          .as("alt_run_id"),
        col("left.output_profile_id")
          .cast(DecimalType(10, 0))
          .as("output_profile_id"),
        col("right.run_start_ts").as("run_start_ts"),
        col("right.run_complete_ts").as("run_complete_ts"),
        col("right.alt_run_status_cd").cast(StringType).as("alt_run_status_cd"),
        col("right.published_ind").as("published_ind"),
        col("right.rec_crt_ts").as("rec_crt_ts"),
        col("right.rec_crt_user_id").as("rec_crt_user_id"),
        col("right.rec_last_upd_ts").as("rec_last_upd_ts"),
        col("right.rec_last_upd_user_id").as("rec_last_upd_user_id"),
        col("right.run_eff_dt").as("run_eff_dt"),
        col("right.as_of_dt").as("as_of_dt"),
        coalesce(col("left.newline").cast(StringType), lit("""
""")).as("newline")
      )

}
