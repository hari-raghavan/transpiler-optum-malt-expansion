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
      col("alt_run_id").cast(DecimalType(16, 0)).as("alt_run_id"),
      col("job_run_id").cast(DecimalType(16, 0)).as("job_run_id"),
      col("rec_crt_ts"),
      col("rec_crt_user_id"),
      coalesce(col("newline"), lit("""
""")).as("newline")
    )

}
