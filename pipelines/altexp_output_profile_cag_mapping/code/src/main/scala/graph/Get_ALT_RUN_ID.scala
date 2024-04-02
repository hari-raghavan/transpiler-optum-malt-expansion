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

object Get_ALT_RUN_ID {

  def apply(context: Context, left: DataFrame, right: DataFrame): DataFrame =
    left
      .as("left")
      .join(right.as("right"), lit(true), "left_outer")
      .select(
        col("right.nextval").cast(DecimalType(16, 0)).as("alt_run_id"),
        col("left.output_profile_id").as("output_profile_id"),
        col("left.job_ids").as("job_ids"),
        col("left.err_msg").as("err_msg"),
        col("left.as_of_dt").as("as_of_dt")
      )

}
