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

object Create_ALT_RUN_ALT_RUN_JOB_DETAILS_Load_Ready_FileReformat_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(coalesce(col("alt_run_id").cast(DecimalType(16, 0)), lit(-1))
                .as("alt_run_id"),
              col("output_profile_id"),
              col("job_ids"),
              col("err_msg"),
              col("as_of_dt")
    )

}
