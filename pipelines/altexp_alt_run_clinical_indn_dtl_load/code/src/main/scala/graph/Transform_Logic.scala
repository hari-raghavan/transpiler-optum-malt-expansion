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
      coalesce(col("alt_run_id").cast(DecimalType(16, 0)), lit(-1))
        .as("alt_run_id"),
      coalesce(col("alt_run_target_dtl_id").cast(DecimalType(16, 0)), lit(-1))
        .as("alt_run_target_dtl_id"),
      col("formulary_name"),
      col("target_ndc"),
      col("tal_assoc_name"),
      col("rank").cast(StringType).as("rank"),
      col("rec_crt_ts"),
      col("rec_crt_user_id"),
      col("clinical_indn_desc"),
      coalesce(col("newline"), lit("""
""")).as("newline")
    )

}
