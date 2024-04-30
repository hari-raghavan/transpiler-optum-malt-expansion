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
      col("output_profile_rebate_dtl_id")
        .cast(DecimalType(10, 0))
        .as("output_profile_rebate_dtl_id"),
      coalesce(trim(col("udl_name")),               col("udl_name")).as("udl_name"),
      col("output_profile_id").cast(DecimalType(10, 0)).as("output_profile_id"),
      coalesce(trim(col("rebate_elig_cd")),         col("rebate_elig_cd"))
        .as("rebate_elig_cd"),
      coalesce(col("newline"), lit("""
""")).as("newline")
    )

}
