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
      col("tar_id").cast(DecimalType(10,     0)).as("tar_id"),
      col("tar_dtl_id").cast(DecimalType(10, 0)).as("tar_dtl_id"),
      coalesce(trim(col("tar_name")),        col("tar_name")).as("tar_name"),
      coalesce(trim(col("sort_ind")),        col("sort_ind")).as("sort_ind"),
      coalesce(trim(col("filter_ind")),      col("filter_ind")).as("filter_ind"),
      col("priority").cast(StringType).as("priority"),
      col("tar_dtl_type_cd").cast(StringType).as("tar_dtl_type_cd"),
      col("tar_roa_df_set_id").cast(DecimalType(10, 0)).as("tar_roa_df_set_id"),
      coalesce(trim(col("target_rule")),            col("target_rule")).as("target_rule"),
      coalesce(trim(col("alt_rule")),               col("alt_rule")).as("alt_rule"),
      coalesce(trim(col("rebate_elig_cd")),         col("rebate_elig_cd"))
        .as("rebate_elig_cd"),
      col("eff_dt"),
      col("term_dt"),
      coalesce(col("newline"), lit("""
""")).as("newline")
    )

}
