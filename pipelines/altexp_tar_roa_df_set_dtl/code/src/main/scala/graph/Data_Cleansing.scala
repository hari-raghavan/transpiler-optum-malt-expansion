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
      col("tar_roa_df_set_dtl_id")
        .cast(DecimalType(10, 0))
        .as("tar_roa_df_set_dtl_id"),
      col("tar_roa_df_set_id").cast(DecimalType(10, 0)).as("tar_roa_df_set_id"),
      coalesce(trim(col("target_roa_cd")),          col("target_roa_cd"))
        .as("target_roa_cd"),
      coalesce(trim(col("target_dosage_form_cd")), col("target_dosage_form_cd"))
        .as("target_dosage_form_cd"),
      coalesce(trim(col("alt_roa_cd")),         col("alt_roa_cd")).as("alt_roa_cd"),
      coalesce(trim(col("alt_dosage_form_cd")), col("alt_dosage_form_cd"))
        .as("alt_dosage_form_cd"),
      col("priority").cast(StringType).as("priority"),
      coalesce(col("newline"), lit("""
""")).as("newline")
    )

}
