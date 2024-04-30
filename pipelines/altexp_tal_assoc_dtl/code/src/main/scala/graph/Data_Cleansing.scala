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
      col("tal_assoc_dtl_id").cast(DecimalType(10, 0)).as("tal_assoc_dtl_id"),
      col("tal_assoc_id").cast(DecimalType(10,     0)).as("tal_assoc_id"),
      coalesce(trim(col("tal_assoc_name")),        col("tal_assoc_name"))
        .as("tal_assoc_name"),
      coalesce(trim(col("tal_assoc_desc")), col("tal_assoc_desc"))
        .as("tal_assoc_desc"),
      col("tal_assoc_type_cd").cast(StringType).as("tal_assoc_type_cd"),
      coalesce(trim(col("target_udl_name")), col("target_udl_name"))
        .as("target_udl_name"),
      coalesce(trim(col("alt_udl_name")), col("alt_udl_name"))
        .as("alt_udl_name"),
      col("alt_rank").cast(StringType).as("alt_rank"),
      col("constituent_rank").cast(StringType).as("constituent_rank"),
      coalesce(trim(col("constituent_group")), col("constituent_group"))
        .as("constituent_group"),
      coalesce(trim(col("constituent_reqd")), col("constituent_reqd"))
        .as("constituent_reqd"),
      coalesce(trim(col("shared_qual")),       col("shared_qual")).as("shared_qual"),
      coalesce(trim(col("override_tac_name")), col("override_tac_name"))
        .as("override_tac_name"),
      coalesce(trim(col("override_tar_name")), col("override_tar_name"))
        .as("override_tar_name"),
      coalesce(col("newline"), lit("""
""")).as("newline")
    )

}
