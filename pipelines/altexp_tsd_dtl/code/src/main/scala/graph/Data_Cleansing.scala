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
      col("tsd_dtl_id").cast(DecimalType(10, 0)).as("tsd_dtl_id"),
      col("tsd_id").cast(DecimalType(10,     0)).as("tsd_id"),
      coalesce(trim(col("tsd_name")),        col("tsd_name")).as("tsd_name"),
      coalesce(trim(col("tsd_cd")),          col("tsd_cd")).as("tsd_cd"),
      coalesce(trim(col("formulary_tier")),  col("formulary_tier"))
        .as("formulary_tier"),
      coalesce(trim(col("formulary_status")), col("formulary_status"))
        .as("formulary_status"),
      col("priority").cast(StringType).as("priority"),
      col("eff_dt"),
      col("term_dt"),
      coalesce(col("newline"), lit("""
""")).as("newline")
    )

}
