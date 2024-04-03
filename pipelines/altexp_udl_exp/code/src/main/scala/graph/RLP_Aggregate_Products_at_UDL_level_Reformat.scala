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

object RLP_Aggregate_Products_at_UDL_level_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("udl_id").cast(DecimalType(10, 0)).as("udl_id"),
      col("udl_nm"),
      col("udl_desc"),
      col("products"),
      col("eff_dt"),
      col("term_dt"),
      col("contents"),
      coalesce(col("newline"), lit("""
""")).as("newline")
    )

}
