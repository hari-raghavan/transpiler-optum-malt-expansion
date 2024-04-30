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
      col("tad_id").cast(DecimalType(10,  0)).as("tad_id"),
      coalesce(trim(col("target_gpi14")), col("target_gpi14"))
        .as("target_gpi14"),
      coalesce(trim(col("alt_grouping_gpi12")), col("alt_grouping_gpi12"))
        .as("alt_grouping_gpi12"),
      coalesce(trim(col("alt_selection_id")), col("alt_selection_id"))
        .as("alt_selection_id"),
      col("rank").cast(StringType).as("rank"),
      col("qty_adjust_factor").cast(DecimalType(6, 3)).as("qty_adjust_factor"),
      coalesce(col("newline"),                     lit("""
""")).as("newline")
    )

}
