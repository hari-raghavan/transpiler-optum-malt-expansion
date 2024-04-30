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
      col("tac_dtl_id").cast(DecimalType(10, 0)).as("tac_dtl_id"),
      col("tac_id").cast(DecimalType(10,     0)).as("tac_id"),
      coalesce(trim(col("tac_name")),        col("tac_name")).as("tac_name"),
      col("priority").cast(StringType).as("priority"),
      coalesce(trim(col("target_rule")), col("target_rule")).as("target_rule"),
      coalesce(trim(col("alt_rule")),    col("alt_rule")).as("alt_rule"),
      col("eff_dt"),
      col("term_dt"),
      coalesce(col("newline"), lit("""
""")).as("newline")
    )

}
