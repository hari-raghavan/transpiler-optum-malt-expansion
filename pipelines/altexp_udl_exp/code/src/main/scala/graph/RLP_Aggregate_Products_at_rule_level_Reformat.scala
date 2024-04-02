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

object RLP_Aggregate_Products_at_rule_level_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("udl_id"),
      col("udl_rule_id"),
      col("udl_nm"),
      col("udl_desc"),
      col("rule_priority"),
      col("inclusion_cd"),
      col("qualifier_cd"),
      col("operator"),
      col("compare_value"),
      col("conjunction_cd"),
      col("rule_expression_id"),
      col("eff_dt"),
      col("term_dt"),
      coalesce(col("newline"), lit("""
""")).as("newline")
    )

}
