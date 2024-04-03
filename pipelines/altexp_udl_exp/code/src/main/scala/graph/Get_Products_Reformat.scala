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

object Get_Products_Reformat {

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
""")).as("newline"),
      coalesce(lookup("LKP_Rule_Products",
                      col("qualifier_cd"),
                      col("operator"),
                      col("compare_value")
               ).getField("products"),
               lit(0).cast(BinaryType)
      ).as("products")
    )

}
