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
      col("user_defined_list_id")
        .cast(DecimalType(10, 0))
        .as("user_defined_list_id"),
      coalesce(trim(col("user_defined_list_name")),
               col("user_defined_list_name")
      ).as("user_defined_list_name"),
      coalesce(trim(col("user_defined_list_desc")),
               col("user_defined_list_desc")
      ).as("user_defined_list_desc"),
      col("user_defined_list_rule_id")
        .cast(DecimalType(10, 0))
        .as("user_defined_list_rule_id"),
      coalesce(trim(col("rule")),    col("rule")).as("rule"),
      coalesce(trim(col("incl_cd")), col("incl_cd")).as("incl_cd"),
      col("eff_dt"),
      col("term_dt"),
      coalesce(col("newline"), lit("""
""")).as("newline")
    )

}
