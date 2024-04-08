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

object Group_Rules_for_Target_Alternative {

  def apply(context: Context, left: DataFrame, right: DataFrame): DataFrame =
    left
      .as("left")
      .join(right.as("right"), col("left.id") === col("right.id"), "outer")
      .select(
        col("left.tac_id").as("tac_id"),
        col("left.tac_name").as("tac_name"),
        col("left.priority").as("priority"),
        col("left.eff_dt").as("eff_dt"),
        col("left.term_dt").as("term_dt"),
        when(!col("left.target_xml.Rule").isNull,
             get_rule_def(col("left.target_xml"))
        ).otherwise(array()).as("target_rule_def"),
        when(!col("right.alt_xml.Rule").isNull,
             get_rule_def(col("right.alt_xml"))
        ).otherwise(array()).as("alt_rule_def"),
        coalesce(col("left.newline"), lit("""
""")).as("newline")
      )

}
