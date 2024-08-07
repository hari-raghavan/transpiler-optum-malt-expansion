package io.prophecy.pipelines.altexp_tac_rules_parser_2.graph

import io.prophecy.libs._
import io.prophecy.pipelines.altexp_tac_rules_parser_2.udfs.PipelineInitCode._
import io.prophecy.pipelines.altexp_tac_rules_parser_2.udfs.UDFs._
import io.prophecy.pipelines.altexp_tac_rules_parser_2.config.Context
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
        col("left.target_xml").as("left_target_xml"),
        col("right.alt_xml").as("right_alt_xml"),
        coalesce(col("left.newline"), lit("""
""")).as("newline")
      )

}
