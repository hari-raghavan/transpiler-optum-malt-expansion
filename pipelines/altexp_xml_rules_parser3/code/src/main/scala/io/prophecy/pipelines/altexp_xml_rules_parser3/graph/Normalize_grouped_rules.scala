package io.prophecy.pipelines.altexp_xml_rules_parser3.graph

import io.prophecy.libs._
import io.prophecy.pipelines.altexp_xml_rules_parser3.config.Context
import io.prophecy.pipelines.altexp_xml_rules_parser3.udfs.UDFs._
import io.prophecy.pipelines.altexp_xml_rules_parser3.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Normalize_grouped_rules {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
      val normalize_out_DF = in.normalize(
        lengthExpression = Some(size(col("rule_def"))),
        finishedExpression = None,
        finishedCondition = None,
        alias = "index",
        colsToSelect = List(
          (element_at(col("rule_def"), col("index") + lit(1)).getField("qualifier_cd")).as("qualifier_cd"),
          (element_at(col("rule_def"), col("index") + lit(1)).getField("operator")).as("operator"),
          (element_at(col("rule_def"), col("index") + lit(1)).getField("conjunction_cd")).as("conjunction_cd"),
          (element_at(col("rule_def"), col("index") + lit(1)).getField("rule_expression_id")).as("rule_expression_id"),
          (when(
            element_at(col("rule_def"),                col("index") + lit(1)).getField("qualifier_cd") === lit("DRUG_NAME"),
            regexp_replace(element_at(col("rule_def"), col("index") + lit(1)).getField("compare_value"),
                           lit("\\Q[[[^0-9]s[^0-9][^0-9]ce[^0-9]]]+\\E"),
                           lit("")
            )
          ).otherwise(element_at(col("rule_def"), col("index") + lit(1)).getField("compare_value"))).as("compare_value"),
          col("udl_id"),
          col("udl_rule_id"),
          col("udl_nm"),
          col("udl_desc"),
          col("rule_priority"),
          col("inclusion_cd"),
          col("eff_dt"),
          col("term_dt"),
          col("newline"),
        ),
        lengthRelatedGlobalExpressions = Map(),
        tempWindowExpr = Map()
      )
    
      val simpleSelect_in_DF = normalize_out_DF.select(
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
          col("newline")
      )
    
      val out = simpleSelect_in_DF
    out
  }

}
