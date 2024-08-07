package io.prophecy.pipelines.altexp_xml_rules_parser3.graph

import io.prophecy.libs._
import io.prophecy.pipelines.altexp_xml_rules_parser3.udfs.PipelineInitCode._
import io.prophecy.pipelines.altexp_xml_rules_parser3.udfs.UDFs._
import io.prophecy.pipelines.altexp_xml_rules_parser3.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Group_the_Rules_and_assign_priority {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("user_defined_list_id").cast(StringType).as("udl_id"),
      col("user_defined_list_rule_id").cast(StringType).as("udl_rule_id"),
      col("user_defined_list_name").as("udl_nm"),
      col("user_defined_list_desc").as("udl_desc"),
      rule_qual_priority(
        get_rule_def(col("xml"), coalesce(col("offset"), lit(0)))
      ).cast(StringType).as("rule_priority"),
      rpad(col("incl_cd"),     1, " ").as("inclusion_cd"),
      get_rule_def(col("xml"), coalesce(col("offset"), lit(0))).as("rule_def"),
      col("eff_dt"),
      col("term_dt"),
      col("newline")
    )

}
