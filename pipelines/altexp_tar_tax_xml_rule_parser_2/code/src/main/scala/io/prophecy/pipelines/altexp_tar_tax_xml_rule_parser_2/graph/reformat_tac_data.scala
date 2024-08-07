package io.prophecy.pipelines.altexp_tar_tax_xml_rule_parser_2.graph

import io.prophecy.libs._
import io.prophecy.pipelines.altexp_tar_tax_xml_rule_parser_2.udfs.PipelineInitCode._
import io.prophecy.pipelines.altexp_tar_tax_xml_rule_parser_2.udfs.UDFs._
import io.prophecy.pipelines.altexp_tar_tax_xml_rule_parser_2.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object reformat_tac_data {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("tac_id"),
      col("tac_name"),
      col("priority"),
      col("eff_dt"),
      col("term_dt"),
      when(!col("left_target_xml.Rule").isNull,
           get_rule_def(col("left_target_xml"), coalesce(col("offset"), lit(0)))
      ).otherwise(array()).as("target_rule_def"),
      when(
        !col("right_alt_xml.Rule").isNull,
        get_rule_def(
          col("right_alt_xml"),
          coalesce(col("offset"), lit(0)) + size(
            col("left_target_xml").getField("Rule").getField("Rule")
          ) + size(col("left_target_xml").getField("Rule").getField("Qual"))
        )
      ).otherwise(array()).as("alt_rule_def"),
      col("newline")
    )

}
