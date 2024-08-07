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

object calculate_offset {

  def apply(context: Context, in: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.{Window, WindowSpec}
    in.withColumn(
      "offset",
      sum(
        size(col("left_target_xml").getField("Rule").getField("Rule")) + size(
          col("left_target_xml").getField("Rule").getField("Qual")
        ) + size(col("right_alt_xml").getField("Rule").getField("Rule")) + size(
          col("right_alt_xml").getField("Rule").getField("Qual")
        )
      ).over(
        Window
          .partitionBy(lit(1))
          .rowsBetween(Window.unboundedPreceding, "-1".toLong)
      )
    )
  }

}
