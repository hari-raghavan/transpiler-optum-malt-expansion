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

object Sort_Rules_Per_UDL {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.orderBy(col("udl_id").asc,
               col("rule_priority").asc,
               col("inclusion_cd").asc,
               col("rule_expression_id").asc,
               col("conjunction_cd").asc
    )

}
