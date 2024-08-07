package io.prophecy.pipelines.altexp_tac_rules_parser_2.graph

import io.prophecy.libs._
import io.prophecy.pipelines.altexp_tac_rules_parser_2.config.Context
import io.prophecy.pipelines.altexp_tac_rules_parser_2.udfs.UDFs._
import io.prophecy.pipelines.altexp_tac_rules_parser_2.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Group_Rules_for_Target_Alternative_sequence1 {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
      lazy val out = in.zipWithIndex(0, 1, "id", spark)
    out
  }

}
