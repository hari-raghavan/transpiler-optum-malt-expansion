package io.prophecy.pipelines.altexp_xml_rules_parser_2.graph

import io.prophecy.libs._
import io.prophecy.pipelines.altexp_xml_rules_parser_2.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object IFILE_UDL_rules {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("csv")
      .option("header", false)
      .option("sep",    "\u0000")
      .schema(
        StructType(
          Array(
            StructField("user_defined_list_id",      StringType, true),
            StructField("user_defined_list_name",    StringType, true),
            StructField("user_defined_list_desc",    StringType, true),
            StructField("user_defined_list_rule_id", StringType, true),
            StructField("rule",                      StringType, true),
            StructField("incl_cd",                   StringType, true),
            StructField("eff_dt",                    StringType, true),
            StructField("term_dt",                   StringType, true),
            StructField("newline",                   StringType, true)
          )
        )
      )
      .load(context.config.EXTRACT_FILE)

}
