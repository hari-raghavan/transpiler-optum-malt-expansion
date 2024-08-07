package io.prophecy.pipelines.altexp_xml_rules_parser_2.graph

import io.prophecy.libs._
import io.prophecy.pipelines.altexp_xml_rules_parser_2.config.Context
import io.prophecy.pipelines.altexp_xml_rules_parser_2.udfs.UDFs._
import io.prophecy.pipelines.altexp_xml_rules_parser_2.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Group_the_Rules_and_assign_priority {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    lazy val get_rule_def_udf2 = udf(
        { (inputRows: Seq[Row]) ⇒
          var seqId = 0;
          inputRows.map { input ⇒
            val xml = input.getAs[Row]("target_xml")
            val ruleDef = xml.getAs[Row]("Rule").getAs[Seq[Row]]("Rule").flatMap { rule ⇒
              seqId = seqId + 1
              rule.getAs[Seq[Row]]("Qual").zipWithIndex.map {
                case (qual, j) ⇒
                  Row(
                    qual.getAs[String]("type0"),
                    qual.getAs[String]("op"),
                    qual.getAs[String]("Qual"),
                    if (j < rule.getAs[Seq[Row]]("OR").length) "O" else "",
                    seqId + 50
                  )
              }
            } ++ xml.getAs[Row]("Rule").getAs[Seq[Row]]("Qual").map { qual ⇒
              seqId = seqId + 1
              Row(
                qual.getAs[String]("type0"),
                qual.getAs[String]("op"),
                qual.getAs[String]("Qual"),
                "",
                seqId + 50
              )
            }
            Row(
              input.getAs[String]("user_defined_list_id"),
              input.getAs[String]("user_defined_list_rule_id"),
              input.getAs[String]("user_defined_list_name"),
              input.getAs[String]("user_defined_list_desc"),
              ruleDef,
              _string_pad(input.getAs[String]("incl_cd"),1 , " "),
              input.getAs[String]("eff_dt"),
              input.getAs[String]("term_dt"),
              input.getAs[String]("newline")
            )
          }
        },
        ArrayType(
          StructType(
            List(
              StructField("udl_id",      StringType),
              StructField("udl_rule_id", StringType),
              StructField("udl_nm",      StringType),
              StructField("udl_desc",    StringType),
              StructField(
                "rule_def",
                ArrayType(
                  StructType(
                    List(
                      StructField("qualifier_cd",       StringType),
                      StructField("operator",           StringType),
                      StructField("compare_value",      StringType),
                      StructField("conjunction_cd",     StringType),
                      StructField("rule_expression_id", IntegerType)
                    )
                  )
                )
              ),
              StructField("incl_cd", StringType),
              StructField("eff_dt",  StringType),
              StructField("term_dt", StringType),
              StructField("newline", StringType)
            )
          )
        )
      )
    
    
        val origColumns = in.columns.map(col)
        val out0 =
          in
            .groupBy(lit(1))
            .agg(collect_list(struct(origColumns: _*)).as("input"))
            .select(explode(get_rule_def_udf2(col("input"))).as("output"))
            .select(col("output.*"))
            .withColumn("rule_priority", rule_qual_priority(col("rule_def")))
    out0
  }

}
