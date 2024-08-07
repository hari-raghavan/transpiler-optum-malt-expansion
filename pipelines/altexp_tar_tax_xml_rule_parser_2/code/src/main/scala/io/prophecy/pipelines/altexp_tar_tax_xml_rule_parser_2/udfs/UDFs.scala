package io.prophecy.pipelines.altexp_tar_tax_xml_rule_parser_2.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) =
    registerAllUDFs(spark)

}

object PipelineInitCode extends Serializable {

  def get_rule_def(
    xml:    org.apache.spark.sql.Column,
    offset: org.apache.spark.sql.Column
  ) =
    concat(
      flatten(
        transform(
          xml.getField("Rule").getField("Rule"),
          (rule, i) =>
            transform(
              rule.getField("Qual"),
              (qual, j) =>
                struct(
                  qual.getField("type0").as("qualifier_cd"),
                  qual.getField("op").as("operator"),
                  qual.getField("Qual").as("compare_value"),
                  when(lit(j) < size(rule.getField("OR")), lit("O"))
                    .otherwise(lit(""))
                    .as("conjunction_cd"),
                  (offset + lit(1) + lit(i) + lit(50)).as("rule_expression_id")
                )
            )
        )
      ),
      transform(
        xml.getField("Rule").getField("Qual"),
        (qual, i) =>
          struct(
            qual.getField("type0").as("qualifier_cd"),
            qual.getField("op").as("operator"),
            qual.getField("Qual").as("compare_value"),
            lit("").as("conjunction_cd"),
            (offset + lit(1) + size(
              xml.getField("Rule").getField("Rule")
            ) + lit(i) + lit(50)).cast(IntegerType).as("rule_expression_id")
          )
      )
    )

}
