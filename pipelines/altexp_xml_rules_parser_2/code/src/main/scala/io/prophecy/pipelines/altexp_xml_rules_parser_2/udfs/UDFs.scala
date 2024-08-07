package io.prophecy.pipelines.altexp_xml_rules_parser_2.udfs

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

  def get_rule_def(xml: org.apache.spark.sql.Column) =
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
                  when(lit(j) < size(rule.getField("OR")), lit("0"))
                    .otherwise(lit(""))
                    .as("conjunction_cd"),
                  (lit(i) + lit(50)).as("rule_expression_id")
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
            (monotonically_increasing_id() + lit(50))
              .cast(IntegerType)
              .as("rule_expression_id")
          )
      )
    )

  def rule_rank(list: org.apache.spark.sql.Column) =
    array_min(
      transform(
        list,
        qual_cd =>
          coalesce(
            element_at(
              filter(
                array(
                  struct(lit("NDC11").as("qual"),       lit(1).as("priority")),
                  struct(lit("NDC9").as("qual"),        lit(2).as("priority")),
                  struct(lit("NDC5").as("qual"),        lit(3).as("priority")),
                  struct(lit("GPI14").as("qual"),       lit(4).as("priority")),
                  struct(lit("GPI12").as("qual"),       lit(5).as("priority")),
                  struct(lit("GPI10").as("qual"),       lit(6).as("priority")),
                  struct(lit("DOSAGE_FORM").as("qual"), lit(7).as("priority")),
                  struct(lit("ROA").as("qual"),         lit(8).as("priority")),
                  struct(lit("DRUG_NAME").as("qual"),   lit(9).as("priority")),
                  struct(lit("GPI8").as("qual"),        lit(10).as("priority")),
                  struct(lit("GPI6").as("qual"),        lit(11).as("priority")),
                  struct(lit("GPI4").as("qual"),        lit(12).as("priority")),
                  struct(lit("MSC").as("qual"),         lit(13).as("priority")),
                  struct(lit("RXOTC").as("qual"),       lit(14).as("priority")),
                  struct(lit("DAYS_UNTIL_DRUG_STATUS_INACTIVE").as("qual"),
                         lit(15).as("priority")
                  ),
                  struct(lit("STATUS_CD").as("qual"),  lit(16).as("priority")),
                  struct(lit("REPACKAGER").as("qual"), lit(17).as("priority")),
                  struct(lit("DESI_CD").as("qual"),    lit(18).as("priority"))
                ),
                xx => xx.getField("qual") === qual_cd
              ),
              1
            ).getField("priority"),
            lit(0)
          )
      )
    )

  def qual_list(rule: org.apache.spark.sql.Column) =
    transform(rule, r => r.getField("qualifier_cd"))

  def rule_qual_priority(rule: org.apache.spark.sql.Column) =
    rule_rank(array_distinct(array_sort(qual_list(rule))))

}
