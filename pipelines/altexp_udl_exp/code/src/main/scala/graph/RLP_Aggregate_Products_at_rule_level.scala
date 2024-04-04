package graph

import io.prophecy.libs._
import config.Context
import udfs.UDFs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object RLP_Aggregate_Products_at_rule_level {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    val process_udf = udf(
      (inputRows: Seq[Row]) => {
        var no_ands = 1;
        var products = _bv_all_zeros()
        var or_products = _bv_all_zeros()
        var udl_id = ""
        var udl_rule_id = ""
        var udl_nm = ""
        var udl_desc = ""
        var rule_priority = ""
        var inclusion_cd = ""
        var qualifier_cd = ""
        var operator = ""
        var compare_value = ""
        var conjunction_cd = ""
        var rule_expression_id = ""
        var eff_dt = ""
        var term_dt = ""
        var newline = ""
       
        inputRows.zipWithIndex.foreach {
          case (in, idx) => {
            or_products = _bv_or(or_products, in.getAs[Array[Byte]]("products"))
            if (in.getAs[String]("conjunction_cd") != "0") {
                if (no_ands){
                   products = or_products
                   or_products = _bv_all_zeros()
                   no_ands = 0
                } else {
                  products = _bv_and(products, or_products)
                  or_products = _bv_all_zeros()
                }
            }
          }
          udl_id = in.getAs[String]("udl_id")
          udl_rule_id = in.getAs[String]("udl_rule_id")
          udl_nm = in.getAs[String]("udl_nm")
          udl_desc = in.getAs[String]("udl_desc")
          rule_priority = in.getAs[String]("rule_priority")
          inclusion_cd = in.getAs[String]("inclusion_cd")
          qualifier_cd = in.getAs[String]("qualifier_cd")
          operator = in.getAs[String]("operator")
          compare_value = in.getAs[String]("compare_value")
          conjunction_cd = in.getAs[String]("conjunction_cd")
          rule_expression_id = in.getAs[String]("rule_expression_id")
          eff_dt = in.getAs[String]("eff_dt")
          term_dt = in.getAs[String]("term_dt")
          newline = in.getAs[String]("newline")
        }
        Row(
          udl_id,
          udl_rule_id,
          udl_nm,
          udl_desc,
          rule_priority,
          inclusion_cd,
          qualifier_cd,
          operator,
          compare_value,
          conjunction_cd,
          rule_expression_id,
          eff_dt,
          term_dt,
          newline,
          products
        )
      },
      StructType(List(
        StructField("udl_id", StringType, false),
        StructField("udl_rule_id", StringType, false),
        StructField("udl_nm", StringType, false),
        StructField("udl_desc", StringType, false),
        StructField("rule_priority", StringType, false),
        StructField("inclusion_cd", StringType, false),
        StructField("qualifier_cd", StringType, false),
        StructField("operator", StringType, false),
        StructField("compare_value", StringType, false),
        StructField("conjunction_cd", StringType, false),
        StructField("rule_expression_id", StringType, false),
        StructField("eff_dt", StringType, false),
        StructField("term_dt", StringType, false),
        StructField("newline", StringType, false),
        StructField("product", BinaryType, false)
      ))
    
    )
    
    
    val origColumns = in0.columns.map(col)
    val out0 = in0
      .groupBy("udl_rule_id")
      .agg(
        collect_list(
          struct(origColumns: _*)
        ).alias("inputRows")
      )
      .select((process_udf(col("inputRows"))).alias("output"))
      .select(col("output.*"))
    out0
  }

}
