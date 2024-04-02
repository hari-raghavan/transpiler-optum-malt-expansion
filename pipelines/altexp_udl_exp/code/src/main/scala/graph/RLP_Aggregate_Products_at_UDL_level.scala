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

object RLP_Aggregate_Products_at_UDL_level {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    val process_udf = udf(
      (inputRows: Seq[Row]) => {
    
        var include_products = 0
        var exclude_products = 0
        var new_products = 0
        var udl_id = ""
        var udl_nm = ""
        var udl_desc = ""
        var products = 0
        var eff_dt = ""
        var term_dt = ""
        var contents = Array[Int]()
        var newline = ""
       
        inputRows.zipWithIndex.foreach {
          case (in, idx) => {
            if (in.getAs[String]("inclusion_cd") == "E")
              exclude_products = exclude_products | in.getAs[Int]("products")
            else if (in.getAs[String]("inclusion_cd") == "I") {
              new_products =  in.getAs[Int]("products") & ~(include_products | exclude_products)
              include_products = include_products | new_products
              contents = Array.concat(contents, Array(new_products))
            }
            udl_id = in.getAs[String]("udl_id")
            udl_nm = in.getAs[String]("udl_nm")
            udl_desc = in.getAs[String]("udl_desc")
            products = in.getAs[String]("products")
            eff_dt = in.getAs[String]("eff_dt")
            term_dt = in.getAs[String]("term_dt")
            newline = in.getAs[String]("newline")
          }
        }
        Row(
          udl_id,
          udl_nm,
          udl_desc,
          product,
          eff_dt,
          term_dt,
          contents,
          newline,
        )
      },
      StructType(List(
        StructField("udl_id", StringType, false),
        StructField("udl_nm", StringType, false),
        StructField("udl_desc", StringType, false),
        StructField("product", IntegerType, false),
        StructField("eff_dt", StringType, false),
        StructField("term_dt", StringType, false),
        StructField("contents", ArrayType(IntegerType), false),
        StructField("newline", StringType, false),
      ))
    
    )
    
    
    val origColumns = in0.columns.map(col)
    val out0 = in0
      .groupBy("udl_id")
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
