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

object Expand_TSD_Reformat {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    val processUDF = udf({
        (input: Seq[Row]) =>
          import _root_.io.prophecy.abinitio.ScalaFunctions._
          import _root_.io.prophecy.libs.AbinitioDMLs._
          val outputRows = scala.collection.mutable.ArrayBuffer[Row]()
          val prod_t = 0
          val prod_ref = 0
    
          input.zipWithIndex.foreach {
            case (in, idx) =>
                prod_t = in.getAs[Int]("prod_t") & ~(prod_ref)
                prod_ref = prod_ref | prod_t
                outputRows.append(
                  Row(
                    in.getAs[BigDecimal]("prod_t"),
                    in.getAs[String]("tsd_cd"),
                    prod_t,
                    in.getAs[String]("newline")
                  )
                )
          }
          outputRows
        },
        ArrayType(StructType(List(
          StructField("tsd_id", DecimalType(10,0), false),
          StructField("tsd_cd", StringType, false),
          StructField("products", IntegeryType, false),
          StructField("newline", StringType, false),
        )
        ))
    )
    
    def prod_t() = coalesce(
        when(
          (col("formulary_tier") =!= lit("*")).and(col("formulary_status") =!= lit("*")),
          lookup("Form_Rule_Products", lit("TIER"), col("formulary_tier"))
            .getField("products")
            .bitwiseAND(lookup("Form_Rule_Products", lit("STATUS"), col("formulary_status")).getField("products"))
        ).when(
            (col("formulary_tier") =!= lit("*")).and(col("formulary_status") === lit("*")),
            lookup("Form_Rule_Products", lit("TIER"), col("formulary_tier")).getField("products")
          )
          .when(
            (col("formulary_tier") === lit("*")).and(col("formulary_status") =!= lit("*")),
            lookup("Form_Rule_Products", lit("STATUS"), col("formulary_status")).getField("products")
          )
          .otherwise(
            lookup("Form_Rule_Products", lit("PA"), lit("Y"))
              .getField("products")
              .bitwiseOR(lookup("Form_Rule_Products", lit("PA"), lit("N")).getField("products"))
          ),
        lit(0)
    )
    
    val out = in.select(struct(prod_t().as("prod_t"), col("tsd_id"), col("tsd_cd"), col("newline")).as("tmp"))
          .groupBy(lit(1))
          .agg(collect_list(col("tmp")).as("input"))
          .select(explode(processUDF(col("input"))).as("output"))
          .select(col("output.*"))
    out0
  }

}
