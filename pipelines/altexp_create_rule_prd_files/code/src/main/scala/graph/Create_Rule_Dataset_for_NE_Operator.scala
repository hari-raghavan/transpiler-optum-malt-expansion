package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Create_Rule_Dataset_for_NE_Operator {

  def apply(context: Context, left: DataFrame, right: DataFrame): DataFrame =
    left
      .as("left")
      .join(right.as("right"), lit(true), "inner")
      .select(
        col("right.qualifier_cd").as("qualifier_cd"),
        lit("ne").as("operator"),
        col("right.compare_value").as("compare_value"),
        col("left.all_products")
          .bitwiseAND(bitwise_not(col("right.products")))
          .as("products")
      )

}
