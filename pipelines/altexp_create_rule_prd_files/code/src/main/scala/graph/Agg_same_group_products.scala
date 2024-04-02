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

object Agg_same_group_products {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("compare_value"))
      .agg(max(col("qualifier_cd")).as("qualifier_cd"),
           max(col("operator")).as("operator"),
           expr("bit_or(cast(products as int))").as("products")
      )

}
