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

object flatten_primary_data {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val flattened =
      in.withColumn("primary_data", explode_outer(col("primary_data")))
    flattened.select(
      if (flattened.columns.contains("primary_data")) col("primary_data")
      else col("primary_data")
    )
  }

}
