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

object Aggregate_products_for_a_TSD_Code_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(col("tsd_id").cast(DecimalType(10, 0)).as("tsd_id"),
              col("tsd_cd"),
              col("products"),
              coalesce(col("newline"), lit("""
""")).as("newline")
    )

}
