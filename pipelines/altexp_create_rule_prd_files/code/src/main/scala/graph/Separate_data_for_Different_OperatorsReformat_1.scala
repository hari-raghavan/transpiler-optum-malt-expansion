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

object Separate_data_for_Different_OperatorsReformat_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(col("qualifier_cd"),
              col("operator"),
              col("compare_value"),
              col("products")
    )

}
