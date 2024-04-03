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

object Create_All_Dataset_Rule {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(lit(1).as("1"))
      .agg(
        max(lit(null)).cast(StringType).as("carrier"),
        max(lit(null)).cast(StringType).as("account"),
        max(lit(null)).cast(StringType).as("group"),
        bv_from_index_vector(collect_list(col("dl_bit").cast(IntegerType)))
          .as("all_products")
      )

}
