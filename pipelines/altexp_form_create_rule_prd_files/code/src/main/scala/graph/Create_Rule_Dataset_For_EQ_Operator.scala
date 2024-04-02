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

object Create_Rule_Dataset_For_EQ_Operator {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("qualifier_cd"), col("compare_value"))
      .agg(max(lit("eq")).as("operator"),
           bv_from_index_vector(collect_list(col("dl_bit").cast(IntegerType)))
             .cast(IntegerType)
             .as("products")
      )

}
