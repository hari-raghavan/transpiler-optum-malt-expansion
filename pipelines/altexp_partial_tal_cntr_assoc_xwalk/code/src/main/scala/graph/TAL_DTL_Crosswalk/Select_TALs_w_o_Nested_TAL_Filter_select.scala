package graph.TAL_DTL_Crosswalk

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.TAL_DTL_Crosswalk.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Select_TALs_w_o_Nested_TAL_Filter_select {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(
      array_contains(split(lit(context.config.TAL_NAME), ","), col("tal_name"))
    )

}
