package graph.TAL_Assoc_Crosswalk

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.TAL_Assoc_Crosswalk.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Process_TAL_Association_Rows_input_select_filter {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val Config = context.config
    in.filter(
      when(is_not_blank(lit(Config.TAL_ASSOC_NAME)),
           array_contains(split(lit(Config.TAL_ASSOC_NAME), ","),
                          col("tal_assoc_name")
           )
      ).otherwise(lookup_match("TAL_DTL_XWALK", col("tal_assoc_name")))
        .cast(BooleanType)
    )
  }

}
