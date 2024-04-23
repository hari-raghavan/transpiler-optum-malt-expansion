package graph.TAL_Container_Assoc

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.TAL_Container_Assoc.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Combine_Association_details_Filter_select {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(
      lookup_match("TAL_ASSOC_XWALK", col("tal_assoc_name")).cast(BooleanType)
    )

}
