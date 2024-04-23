package graph.TAL_Assoc_Crosswalk.Expand_TAL_Assoc_Data_At_UDL_level

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.TAL_Assoc_Crosswalk.Expand_TAL_Assoc_Data_At_UDL_level.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object MRG {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.orderBy(col("tal_assoc_id").asc)

}
