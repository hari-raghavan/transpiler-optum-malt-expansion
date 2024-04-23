package graph.TAL_Container_Assoc.Expand_TAL_Data_At_UDL_level

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.TAL_Container_Assoc.Expand_TAL_Data_At_UDL_level.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object s {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.orderBy(col("tal_id").asc)

}
