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

object s_UnionAll {

  def apply(context: Context, in3: DataFrame, in2: DataFrame): DataFrame =
    List(in3, in2).flatMap(Option(_)).reduce(_.unionAll(_))

}
