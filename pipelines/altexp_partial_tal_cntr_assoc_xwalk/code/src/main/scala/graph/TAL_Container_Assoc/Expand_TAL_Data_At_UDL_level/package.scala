package graph.TAL_Container_Assoc

import io.prophecy.libs._
import udfs.PipelineInitCode._
import graph.TAL_Container_Assoc.Expand_TAL_Data_At_UDL_level.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Expand_TAL_Data_At_UDL_level {

  def apply(context: Context, in: DataFrame): Unit = {
    val df_Expand_Alternate_UDL_1 = Expand_Alternate_UDL_1(context, in)
    val df_Expand_target_UDL_1    = Expand_target_UDL_1(context,    in)
    val df_s_UnionAll =
      s_UnionAll(context, df_Expand_Alternate_UDL_1, df_Expand_target_UDL_1)
    val df_s = s(context, df_s_UnionAll)
    OFILE_Normaize_TAL_data_UDL_level(context, df_s)
  }

}
