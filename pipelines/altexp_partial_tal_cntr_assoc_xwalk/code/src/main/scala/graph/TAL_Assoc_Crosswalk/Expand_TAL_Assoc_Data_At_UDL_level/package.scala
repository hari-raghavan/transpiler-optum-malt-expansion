package graph.TAL_Assoc_Crosswalk

import io.prophecy.libs._
import udfs.PipelineInitCode._
import graph.TAL_Assoc_Crosswalk.Expand_TAL_Assoc_Data_At_UDL_level.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Expand_TAL_Assoc_Data_At_UDL_level {

  def apply(context: Context, in: DataFrame): Unit = {
    val df_Expand_Alternate_UDL = Expand_Alternate_UDL(context, in)
    val df_Expand_target_UDL    = Expand_target_UDL(context,    in)
    val df_MRG_UnionAll =
      MRG_UnionAll(context, df_Expand_Alternate_UDL, df_Expand_target_UDL)
    val df_MRG = MRG(context, df_MRG_UnionAll)
    OFILE_Normalize_TAL_Assoc_data_UDL_level(context, df_MRG)
  }

}
