package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import graph.TAL_Assoc_Crosswalk.config._
import graph.TAL_Assoc_Crosswalk.Expand_TAL_Assoc_Data_At_UDL_level
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object TAL_Assoc_Crosswalk {

  def apply(context: Context): Unit = {
    val df_TAL_Assoc_Dtl = TAL_Assoc_Dtl(context)
    val df_Process_TAL_Association_Rows_input_select_filter =
      Process_TAL_Association_Rows_input_select_filter(context,
                                                       df_TAL_Assoc_Dtl
      )
    val df_TAL_Assoc_Crosswalk__Process_TAL_Association_Rows =
      TAL_Assoc_Crosswalk__Process_TAL_Association_Rows(
        context,
        df_Process_TAL_Association_Rows_input_select_filter
      )
    val df_Process_TAL_Association_Rows_Reformat =
      Process_TAL_Association_Rows_Reformat(
        context,
        df_TAL_Assoc_Crosswalk__Process_TAL_Association_Rows
      )
    val df_Process_TAL_Association_Rows_output_select_filter =
      Process_TAL_Association_Rows_output_select_filter(
        context,
        df_Process_TAL_Association_Rows_Reformat
      )
    if (
      _root_.io.prophecy.abinitio.ScalaFunctions.convertToBoolean(
        _root_.io.prophecy.abinitio.ScalaFunctions
          ._is_blank(context.config.TAL_ASSOC_NAME)
      )
    )
      TAL_ASSOC_XWALK_lookup(
        context,
        df_Process_TAL_Association_Rows_output_select_filter
      )
    TAL_ASSOC_XWALK(context,
                    df_Process_TAL_Association_Rows_output_select_filter
    )
    if (
      _root_.io.prophecy.abinitio.ScalaFunctions._not(
        _root_.io.prophecy.abinitio.ScalaFunctions.convertToBoolean(
          _root_.io.prophecy.abinitio.ScalaFunctions
            ._is_blank(context.config.TAL_ASSOC_NAME)
        )
      )
    )
      Expand_TAL_Assoc_Data_At_UDL_level.apply(
        Expand_TAL_Assoc_Data_At_UDL_level.config.Context(
          context.spark,
          context.config.Expand_TAL_Assoc_Data_At_UDL_level
        ),
        df_Process_TAL_Association_Rows_output_select_filter
      )
  }

}
