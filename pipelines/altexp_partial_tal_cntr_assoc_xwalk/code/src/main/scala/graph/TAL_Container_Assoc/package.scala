package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import graph.TAL_Container_Assoc.config._
import graph.TAL_Container_Assoc.Expand_TAL_Data_At_UDL_level
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object TAL_Container_Assoc {

  def apply(context: Context): Unit = {
    val df_IFILE_TAL_DTL_XWALK = IFILE_TAL_DTL_XWALK(context)
    val df_Combine_Association_details_Filter_select =
      Combine_Association_details_Filter_select(context, df_IFILE_TAL_DTL_XWALK)
    val df_Combine_Association_details_Reformat =
      Combine_Association_details_Reformat(
        context,
        df_Combine_Association_details_Filter_select
      )
    val df_Sort = Sort(context, df_Combine_Association_details_Reformat)
    val df_TAL_Container_Assoc__Scan_Remove_Exclusion_Products_from_Inclusion_and_Standard_Association =
      TAL_Container_Assoc__Scan_Remove_Exclusion_Products_from_Inclusion_and_Standard_Association(
        context,
        df_Sort
      )
    val df_Scan_Remove_Exclusion_Products_from_Inclusion_and_Standard_Association_output_select_filter =
      Scan_Remove_Exclusion_Products_from_Inclusion_and_Standard_Association_output_select_filter(
        context,
        df_TAL_Container_Assoc__Scan_Remove_Exclusion_Products_from_Inclusion_and_Standard_Association
      )
    val df_TAL_Name = TAL_Name(
      context,
      df_Scan_Remove_Exclusion_Products_from_Inclusion_and_Standard_Association_output_select_filter
    )
    Expand_TAL_Data_At_UDL_level.apply(
      Expand_TAL_Data_At_UDL_level.config
        .Context(context.spark, context.config.Expand_TAL_Data_At_UDL_level),
      df_TAL_Name
    )
  }

}
