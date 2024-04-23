package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import graph.TAL_DTL_Crosswalk.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object TAL_DTL_Crosswalk {

  def apply(context: Context): Unit = {
    val df_TAL_Container_Dtls = TAL_Container_Dtls(context)
    val df_Select_TALs_w_o_Nested_TAL_Filter_select =
      Select_TALs_w_o_Nested_TAL_Filter_select(context, df_TAL_Container_Dtls)
    val (df_Select_TALs_w_o_Nested_TAL_RowDistributor_out0,
         df_Select_TALs_w_o_Nested_TAL_RowDistributor_out1
    ) = Select_TALs_w_o_Nested_TAL_RowDistributor(
      context,
      df_Select_TALs_w_o_Nested_TAL_Filter_select
    )
    val df_Select_TALs_w_o_Nested_TALReformat_0 =
      Select_TALs_w_o_Nested_TALReformat_0(
        context,
        df_Select_TALs_w_o_Nested_TAL_RowDistributor_out0
      )
    val df_Select_TALs_w_o_Nested_TALReformat_1 =
      Select_TALs_w_o_Nested_TALReformat_1(
        context,
        df_Select_TALs_w_o_Nested_TAL_RowDistributor_out1
      )
    val df_Get_the_association_details_for_nested_TAL =
      Get_the_association_details_for_nested_TAL(
        context,
        df_Select_TALs_w_o_Nested_TALReformat_1
      )
    val df_Merge_UnionAll = Merge_UnionAll(
      context,
      df_Select_TALs_w_o_Nested_TALReformat_0,
      df_Get_the_association_details_for_nested_TAL
    )
    val df_Merge = Merge(context, df_Merge_UnionAll)
    TAL_DTL_XWALK_lookup(context, df_Merge)
    TAL_DTL_XWALK(context,        df_Merge)
  }

}
