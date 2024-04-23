package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import graph.Association_Processing.config._
import graph.Association_Processing.Alternative_Rollup_Processing
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Association_Processing {

  def apply(
    context: Context,
    in:      DataFrame,
    in1:     DataFrame,
    in2:     DataFrame,
    in3:     DataFrame
  ): Subgraph4 = {
    val df_Apply_TAC_TAR_on_STD_Assoc_and_create_target_alternatives_pair_Reformat =
      Apply_TAC_TAR_on_STD_Assoc_and_create_target_alternatives_pair_Reformat(
        context,
        in,
        in1,
        in2,
        in3
      )
    val (df_Rank_Alts_RowDistributor_out0, df_Rank_Alts_RowDistributor_out1) =
      Rank_Alts_RowDistributor(
        context,
        df_Apply_TAC_TAR_on_STD_Assoc_and_create_target_alternatives_pair_Reformat
      )
    val df_Rank_AltsReformat_0 =
      Rank_AltsReformat_0(context, df_Rank_Alts_RowDistributor_out0)
    val df_Normalize_TAL_Target_Drugs_Without_Alternatives =
      Normalize_TAL_Target_Drugs_Without_Alternatives(context,
                                                      df_Rank_AltsReformat_0
      )
    val df_Rank_AltsReformat_1 =
      Rank_AltsReformat_1(context, df_Rank_Alts_RowDistributor_out1)
    val df_Normalize_TAL_Target_Drugs =
      Normalize_TAL_Target_Drugs(context, df_Rank_AltsReformat_1)
    val df_Separate_Target_and_Alternative_Load_Ready_FilesReformat_1 =
      Separate_Target_and_Alternative_Load_Ready_FilesReformat_1(
        context,
        df_Normalize_TAL_Target_Drugs
      )
    val df_Normalize_TAL_Alts_assign_attributes =
      Normalize_TAL_Alts_assign_attributes(
        context,
        df_Separate_Target_and_Alternative_Load_Ready_FilesReformat_1
      )
    val (df_Alternative_Rollup_Processing_out1,
         df_Alternative_Rollup_Processing_out
    ) = Alternative_Rollup_Processing.apply(
      Alternative_Rollup_Processing.config
        .Context(context.spark, context.config.Alternative_Rollup_Processing),
      df_Normalize_TAL_Alts_assign_attributes
    )
    val df_RPL = RPL(context,
                     df_Alternative_Rollup_Processing_out,
                     df_Alternative_Rollup_Processing_out1
    )
    val df_Tranform_Proxy_DTL_Layout =
      Tranform_Proxy_DTL_Layout(context, df_RPL)
    val df_Separate_Target_and_Alternative_Load_Ready_FilesReformat_0 =
      Separate_Target_and_Alternative_Load_Ready_FilesReformat_0(
        context,
        df_Normalize_TAL_Target_Drugs
      )
    val df_Get_Unique_Target_Products = Get_Unique_Target_Products(
      context,
      df_Normalize_TAL_Target_Drugs_Without_Alternatives,
      df_Separate_Target_and_Alternative_Load_Ready_FilesReformat_0
    )
    val df_Normalize_Clinical_Indication =
      Normalize_Clinical_Indication(context, df_Get_Unique_Target_Products)
    (df_Normalize_Clinical_Indication,
     df_Tranform_Proxy_DTL_Layout,
     df_Get_Unique_Target_Products,
     df_RPL
    )
  }

}
