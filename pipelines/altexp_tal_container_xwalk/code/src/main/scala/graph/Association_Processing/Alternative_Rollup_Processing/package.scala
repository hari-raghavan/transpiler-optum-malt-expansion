package graph.Association_Processing

import io.prophecy.libs._
import udfs.PipelineInitCode._
import graph.Association_Processing.Alternative_Rollup_Processing.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Alternative_Rollup_Processing {

  def apply(context: Context, in: DataFrame): Subgraph2 = {
    val (df_FBE_Target_Product_with_without_Constituent_Data_out0,
         df_FBE_Target_Product_with_without_Constituent_Data_out1
    ) = FBE_Target_Product_with_without_Constituent_Data(context, in)
    val df_Association_Processing__Alternative_Rollup_Processing__Identify_Alt_Proxy_Targets_w_Constituents =
      Association_Processing__Alternative_Rollup_Processing__Identify_Alt_Proxy_Targets_w_Constituents(
        context,
        df_FBE_Target_Product_with_without_Constituent_Data_out1
      )
    val df_Association_Processing__Alternative_Rollup_Processing__Identify_Alt_Proxy_Targets_w_o_Constituent_1 =
      Association_Processing__Alternative_Rollup_Processing__Identify_Alt_Proxy_Targets_w_o_Constituent_1(
        context,
        df_FBE_Target_Product_with_without_Constituent_Data_out0
      )
    val df_flatten = flatten(
      context,
      df_Association_Processing__Alternative_Rollup_Processing__Identify_Alt_Proxy_Targets_w_o_Constituent_1
    )
    val df_Identify_Alt_Proxy_Targets_w_Constituents_Reformat =
      Identify_Alt_Proxy_Targets_w_Constituents_Reformat(
        context,
        df_Association_Processing__Alternative_Rollup_Processing__Identify_Alt_Proxy_Targets_w_Constituents
      )
    val df_Identify_Alt_Proxy_Targets_w_o_Constituent_Reformat =
      Identify_Alt_Proxy_Targets_w_o_Constituent_Reformat(context, df_flatten)
    val df_Normalize_Alt_Proxy_Products = Normalize_Alt_Proxy_Products(
      context,
      df_Identify_Alt_Proxy_Targets_w_Constituents_Reformat
    )
    (df_Identify_Alt_Proxy_Targets_w_o_Constituent_Reformat,
     df_Normalize_Alt_Proxy_Products
    )
  }

}
