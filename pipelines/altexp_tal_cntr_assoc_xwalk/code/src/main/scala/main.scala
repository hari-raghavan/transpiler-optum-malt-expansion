import io.prophecy.libs._
import config._
import udfs.UDFs._
import udfs.PipelineInitCode._
import graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_LKP_Rule_Products = LKP_Rule_Products(context)
    LKP_Rule_Products_lookup(context, df_LKP_Rule_Products)
    val df_Expanded_UDL = Expanded_UDL(context)
    Expanded_UDL_lookup(context, df_Expanded_UDL)
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
    val df_MERGE_UnionAll = MERGE_UnionAll(
      context,
      df_Select_TALs_w_o_Nested_TALReformat_0,
      df_Get_the_association_details_for_nested_TAL
    )
    val df_MERGE         = MERGE(context, df_MERGE_UnionAll)
    val df_TAL_Assoc_Dtl = TAL_Assoc_Dtl(context)
    val df_Select_Valid_Associations =
      Select_Valid_Associations(context, df_MERGE, df_TAL_Assoc_Dtl)
    val df_SWG = SWG(context, df_Select_Valid_Associations)
    val df_Process_TAL_Association_Rows =
      Process_TAL_Association_Rows(context, df_SWG)
    val df_Process_TAL_Association_Rows_Reformat =
      Process_TAL_Association_Rows_Reformat(context,
                                            df_Process_TAL_Association_Rows
      )
    val df_Process_TAL_Association_Rows_output_select_filter =
      Process_TAL_Association_Rows_output_select_filter(
        context,
        df_Process_TAL_Association_Rows_Reformat
      )
    val (df_Filter_Exclusion_Association_out0,
         df_Filter_Exclusion_Association_out1
    ) = Filter_Exclusion_Association(
      context,
      df_Process_TAL_Association_Rows_output_select_filter
    )
    val df_Create_LKP_for_override_qualifiers_input_select_filter =
      Create_LKP_for_override_qualifiers_input_select_filter(
        context,
        df_Filter_Exclusion_Association_out1
      )
    val df_Create_LKP_for_override_qualifiers =
      Create_LKP_for_override_qualifiers(
        context,
        df_Create_LKP_for_override_qualifiers_input_select_filter
      )
    val df_Create_LKP_for_override_qualifiers_Reformat =
      Create_LKP_for_override_qualifiers_Reformat(
        context,
        df_Create_LKP_for_override_qualifiers
      )
    LKP_Shared_Qualifier_Products_lookup(
      context,
      df_Create_LKP_for_override_qualifiers_Reformat
    )
    val df_CONC = CONC(context,
                       df_Filter_Exclusion_Association_out1,
                       df_Filter_Exclusion_Association_out0
    )
    LKP_Shared_Qualifier_Products(context,
                                  df_Create_LKP_for_override_qualifiers_Reformat
    )
    val df_Scan_Remove_Exclusion_Products_from_Inclusion_and_Standard_Association =
      Scan_Remove_Exclusion_Products_from_Inclusion_and_Standard_Association(
        context,
        df_CONC
      )
    TAL_Container_w_Assoc_in_BV_Products(
      context,
      df_Scan_Remove_Exclusion_Products_from_Inclusion_and_Standard_Association
    )
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri",
                   "pipelines/altexp_tal_cntr_assoc_xwalk"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark,
                                "pipelines/altexp_tal_cntr_assoc_xwalk"
    ) {
      apply(context)
    }
  }

}
