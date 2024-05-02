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
    val df_IFIL_Formulary_Dataset = IFIL_Formulary_Dataset(context)
    val df_Sort_ST_Group_ST_Numbers =
      Sort_ST_Group_ST_Numbers(context, df_IFIL_Formulary_Dataset)
    val df_Rollup_Group_Numbers_input_select_filter =
      Rollup_Group_Numbers_input_select_filter(context,
                                               df_Sort_ST_Group_ST_Numbers
      )
    val df_Rollup_Group_Numbers =
      Rollup_Group_Numbers(context, df_Rollup_Group_Numbers_input_select_filter)
    val df_Rollup_Group_Numbers_Reformat =
      Rollup_Group_Numbers_Reformat(context, df_Rollup_Group_Numbers)
    Step_Therapy_DTL_File(context,           df_Rollup_Group_Numbers_Reformat)
    val df_Products = Products(context)
    Products_lookup(context,              df_Products)
    Step_Therapy_DTL_File_lookup(context, df_Rollup_Group_Numbers_Reformat)
    val df_OFIL_TAC_Rule_Xwalk = OFIL_TAC_Rule_Xwalk(context)
    val df_Reformat_To_separate_NFST_and_ST_Filter_select =
      Reformat_To_separate_NFST_and_ST_Filter_select(context,
                                                     df_OFIL_TAC_Rule_Xwalk
      )
    val (df_Reformat_To_separate_NFST_and_ST_RowDistributor_out0,
         df_Reformat_To_separate_NFST_and_ST_RowDistributor_out1
    ) = Reformat_To_separate_NFST_and_ST_RowDistributor(
      context,
      df_Reformat_To_separate_NFST_and_ST_Filter_select
    )
    val df_Reformat_To_separate_NFST_and_STReformat_1 =
      Reformat_To_separate_NFST_and_STReformat_1(
        context,
        df_Reformat_To_separate_NFST_and_ST_RowDistributor_out1
      )
    val df_Map_T_A_pairs_as_per_ST_Group_NM_ST =
      Map_T_A_pairs_as_per_ST_Group_NM_ST(
        context,
        df_Reformat_To_separate_NFST_and_STReformat_1
      )
    val df_Reformat_To_separate_NFST_and_STReformat_0 =
      Reformat_To_separate_NFST_and_STReformat_0(
        context,
        df_Reformat_To_separate_NFST_and_ST_RowDistributor_out0
      )
    val df_Map_T_A_pairs_as_per_ST_Group_NM_NFST =
      Map_T_A_pairs_as_per_ST_Group_NM_NFST(
        context,
        df_Reformat_To_separate_NFST_and_STReformat_0
      )
    val df_TAL_Container =
      if (
        if (
          _root_.io.prophecy.abinitio.ScalaFunctions
            .convertToBoolean(context.config.TAC_CONDITION > 1)
        ) _root_.io.prophecy.abinitio.ScalaFunctions.convertToBoolean(1)
        else _root_.io.prophecy.abinitio.ScalaFunctions.convertToBoolean(0)
      )
        TAL_Container(context)
      else null
    val df_Concat = Concat(context,
                           df_Map_T_A_pairs_as_per_ST_Group_NM_NFST,
                           df_TAL_Container
    )
    val df_Drop_Targets_From_Tal_Assoc =
      Drop_Targets_From_Tal_Assoc(context, df_Concat)
    val df_Gather = Gather(context,
                           df_Drop_Targets_From_Tal_Assoc,
                           df_Map_T_A_pairs_as_per_ST_Group_NM_ST
    )
    TAC_Container_Step_Therapy(context, df_Gather)
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
                   "pipelines/altexp_step_therapy_xwalk"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/altexp_step_therapy_xwalk") {
      apply(context)
    }
  }

}
