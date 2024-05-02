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
    val df_Rebate_UDL = Rebate_UDL(context)
    Rebate_UDL_lookup(context, df_Rebate_UDL)
    val df_TSD = TSD(context)
    TSD_lookup(context, df_TSD)
    val df_Expanded_UDL = Expanded_UDL(context)
    Expanded_UDL_lookup(context, df_Expanded_UDL)
    val df_Rule_Prdcts           = Rule_Prdcts(context)
    val df_Formulary_Rule_Prdcts = Formulary_Rule_Prdcts(context)
    Formulary_Rule_Prdcts_lookup(context, df_Formulary_Rule_Prdcts)
    val df_LKP_TAR_ROA_DF = LKP_TAR_ROA_DF(context)
    Rule_Prdcts_lookup(context,    df_Rule_Prdcts)
    LKP_TAR_ROA_DF_lookup(context, df_LKP_TAR_ROA_DF)
    val df_IFILE_TAR_Rule_Xwalk = IFILE_TAR_Rule_Xwalk(context)
    val df_Populate_TAR_Target_Alternatives_Crosswalk_input_select_filter =
      Populate_TAR_Target_Alternatives_Crosswalk_input_select_filter(
        context,
        df_IFILE_TAR_Rule_Xwalk
      )
    val df_Populate_TAR_Target_Alternate_Crosswalk =
      Populate_TAR_Target_Alternate_Crosswalk(
        context,
        df_Populate_TAR_Target_Alternatives_Crosswalk_input_select_filter
      )
    val df_Populate_TAR_Target_Alternatives_Crosswalk_output_select_filter =
      Populate_TAR_Target_Alternatives_Crosswalk_output_select_filter(
        context,
        df_Populate_TAR_Target_Alternate_Crosswalk
      )
    OFILE_TAR_Expansion_Xwalk(
      context,
      df_Populate_TAR_Target_Alternatives_Crosswalk_output_select_filter
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
    spark.conf
      .set("prophecy.metadata.pipeline.uri", "pipelines/altexp_tar_xwalk")
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/altexp_tar_xwalk") {
      apply(context)
    }
  }

}
