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
    val df_Drug_Data_Set_Drug_Data_Set_DTL = Drug_Data_Set_Drug_Data_Set_DTL(
      context
    )
    val df_Drug_Data_Set_Drug_Data_Set_DTL_sync =
      Drug_Data_Set_Drug_Data_Set_DTL_sync(context,
                                           df_Drug_Data_Set_Drug_Data_Set_DTL
      )
    val df_Sort_GPI12s =
      Sort_GPI12s(context, df_Drug_Data_Set_Drug_Data_Set_DTL_sync)
    val df_Curr_Map_GPI14_under_GPI12_input_select_filter =
      Curr_Map_GPI14_under_GPI12_input_select_filter(context, df_Sort_GPI12s)
    val df_Curr_Map_GPI14_under_GPI12 = Curr_Map_GPI14_under_GPI12(
      context,
      df_Curr_Map_GPI14_under_GPI12_input_select_filter
    )
    val df_Curr_Map_GPI14_under_GPI12_Reformat =
      Curr_Map_GPI14_under_GPI12_Reformat(context,
                                          df_Curr_Map_GPI14_under_GPI12
      )
    val df_Curr_Assign_Rank_and_Calculate_Ratio =
      Curr_Assign_Rank_and_Calculate_Ratio(
        context,
        df_Curr_Map_GPI14_under_GPI12_Reformat
      )
    val df_GPI_RANK_RATIO_Load_DateTimeNormalize =
      GPI_RANK_RATIO_Load_DateTimeNormalize(
        context,
        df_Curr_Assign_Rank_and_Calculate_Ratio
      )
    GPI_RANK_RATIO_Load(context, df_GPI_RANK_RATIO_Load_DateTimeNormalize)
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
      .set("prophecy.metadata.pipeline.uri", "pipelines/altexp_gpi_rank_ratio")
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/altexp_gpi_rank_ratio") {
      apply(context)
    }
  }

}
