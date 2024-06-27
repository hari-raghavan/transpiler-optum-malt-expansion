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
    val df_IFILE_Input_File_2 = IFILE_Input_File_2(context)
    val df_Create_Exp_Tab     = Create_Exp_Tab(context, df_IFILE_Input_File_2)
    val df_Create_Criteria_Tab =
      Create_Criteria_Tab(context, df_IFILE_Input_File_2)
    val df_Create_Criteria_Tab_Reformat =
      Create_Criteria_Tab_Reformat(context, df_Create_Criteria_Tab)
    val df_CONC =
      CONC(context, df_Create_Exp_Tab, df_Create_Criteria_Tab_Reformat)
    val df_Count_of_records = Count_of_records(context, df_CONC)
    val df_Count_of_records_Reformat =
      Count_of_records_Reformat(context, df_Count_of_records)
    Flag_lookup(context,                 df_Count_of_records_Reformat)
    val df_Products_2 = Products_2(context)
    Products_lookup(context, df_Products_2)
    val (df_check_records_limit_RowDistributor_out0,
         df_check_records_limit_RowDistributor_out1
    ) = check_records_limit_RowDistributor(context, df_CONC)
    val df_check_records_limitReformat_1 = check_records_limitReformat_1(
      context,
      df_check_records_limit_RowDistributor_out1
    )
    Partial_UDL_TALA_TAL_Output_2(context, df_check_records_limitReformat_1)
    Flag_2(context,                        df_Count_of_records_Reformat)
    val df_check_records_limitReformat_0 = check_records_limitReformat_0(
      context,
      df_check_records_limit_RowDistributor_out0
    )
    Write_Excel_Enrich_Product_Data_2(context, df_check_records_limitReformat_0)
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
      .set("prophecy.metadata.pipeline.uri", "pipelines/altexp_enrich_tal_exp")
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/altexp_enrich_tal_exp") {
      apply(context)
    }
  }

}
