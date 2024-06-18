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
    val df_TAL_Output_File  = TAL_Output_File(context)
    val df_Count_of_records = Count_of_records(context, df_TAL_Output_File)
    val df_Count_of_records_Reformat =
      Count_of_records_Reformat(context, df_Count_of_records)
    Flag_lookup(context,                 df_Count_of_records_Reformat)
    val (df_check_records_limit_RowDistributor_out0,
         df_check_records_limit_RowDistributor_out1
    ) = check_records_limit_RowDistributor(context, df_TAL_Output_File)
    val df_check_records_limitReformat_1 = check_records_limitReformat_1(
      context,
      df_check_records_limit_RowDistributor_out1
    )
    PartialTAL_TALA_Output(context, df_check_records_limitReformat_1)
    Flag(context,                   df_Count_of_records_Reformat)
    val df_check_records_limitReformat_0 = check_records_limitReformat_0(
      context,
      df_check_records_limit_RowDistributor_out0
    )
    Write_Excel_Enrich_Product_Data(context, df_check_records_limitReformat_0)
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
                   "pipelines/altexp_enrich_partial_exp_output_excel"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(
      spark,
      "pipelines/altexp_enrich_partial_exp_output_excel"
    ) {
      apply(context)
    }
  }

}
