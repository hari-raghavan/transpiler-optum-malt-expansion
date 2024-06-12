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
    val df_IFIL_Extract_Data = IFIL_Extract_Data(context)
    val df_add_id_column     = add_id_column(context, df_IFIL_Extract_Data)
    val df_Accumulate_Products_for_each_CAG_and_Prioritize_1 =
      Accumulate_Products_for_each_CAG_and_Prioritize_1(context,
                                                        df_add_id_column
      )
    val df_Accumulate_Products_for_each_CAG_and_Prioritize_Reformat_1 =
      Accumulate_Products_for_each_CAG_and_Prioritize_Reformat_1(
        context,
        df_Accumulate_Products_for_each_CAG_and_Prioritize_1
      )
    val df_aggregate_by_group = aggregate_by_group(
      context,
      df_Accumulate_Products_for_each_CAG_and_Prioritize_Reformat_1
    )
    val df_Reformat_1           = Reformat_1(context,           df_aggregate_by_group)
    val df_flatten_primary_data = flatten_primary_data(context, df_Reformat_1)
    val df_reformatted_data     = reformatted_data(context,     df_aggregate_by_group)
    val df_reformat_primary_data =
      reformat_primary_data(context, df_flatten_primary_data)
    val df_Create_Product_Crosswalk_For_Different_Levels_2 =
      Create_Product_Crosswalk_For_Different_Levels_2(context,
                                                      df_reformat_primary_data
      )
    val df_Reference_File_Creation = Reference_File_Creation(
      context,
      df_Create_Product_Crosswalk_For_Different_Levels_2
    )
    Reference_File(context, df_Reference_File_Creation)
    val df_Create_Product_Crosswalk_For_Different_Levels_1 =
      Create_Product_Crosswalk_For_Different_Levels_1(context,
                                                      df_reformatted_data
      )
    val df_Normalize_CAG_Data_1 = Normalize_CAG_Data_1(
      context,
      df_Create_Product_Crosswalk_For_Different_Levels_1
    )
    Write_Data_at_CAG_level(context, df_Normalize_CAG_Data_1)
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
                   "pipelines/altexp_form_ovrrd_xwalk_form_v2"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark,
                                "pipelines/altexp_form_ovrrd_xwalk_form_v2"
    ) {
      apply(context)
    }
  }

}
