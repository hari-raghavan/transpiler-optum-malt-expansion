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
    val df_Accumulate_Products_for_each_CAG_and_Prioritize =
      Accumulate_Products_for_each_CAG_and_Prioritize(context,
                                                      df_IFIL_Extract_Data
      )
    val df_Accumulate_Products_for_each_CAG_and_Prioritize_Reformat =
      Accumulate_Products_for_each_CAG_and_Prioritize_Reformat(
        context,
        df_Accumulate_Products_for_each_CAG_and_Prioritize
      )
    val df_Sorting_on_Priority = Sorting_on_Priority(
      context,
      df_Accumulate_Products_for_each_CAG_and_Prioritize_Reformat
    )
    val df_Create_Product_Crosswalk_For_Different_Levels =
      Create_Product_Crosswalk_For_Different_Levels(context,
                                                    df_Sorting_on_Priority
      )
    val df_Normalize_CAG_Data = Normalize_CAG_Data(
      context,
      df_Create_Product_Crosswalk_For_Different_Levels
    )
    val df_Reference_File_Creation = Reference_File_Creation(
      context,
      df_Create_Product_Crosswalk_For_Different_Levels
    )
    Reference_File(context,          df_Reference_File_Creation)
    Write_Data_at_CAG_level(context, df_Normalize_CAG_Data)
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
                   "pipelines/altexp_form_ovrrd_xwalk_form"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark,
                                "pipelines/altexp_form_ovrrd_xwalk_form"
    ) {
      apply(context)
    }
  }

}
