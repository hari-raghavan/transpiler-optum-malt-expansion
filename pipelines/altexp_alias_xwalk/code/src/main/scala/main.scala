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
    val df_Alias_Extract = Alias_Extract(context)
    val df_Rearrange_on_sequence =
      Rearrange_on_sequence(context, df_Alias_Extract)
    val df_Accumulate_alias_info_for_each_alias_name =
      Accumulate_alias_info_for_each_alias_name(context,
                                                df_Rearrange_on_sequence
      )
    val df_Accumulate_alias_info_for_each_alias_name_Reformat =
      Accumulate_alias_info_for_each_alias_name_Reformat(
        context,
        df_Accumulate_alias_info_for_each_alias_name
      )
    Alias_Xwalk(context, df_Accumulate_alias_info_for_each_alias_name_Reformat)
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
      .set("prophecy.metadata.pipeline.uri", "pipelines/altexp_alias_xwalk")
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/altexp_alias_xwalk") {
      apply(context)
    }
  }

}
