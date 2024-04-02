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
    val df_Form_Rule_Products = Form_Rule_Products(context)
    Form_Rule_Products_lookup(context, df_Form_Rule_Products)
    val df_IFILE_TSD_Extract = IFILE_TSD_Extract(context)
    val df_Expand_TSD_Filter_select =
      Expand_TSD_Filter_select(context, df_IFILE_TSD_Extract)
    val df_Expand_TSD_Reformat =
      Expand_TSD_Reformat(context, df_Expand_TSD_Filter_select)
    val df_Aggregate_products_for_a_TSD_Code =
      Aggregate_products_for_a_TSD_Code(context, df_Expand_TSD_Reformat)
    val df_Aggregate_products_for_a_TSD_Code_Reformat =
      Aggregate_products_for_a_TSD_Code_Reformat(
        context,
        df_Aggregate_products_for_a_TSD_Code
      )
    TSD_Products(context, df_Aggregate_products_for_a_TSD_Code_Reformat)
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
      .set("prophecy.metadata.pipeline.uri", "pipelines/altexp_form_tsd_xwalk")
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/altexp_form_tsd_xwalk") {
      apply(context)
    }
  }

}
