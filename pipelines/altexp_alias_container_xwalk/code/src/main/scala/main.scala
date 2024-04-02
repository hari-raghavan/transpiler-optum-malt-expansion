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
    val df_Alias_Xwalk = Alias_Xwalk(context)
    Alias_Xwalk_lookup(context, df_Alias_Xwalk)
    val df_Product_File      = Product_File(context)
    val df_Apply_alias_rules = Apply_alias_rules(context, df_Product_File)
    Write_Aliased_product_files(context, df_Apply_alias_rules)
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
      .newSession()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri",
                   "pipelines/altexp_alias_container_xwalk"
    )
    registerUDFs(spark)
    try MetricsCollector.start(spark,
                               "pipelines/altexp_alias_container_xwalk",
                               context.config
    )
    catch {
      case _: Throwable =>
        MetricsCollector.start(spark, "pipelines/altexp_alias_container_xwalk")
    }
    apply(context)
    MetricsCollector.end(spark)
  }

}
