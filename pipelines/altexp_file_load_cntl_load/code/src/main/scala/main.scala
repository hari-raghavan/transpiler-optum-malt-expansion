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
    val df_Unload_Job_Details = Unload_Job_Details(context)
    val df_Unload_Job_Details_sync =
      Unload_Job_Details_sync(context, df_Unload_Job_Details)
    val df_Populate_Audit_Fields =
      Populate_Audit_Fields(context,           df_Unload_Job_Details_sync)
    Load_data_to_File_Load_Cntl_table(context, df_Populate_Audit_Fields)
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
                   "pipelines/altexp_file_load_cntl_load"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/altexp_file_load_cntl_load") {
      apply(context)
    }
  }

}
