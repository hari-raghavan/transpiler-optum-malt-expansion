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
    val df_IFIL_TAD_Extract = IFIL_TAD_Extract(context)
    val df_Populate_ALT_SELECTION_CD_and_Create_TAD_Xwalk =
      Populate_ALT_SELECTION_CD_and_Create_TAD_Xwalk(context,
                                                     df_IFIL_TAD_Extract
      )
    val df_Populate_ALT_SELECTION_CD_and_Create_TAD_Xwalk_Reformat =
      Populate_ALT_SELECTION_CD_and_Create_TAD_Xwalk_Reformat(
        context,
        df_Populate_ALT_SELECTION_CD_and_Create_TAD_Xwalk
      )
    OFIL_TAD_Xwalk(context,
                   df_Populate_ALT_SELECTION_CD_and_Create_TAD_Xwalk_Reformat
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
      .set("prophecy.metadata.pipeline.uri", "pipelines/altexp_tad_xwalk")
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/altexp_tad_xwalk") {
      apply(context)
    }
  }

}
