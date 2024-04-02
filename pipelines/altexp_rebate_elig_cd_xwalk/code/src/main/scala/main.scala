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
    val df_Expanded_UDL = Expanded_UDL(context)
    Expanded_UDL_lookup(context, df_Expanded_UDL)
    val df_Rebate_Elig_cd_Extract = Rebate_Elig_cd_Extract(context)
    val df_Rebate_Elig_Cd_Crosswalk_input_select_filter =
      Rebate_Elig_Cd_Crosswalk_input_select_filter(context,
                                                   df_Rebate_Elig_cd_Extract
      )
    val df_Rebate_Elig_Cd_Crosswalk = Rebate_Elig_Cd_Crosswalk(
      context,
      df_Rebate_Elig_Cd_Crosswalk_input_select_filter
    )
    val df_Rebate_Elig_Cd_Crosswalk_Reformat =
      Rebate_Elig_Cd_Crosswalk_Reformat(context, df_Rebate_Elig_Cd_Crosswalk)
    val df_Normalize_Rebate_Eligible_Products =
      Normalize_Rebate_Eligible_Products(context,
                                         df_Rebate_Elig_Cd_Crosswalk_Reformat
      )
    OFILE_Rebate_Eligible_Xwalk(context, df_Normalize_Rebate_Eligible_Products)
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
                   "pipelines/altexp_rebate_elig_cd_xwalk"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark,
                                "pipelines/altexp_rebate_elig_cd_xwalk"
    ) {
      apply(context)
    }
  }

}
