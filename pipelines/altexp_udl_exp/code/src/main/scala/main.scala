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
    val df_LKP_Rule_Products = LKP_Rule_Products(context)
    LKP_Rule_Products_lookup(context, df_LKP_Rule_Products)
    val df_LKP_UDL_Master_CrossWalk = LKP_UDL_Master_CrossWalk(context)
    LKP_UDL_Master_CrossWalk_lookup(context, df_LKP_UDL_Master_CrossWalk)
    val df_IFILE_Rule_CrossWalk = IFILE_Rule_CrossWalk(context)
    val df_Get_Products_Filter_select =
      Get_Products_Filter_select(context, df_IFILE_Rule_CrossWalk)
    val df_Get_Products_Reformat =
      Get_Products_Reformat(context, df_Get_Products_Filter_select)
    val df_RLP_Aggregate_Products_at_rule_level =
      RLP_Aggregate_Products_at_rule_level(context, df_Get_Products_Reformat)
    val df_RLP_Aggregate_Products_at_rule_level_Reformat =
      RLP_Aggregate_Products_at_rule_level_Reformat(
        context,
        df_RLP_Aggregate_Products_at_rule_level
      )
    val df_RLP_Aggregate_Products_at_rule_level_output_select_filter =
      RLP_Aggregate_Products_at_rule_level_output_select_filter(
        context,
        df_RLP_Aggregate_Products_at_rule_level_Reformat
      )
    val df_RLP_Aggregate_Products_at_UDL_level =
      RLP_Aggregate_Products_at_UDL_level(
        context,
        df_RLP_Aggregate_Products_at_rule_level_output_select_filter
      )
    val df_RLP_Aggregate_Products_at_UDL_level_Reformat =
      RLP_Aggregate_Products_at_UDL_level_Reformat(
        context,
        df_RLP_Aggregate_Products_at_UDL_level
      )
    OFILE_Current_snapshot_of_Baseline_CAG(
      context,
      df_RLP_Aggregate_Products_at_UDL_level_Reformat
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/altexp_udl_exp")
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/altexp_udl_exp") {
      apply(context)
    }
  }

}
