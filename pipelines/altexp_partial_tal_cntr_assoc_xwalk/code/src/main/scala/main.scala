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
    if (
      _root_.io.prophecy.abinitio.ScalaFunctions.convertToBoolean(
        _root_.io.prophecy.abinitio.ScalaFunctions
          ._is_blank(context.config.TAL_ASSOC_NAME)
      )
    )
      TAL_Container_Assoc.apply(
        TAL_Container_Assoc.config
          .Context(context.spark, context.config.TAL_Container_Assoc)
      )
    TAL_Assoc_Crosswalk.apply(
      TAL_Assoc_Crosswalk.config
        .Context(context.spark, context.config.TAL_Assoc_Crosswalk)
    )
    val df_Expanded_UDL = Expanded_UDL(context)
    Expanded_UDL_lookup(context, df_Expanded_UDL)
    if (
      _root_.io.prophecy.abinitio.ScalaFunctions.convertToBoolean(
        _root_.io.prophecy.abinitio.ScalaFunctions
          ._is_blank(context.config.TAL_ASSOC_NAME)
      )
    )
      TAL_DTL_Crosswalk.apply(
        TAL_DTL_Crosswalk.config
          .Context(context.spark, context.config.TAL_DTL_Crosswalk)
      )
    val df_LKP_CLIN_INDCN = LKP_CLIN_INDCN(context)
    LKP_CLIN_INDCN_lookup(context, df_LKP_CLIN_INDCN)
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
                   "pipelines/altexp_partial_tal_cntr_assoc_xwalk"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark,
                                "pipelines/altexp_partial_tal_cntr_assoc_xwalk"
    ) {
      apply(context)
    }
  }

}
