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
    val df_LKP_Lookup4  = LKP_Lookup4(context)
    val df_LKP_Lookup14 = LKP_Lookup14(context)
    if (
      _root_.io.prophecy.abinitio.ScalaFunctions._not(
        _root_.io.prophecy.abinitio.ScalaFunctions.convertToBoolean(
          _root_.io.prophecy.abinitio.ScalaFunctions
            ._is_blank(context.config.LKP_FILE)
        )
      )
    )
      LKP_Lookup_lookup(context, df_LKP_Lookup4)
    if (
      _root_.io.prophecy.abinitio.ScalaFunctions._not(
        _root_.io.prophecy.abinitio.ScalaFunctions.convertToBoolean(
          _root_.io.prophecy.abinitio.ScalaFunctions
            ._is_blank(context.config.LKP_FILE1)
        )
      )
    )
      LKP_Lookup1_lookup(context, df_LKP_Lookup14)
    val df_Source_Table4     = Source_Table4(context)
    val df_Source_Table_sync = Source_Table_sync(context, df_Source_Table4)
    val df_Data_Cleansing    = Data_Cleansing(context,    df_Source_Table_sync)
    val (df_Filter_Condition_out0, df_Filter_Condition_out1) =
      Filter_Condition(context, df_Data_Cleansing)
    val df_Sort_Data_on_Key_ss =
      if (
        _root_.io.prophecy.abinitio.ScalaFunctions._not(
          _root_.io.prophecy.abinitio.ScalaFunctions.convertToBoolean(
            _root_.io.prophecy.abinitio.ScalaFunctions
              ._is_blank(context.config.SORT_KEY)
          )
        )
      ) Sort_Data_on_Key_ss(context, df_Filter_Condition_out0)
      else df_Filter_Condition_out0
    val df_Remove_Duplicate_on_Key_ss =
      if (
        _root_.io.prophecy.abinitio.ScalaFunctions._not(
          _root_.io.prophecy.abinitio.ScalaFunctions.convertToBoolean(
            _root_.io.prophecy.abinitio.ScalaFunctions
              ._is_blank(context.config.DEDUP_KEY)
          )
        )
      ) Remove_Duplicate_on_Key_ss(context, df_Sort_Data_on_Key_ss)
      else df_Sort_Data_on_Key_ss
    OFILE4(context,       df_Remove_Duplicate_on_Key_ss)
    REJECT_FILE4(context, df_Filter_Condition_out1)
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/altexp_tac_dtl")
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/altexp_tac_dtl") {
      apply(context)
    }
  }

}