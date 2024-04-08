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
    val df_IFIL_TAC_rules = IFIL_TAC_rules(context)
    val df_Convert_Target_XMLTYPE_into_DML_described_format =
      Convert_Target_XMLTYPE_into_DML_described_format(context,
                                                       df_IFIL_TAC_rules
      )
    val df_Group_Rules_for_Target_Alternative_sequence0 =
      Group_Rules_for_Target_Alternative_sequence0(
        context,
        df_Convert_Target_XMLTYPE_into_DML_described_format
      )
    val df_Convert_Alternative_XMLTYPE_into_DML_described_format =
      Convert_Alternative_XMLTYPE_into_DML_described_format(context,
                                                            df_IFIL_TAC_rules
      )
    val df_Group_Rules_for_Target_Alternative_sequence1 =
      Group_Rules_for_Target_Alternative_sequence1(
        context,
        df_Convert_Alternative_XMLTYPE_into_DML_described_format
      )
    val df_Group_Rules_for_Target_Alternative =
      Group_Rules_for_Target_Alternative(
        context,
        df_Group_Rules_for_Target_Alternative_sequence0,
        df_Group_Rules_for_Target_Alternative_sequence1
      )
    OFIL_TAC_Rule_Xwalk(context, df_Group_Rules_for_Target_Alternative)
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
                   "pipelines/altexp_tac_rules_parser"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/altexp_tac_rules_parser") {
      apply(context)
    }
  }

}
