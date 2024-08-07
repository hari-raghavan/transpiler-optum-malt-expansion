package io.prophecy.pipelines.altexp_tac_rules_parser_2

import io.prophecy.libs._
import io.prophecy.pipelines.altexp_tac_rules_parser_2.config._
import io.prophecy.pipelines.altexp_tac_rules_parser_2.udfs.UDFs._
import io.prophecy.pipelines.altexp_tac_rules_parser_2.udfs.PipelineInitCode._
import io.prophecy.pipelines.altexp_tac_rules_parser_2.graph._
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
    val df_calculate_offset =
      calculate_offset(context, df_Group_Rules_for_Target_Alternative)
    val df_reformat_tac_data = reformat_tac_data(context, df_calculate_offset)
    OFIL_TAC_Rule_Xwalk(context, df_reformat_tac_data)
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
                   "pipelines/altexp_tac_rules_parser_2"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/altexp_tac_rules_parser_2") {
      apply(context)
    }
  }

}
