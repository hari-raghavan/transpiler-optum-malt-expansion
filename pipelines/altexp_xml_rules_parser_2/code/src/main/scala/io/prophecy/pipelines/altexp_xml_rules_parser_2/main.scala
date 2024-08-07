package io.prophecy.pipelines.altexp_xml_rules_parser_2

import io.prophecy.libs._
import io.prophecy.pipelines.altexp_xml_rules_parser_2.config._
import io.prophecy.pipelines.altexp_xml_rules_parser_2.udfs.UDFs._
import io.prophecy.pipelines.altexp_xml_rules_parser_2.udfs.PipelineInitCode._
import io.prophecy.pipelines.altexp_xml_rules_parser_2.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_IFILE_UDL_rules = IFILE_UDL_rules(context)
    val df_Convert_XMLTYPE_into_DML_described_format =
      Convert_XMLTYPE_into_DML_described_format(context, df_IFILE_UDL_rules)
    val df_Group_the_Rules_and_assign_priority =
      Group_the_Rules_and_assign_priority(
        context,
        df_Convert_XMLTYPE_into_DML_described_format
      )
    val df_RLP_List_qualifiers_per_UDL_and_set_override_flag =
      RLP_List_qualifiers_per_UDL_and_set_override_flag(
        context,
        df_Group_the_Rules_and_assign_priority
      )
    val df_RLP_List_qualifiers_per_UDL_and_set_override_flag_Reformat =
      RLP_List_qualifiers_per_UDL_and_set_override_flag_Reformat(
        context,
        df_RLP_List_qualifiers_per_UDL_and_set_override_flag
      )
    OFILE_UDL_Master_CrossWalk(
      context,
      df_RLP_List_qualifiers_per_UDL_and_set_override_flag_Reformat
    )
    val df_Normalize_grouped_rules =
      Normalize_grouped_rules(context, df_Group_the_Rules_and_assign_priority)
    val df_Sort_Rules_Per_UDL =
      Sort_Rules_Per_UDL(context, df_Normalize_grouped_rules)
    OFILE_Rule_CrossWalk(context, df_Sort_Rules_Per_UDL)
    val df_Ignore_rule_priority =
      Ignore_rule_priority(context,                     df_Normalize_grouped_rules)
    OFILE_Rule_CrossWalk_Without_Rule_Priority(context, df_Ignore_rule_priority)
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
                   "pipelines/altexp_xml_rules_parser_2"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/altexp_xml_rules_parser_2") {
      apply(context)
    }
  }

}
