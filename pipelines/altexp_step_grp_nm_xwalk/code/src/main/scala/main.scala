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
    val df_IFIL_Formulary_Dataset = IFIL_Formulary_Dataset(context)
    val df_Select_Records_With_Valid_Step_Number_Value =
      Select_Records_With_Valid_Step_Number_Value(context,
                                                  df_IFIL_Formulary_Dataset
      )
    val df_Collect_Step_Group_Names_and_Create_Crosswalk =
      Collect_Step_Group_Names_and_Create_Crosswalk(
        context,
        df_Select_Records_With_Valid_Step_Number_Value
      )
    val df_Collect_Step_Group_Names_and_Create_Crosswalk_Reformat =
      Collect_Step_Group_Names_and_Create_Crosswalk_Reformat(
        context,
        df_Collect_Step_Group_Names_and_Create_Crosswalk
      )
    OFIL_Step_Group_Name_Xwalk(
      context,
      df_Collect_Step_Group_Names_and_Create_Crosswalk_Reformat
    )
    val df_IFIL_TAC_Rule_Xwalk = IFIL_TAC_Rule_Xwalk(context)
    val df_Collect_ST_Group_Numbers_for_Target_Alternatives_Filter_output_index =
      Collect_ST_Group_Numbers_for_Target_Alternatives_Filter_output_index(
        context,
        df_IFIL_TAC_Rule_Xwalk
      )
    val df_Collect_ST_Group_Numbers_for_Target_Alternatives_Filter_select =
      Collect_ST_Group_Numbers_for_Target_Alternatives_Filter_select(
        context,
        df_Collect_ST_Group_Numbers_for_Target_Alternatives_Filter_output_index
      )
    val df_Collect_ST_Group_Numbers_for_Target_Alternatives_Reformat =
      Collect_ST_Group_Numbers_for_Target_Alternatives_Reformat(
        context,
        df_Collect_ST_Group_Numbers_for_Target_Alternatives_Filter_select
      )
    OFIL_Step_Group_Number_Xwalk(
      context,
      df_Collect_ST_Group_Numbers_for_Target_Alternatives_Reformat
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
    spark.conf.set("prophecy.metadata.pipeline.uri",
                   "pipelines/altexp_step_grp_nm_xwalk"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/altexp_step_grp_nm_xwalk") {
      apply(context)
    }
  }

}
