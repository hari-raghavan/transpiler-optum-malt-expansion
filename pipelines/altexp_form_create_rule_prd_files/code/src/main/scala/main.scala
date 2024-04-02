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
    val df_LKP_Product_Lookup = LKP_Product_Lookup(context)
    LKP_Product_Lookup_lookup(context, df_LKP_Product_Lookup)
    val df_Form_CAG_dataset = Form_CAG_dataset(context)
    val df_Generate_Record_for_Qualifier_and_their_Values =
      Generate_Record_for_Qualifier_and_their_Values(context,
                                                     df_Form_CAG_dataset
      )
    val df_Sort_QualVal =
      Sort_QualVal(context, df_Generate_Record_for_Qualifier_and_their_Values)
    val df_Create_Rule_Dataset_For_EQ_Operator =
      Create_Rule_Dataset_For_EQ_Operator(context, df_Sort_QualVal)
    val df_Create_Rule_Dataset_For_EQ_Operator_Reformat =
      Create_Rule_Dataset_For_EQ_Operator_Reformat(
        context,
        df_Create_Rule_Dataset_For_EQ_Operator
      )
    Form_Rule_Products(context, df_Create_Rule_Dataset_For_EQ_Operator_Reformat)
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
                   "pipelines/altexp_form_create_rule_prd_files"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark,
                                "pipelines/altexp_form_create_rule_prd_files"
    ) {
      apply(context)
    }
  }

}
