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
    val df_CAG_dataset        = CAG_dataset(context)
    val df_Select_valid_drugs = Select_valid_drugs(context, df_CAG_dataset)
    val df_Sort_NDC           = Sort_NDC(context,           df_Select_valid_drugs)
    val df_Generate_Record_for_Qualifier_and_their_Values =
      Generate_Record_for_Qualifier_and_their_Values(context, df_Sort_NDC)
    val df_Sort_QualVal =
      Sort_QualVal(context, df_Generate_Record_for_Qualifier_and_their_Values)
    val df_Create_Rule_Dataset_For_EQ_Operator =
      Create_Rule_Dataset_For_EQ_Operator(context, df_Sort_QualVal)
    val df_Create_Rule_Dataset_For_EQ_Operator_Reformat =
      Create_Rule_Dataset_For_EQ_Operator_Reformat(
        context,
        df_Create_Rule_Dataset_For_EQ_Operator
      )
    val (df_Separate_data_for_Different_Operators_RowDistributor_out0,
         df_Separate_data_for_Different_Operators_RowDistributor_out1,
         df_Separate_data_for_Different_Operators_RowDistributor_out2,
         df_Separate_data_for_Different_Operators_RowDistributor_out3,
         df_Separate_data_for_Different_Operators_RowDistributor_out4,
         df_Separate_data_for_Different_Operators_RowDistributor_out5,
         df_Separate_data_for_Different_Operators_RowDistributor_out6
    ) = Separate_data_for_Different_Operators_RowDistributor(
      context,
      df_Create_Rule_Dataset_For_EQ_Operator_Reformat
    )
    val df_Separate_data_for_Different_OperatorsReformat_1 =
      Separate_data_for_Different_OperatorsReformat_1(
        context,
        df_Separate_data_for_Different_Operators_RowDistributor_out1
      )
    val df_Separate_data_for_Different_OperatorsReformat_2 =
      Separate_data_for_Different_OperatorsReformat_2(
        context,
        df_Separate_data_for_Different_Operators_RowDistributor_out2
      )
    val df_Separate_data_for_Different_OperatorsReformat_3 =
      Separate_data_for_Different_OperatorsReformat_3(
        context,
        df_Separate_data_for_Different_Operators_RowDistributor_out3
      )
    val df_Separate_data_for_Different_OperatorsReformat_4 =
      Separate_data_for_Different_OperatorsReformat_4(
        context,
        df_Separate_data_for_Different_Operators_RowDistributor_out4
      )
    val df_Separate_data_for_Different_OperatorsReformat_5 =
      Separate_data_for_Different_OperatorsReformat_5(
        context,
        df_Separate_data_for_Different_Operators_RowDistributor_out5
      )
    val df_Separate_data_for_Different_OperatorsReformat_6 =
      Separate_data_for_Different_OperatorsReformat_6(
        context,
        df_Separate_data_for_Different_Operators_RowDistributor_out6
      )
    val df_Agg_same_group_products_UnionAll = Agg_same_group_products_UnionAll(
      context,
      df_Separate_data_for_Different_OperatorsReformat_2,
      df_Separate_data_for_Different_OperatorsReformat_3,
      df_Separate_data_for_Different_OperatorsReformat_4,
      df_Separate_data_for_Different_OperatorsReformat_5,
      df_Separate_data_for_Different_OperatorsReformat_6
    )
    val df_Agg_same_group_products =
      Agg_same_group_products(context, df_Agg_same_group_products_UnionAll)
    val df_Agg_same_group_products_Reformat =
      Agg_same_group_products_Reformat(context, df_Agg_same_group_products)
    val df_Create_Product_Lookup = Create_Product_Lookup(context, df_Sort_NDC)
    val df_Create_All_Dataset_Rule =
      Create_All_Dataset_Rule(context, df_Create_Product_Lookup)
    val df_Create_All_Dataset_Rule_Reformat =
      Create_All_Dataset_Rule_Reformat(context, df_Create_All_Dataset_Rule)
    val df_Separate_data_for_Different_OperatorsReformat_0 =
      Separate_data_for_Different_OperatorsReformat_0(
        context,
        df_Separate_data_for_Different_Operators_RowDistributor_out0
      )
    val df_Create_Rule_Dataset_for_NE_Operator =
      Create_Rule_Dataset_for_NE_Operator(
        context,
        df_Create_All_Dataset_Rule_Reformat,
        df_Separate_data_for_Different_OperatorsReformat_0
      )
    val df_Gather = Gather(context,
                           df_Separate_data_for_Different_OperatorsReformat_1,
                           df_Agg_same_group_products_Reformat,
                           df_Create_Rule_Dataset_for_NE_Operator
    )
    Rule_Products(context, df_Gather)
    Products(context,      df_Create_Product_Lookup)
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
                   "pipelines/altexp_create_rule_prd_files"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark,
                                "pipelines/altexp_create_rule_prd_files"
    ) {
      apply(context)
    }
  }

}
