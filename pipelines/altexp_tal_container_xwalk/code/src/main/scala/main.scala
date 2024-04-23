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
    val df_LKP_TAR_Exp = LKP_TAR_Exp(context)
    LKP_TAR_Exp_lookup(context, df_LKP_TAR_Exp)
    val df_LKP_TAC_Exp = LKP_TAC_Exp(context)
    LKP_TAC_Exp_lookup(context, df_LKP_TAC_Exp)
    val df_LKP_Rule_Products = LKP_Rule_Products(context)
    LKP_Rule_Products_lookup(context, df_LKP_Rule_Products)
    val df_Expanded_UDL = Expanded_UDL(context)
    Expanded_UDL_lookup(context, df_Expanded_UDL)
    val df_Gpi_rank_ratio = Gpi_rank_ratio(context)
    Gpi_rank_ratio_lookup(context, df_Gpi_rank_ratio)
    val df_LKP_ST_GRP_NUM = LKP_ST_GRP_NUM(context)
    val df_LKP_ST_GRP_NUM_with_sequence =
      LKP_ST_GRP_NUM_with_sequence(context, df_LKP_ST_GRP_NUM)
    LKP_ST_GRP_NUM_lookup(context,          df_LKP_ST_GRP_NUM_with_sequence)
    val df_LKP_Alias_Prod = LKP_Alias_Prod(context)
    LKP_Alias_Prod_lookup(context, df_LKP_Alias_Prod)
    val df_LKP_Rebate_eligible = LKP_Rebate_eligible(context)
    LKP_Rebate_eligible_lookup(context, df_LKP_Rebate_eligible)
    val df_LKP_CLIN_INDCN = LKP_CLIN_INDCN(context)
    LKP_CLIN_INDCN_lookup(context, df_LKP_CLIN_INDCN)
    val df_LKP_Step_Grp_NM = LKP_Step_Grp_NM(context)
    LKP_Step_Grp_NM_lookup(context, df_LKP_Step_Grp_NM)
    val df_LKP_Prod = LKP_Prod(context)
    LKP_Prod_lookup(context, df_LKP_Prod)
    val df_Expanded_UDL_wt_rl_priority = Expanded_UDL_wt_rl_priority(context)
    Expanded_UDL_wt_rl_priority_lookup(context, df_Expanded_UDL_wt_rl_priority)
    val df_TAD_Xwalk = TAD_Xwalk(context)
    TAD_Xwalk_lookup(context, df_TAD_Xwalk)
    val df_LKP_Form_CAG_dataset = LKP_Form_CAG_dataset(context)
    LKP_Form_CAG_dataset_lookup(context, df_LKP_Form_CAG_dataset)
    val df_IFIL_CAG_TAL_Container = IFIL_CAG_TAL_Container(context)
    val (df_Association_Processing_out3,
         df_Association_Processing_out2,
         df_Association_Processing_out1,
         df_Association_Processing_out
    ) = Association_Processing.apply(
      Association_Processing.config
        .Context(context.spark, context.config.Association_Processing),
      df_IFIL_CAG_TAL_Container,
      df_LKP_Prod,
      df_Expanded_UDL_wt_rl_priority,
      df_LKP_Rule_Products
    )
    Target_Load_Ready(context,        df_Association_Processing_out1)
    Clinical_indn_Load_Ready(context, df_Association_Processing_out3)
    Alt_Load_Ready(context,           df_Association_Processing_out)
    Alt_Proxy_Load_Ready(context,     df_Association_Processing_out2)
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
                   "pipelines/altexp_tal_container_xwalk"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/altexp_tal_container_xwalk") {
      apply(context)
    }
  }

}
