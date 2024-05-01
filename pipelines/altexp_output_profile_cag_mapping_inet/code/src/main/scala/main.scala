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
    val df_TSD_Dtl = TSD_Dtl(context)
    TSD_Dtl_lookup(context, df_TSD_Dtl)
    val df_TAR_Dtl                = TAR_Dtl(context)
    val df_Output_Profile_Extract = Output_Profile_Extract(context)
    val df_CAG_Override_Ref       = CAG_Override_Ref(context)
    val df_Form_ref_file_lkp      = Form_ref_file_lkp(context)
    val df_TAL_Dtl                = TAL_Dtl(context)
    val df_TAC_Dtl                = TAC_Dtl(context)
    TAC_Dtl_lookup(context, df_TAC_Dtl)
    val df_Formulary_Override_Ref = Formulary_Override_Ref(context)
    Formulary_Override_Ref_lookup(context, df_Formulary_Override_Ref)
    TAL_Dtl_lookup(context,                df_TAL_Dtl)
    Form_ref_file_lkp_lookup(context,      df_Form_ref_file_lkp)
    TAR_Dtl_lookup(context,                df_TAR_Dtl)
    CAG_Override_Ref_lookup(context,       df_CAG_Override_Ref)
    val df_Map_Cag_and_Formulary_Override_URLs =
      Map_Cag_and_Formulary_Override_URLs(context, df_Output_Profile_Extract)
    val df_Map_Cag_and_Formulary_Override_URLs_Reformat =
      Map_Cag_and_Formulary_Override_URLs_Reformat(
        context,
        df_Map_Cag_and_Formulary_Override_URLs
      )
    val (df_Separate_records_to_create_as_of_dt_reference_file_RowDistributor_out0,
         df_Separate_records_to_create_as_of_dt_reference_file_RowDistributor_out1
    ) = Separate_records_to_create_as_of_dt_reference_file_RowDistributor(
      context,
      df_Map_Cag_and_Formulary_Override_URLs_Reformat
    )
    val df_Separate_records_to_create_as_of_dt_reference_fileReformat_0_UnionAll =
      Separate_records_to_create_as_of_dt_reference_fileReformat_0_UnionAll(
        context,
        df_Separate_records_to_create_as_of_dt_reference_file_RowDistributor_out0,
        df_Separate_records_to_create_as_of_dt_reference_file_RowDistributor_out0
      )
    val df_Separate_records_to_create_as_of_dt_reference_fileReformat_0 =
      Separate_records_to_create_as_of_dt_reference_fileReformat_0(
        context,
        df_Separate_records_to_create_as_of_dt_reference_fileReformat_0_UnionAll
      )
    Master_Cag_Mapping_File(
      context,
      df_Separate_records_to_create_as_of_dt_reference_fileReformat_0
    )
    val df_Separate_records_to_create_as_of_dt_reference_fileReformat_2 =
      Separate_records_to_create_as_of_dt_reference_fileReformat_2(
        context,
        df_Separate_records_to_create_as_of_dt_reference_file_RowDistributor_out1
      )
    UDL_ref_File(context,
                 df_Separate_records_to_create_as_of_dt_reference_fileReformat_2
    )
    val df_Get_FILE_LOAD_CNTL_ID_jdbc = Get_FILE_LOAD_CNTL_ID_jdbc(context)
    val df_Generate_Records_For_Load_Ready_Files =
      Generate_Records_For_Load_Ready_Files(
        context,
        df_Separate_records_to_create_as_of_dt_reference_fileReformat_0
      )
    val df_Get_ALT_RUN_ID_jdbc = Get_ALT_RUN_ID_jdbc(context)
    val df_Get_ALT_RUN_ID_jdbcdb_predicate_pushdown =
      Get_ALT_RUN_ID_jdbcdb_predicate_pushdown(
        context,
        df_Get_ALT_RUN_ID_jdbc,
        df_Generate_Records_For_Load_Ready_Files
      )
    val df_Get_ALT_RUN_ID_jdbc_drop_duplicate =
      Get_ALT_RUN_ID_jdbc_drop_duplicate(
        context,
        df_Get_ALT_RUN_ID_jdbcdb_predicate_pushdown
      )
    val df_Get_ALT_RUN_ID = Get_ALT_RUN_ID(
      context,
      df_Generate_Records_For_Load_Ready_Files,
      df_Get_ALT_RUN_ID_jdbc_drop_duplicate
    )
    val df_Create_ALT_RUN_ALT_RUN_JOB_DETAILS_Load_Ready_FileReformat_0 =
      Create_ALT_RUN_ALT_RUN_JOB_DETAILS_Load_Ready_FileReformat_0(
        context,
        df_Get_ALT_RUN_ID
      )
    ALT_RUN_load_ready(
      context,
      df_Create_ALT_RUN_ALT_RUN_JOB_DETAILS_Load_Ready_FileReformat_0
    )
    val df_Create_ALT_RUN_ALT_RUN_JOB_DETAILS_Load_Ready_FileReformat_1 =
      Create_ALT_RUN_ALT_RUN_JOB_DETAILS_Load_Ready_FileReformat_1(
        context,
        df_Get_ALT_RUN_ID
      )
    val df_Generate_Record_For_Each_OP_and_Job_Combination =
      Generate_Record_For_Each_OP_and_Job_Combination(
        context,
        df_Create_ALT_RUN_ALT_RUN_JOB_DETAILS_Load_Ready_FileReformat_1
      )
    val df_Get_FILE_LOAD_CNTL_ID_jdbcdb_predicate_pushdown =
      Get_FILE_LOAD_CNTL_ID_jdbcdb_predicate_pushdown(
        context,
        df_Get_FILE_LOAD_CNTL_ID_jdbc,
        df_Generate_Record_For_Each_OP_and_Job_Combination
      )
    val df_Get_FILE_LOAD_CNTL_ID_jdbc_drop_duplicate =
      Get_FILE_LOAD_CNTL_ID_jdbc_drop_duplicate(
        context,
        df_Get_FILE_LOAD_CNTL_ID_jdbcdb_predicate_pushdown
      )
    val df_Get_FILE_LOAD_CNTL_ID = Get_FILE_LOAD_CNTL_ID(
      context,
      df_Generate_Record_For_Each_OP_and_Job_Combination,
      df_Get_FILE_LOAD_CNTL_ID_jdbc_drop_duplicate
    )
    ALT_RUN_JOB_DETAILS_load_ready(context, df_Get_FILE_LOAD_CNTL_ID)
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
                   "pipelines/altexp_output_profile_cag_mapping_inet"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(
      spark,
      "pipelines/altexp_output_profile_cag_mapping_inet"
    ) {
      apply(context)
    }
  }

}
