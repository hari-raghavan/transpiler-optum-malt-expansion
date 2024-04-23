package graph.Association_Processing

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.Association_Processing.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Separate_Target_and_Alternative_Load_Ready_FilesReformat_0 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      coalesce(col("target_dtl.alt_run_target_dtl_id").cast(DecimalType(16, 0)),
               lit(-1)
      ).as("alt_run_target_dtl_id"),
      coalesce(col("target_dtl.alt_run_id").cast(DecimalType(16, 0)), lit(-1))
        .as("alt_run_id"),
      col("tal_assoc_name"),
      col("tala"),
      col("tar_udl"),
      col("target_dtl.formulary_name").as("formulary_name"),
      col("target_dtl.target_ndc").as("target_ndc"),
      col("target_dtl.target_formulary_tier").as("target_formulary_tier"),
      col("target_dtl.target_formulary_status").as("target_formulary_status"),
      col("target_dtl.target_pa_reqd_ind").as("target_pa_reqd_ind"),
      col("target_dtl.target_step_therapy_ind").as("target_step_therapy_ind"),
      col("target_dtl.target_specialty_ind").as("target_specialty_ind"),
      col("target_dtl.target_multi_src_cd").as("target_multi_src_cd"),
      col("target_dtl.target_roa_cd").as("target_roa_cd"),
      col("target_dtl.target_dosage_form_cd").as("target_dosage_form_cd"),
      col("target_dtl.target_gpi14").as("target_gpi14"),
      col("target_dtl.target_prod_name_ext").as("target_prod_name_ext"),
      col("target_dtl.target_prod_short_desc").as("target_prod_short_desc"),
      col("target_dtl.target_gpi14_desc").as("target_gpi14_desc"),
      col("target_dtl.target_gpi8_desc").as("target_gpi8_desc"),
      col("target_dtl.target_formulary_tier_desc")
        .as("target_formulary_tier_desc"),
      col("target_dtl.target_formulary_status_desc")
        .as("target_formulary_status_desc"),
      col("target_dtl.target_pa_type_cd").as("target_pa_type_cd"),
      col("target_dtl.target_step_therapy_type_cd")
        .as("target_step_therapy_type_cd"),
      col("target_dtl.target_step_therapy_group_name")
        .as("target_step_therapy_group_name"),
      col("target_dtl.target_step_therapy_step_num")
        .cast(StringType)
        .as("target_step_therapy_step_num"),
      col("target_dtl.formulary_cd").as("formulary_cd"),
      col("target_dtl.rec_crt_ts").as("rec_crt_ts"),
      col("target_dtl.rec_crt_user_id").as("rec_crt_user_id"),
      col("target_dtl.last_exp_dt").as("last_exp_dt"),
      coalesce(rpad(col("target_dtl.newline"), 1, " "), lit("""
""")).as("newline")
    )

}
