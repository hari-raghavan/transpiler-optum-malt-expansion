package graph.Association_Processing.Alternative_Rollup_Processing

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.Association_Processing.Alternative_Rollup_Processing.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Identify_Alt_Proxy_Targets_w_Constituents_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(alt_run_alt_dtl_load_vec(context).as("alt_run_alt_dtl_load_vec"))

  def alt_run_alt_dtl_load_vec(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    transform(
      col("alt_run_alt_dtl_load_vec"),
      x =>
        struct(
          coalesce(x.getField("alt_run_alt_dtl_id"), lit(-1))
            .as("alt_run_alt_dtl_id"),
          coalesce(x.getField("alt_run_id"),            lit(-1)).as("alt_run_id"),
          coalesce(x.getField("alt_run_target_dtl_id"), lit(-1))
            .as("alt_run_target_dtl_id"),
          x.getField("formulary_name").as("formulary_name"),
          x.getField("target_ndc").as("target_ndc"),
          x.getField("alt_ndc").as("alt_ndc"),
          x.getField("alt_formulary_tier").as("alt_formulary_tier"),
          x.getField("alt_multi_src_cd").as("alt_multi_src_cd"),
          x.getField("alt_roa_cd").as("alt_roa_cd"),
          x.getField("alt_dosage_form_cd").as("alt_dosage_form_cd"),
          x.getField("rank").as("rank"),
          x.getField("alt_step_therapy_ind").as("alt_step_therapy_ind"),
          x.getField("alt_pa_reqd_ind").as("alt_pa_reqd_ind"),
          x.getField("alt_formulary_status").as("alt_formulary_status"),
          x.getField("alt_specialty_ind").as("alt_specialty_ind"),
          x.getField("alt_gpi14").as("alt_gpi14"),
          x.getField("alt_prod_name_ext").as("alt_prod_name_ext"),
          x.getField("alt_prod_short_desc").as("alt_prod_short_desc"),
          x.getField("alt_prod_short_desc_grp").as("alt_prod_short_desc_grp"),
          x.getField("alt_gpi14_desc").as("alt_gpi14_desc"),
          x.getField("alt_gpi8_desc").as("alt_gpi8_desc"),
          x.getField("alt_formulary_tier_desc").as("alt_formulary_tier_desc"),
          x.getField("alt_formulary_status_desc")
            .as("alt_formulary_status_desc"),
          x.getField("alt_pa_type_cd").as("alt_pa_type_cd"),
          x.getField("alt_step_therapy_type_cd").as("alt_step_therapy_type_cd"),
          x.getField("alt_step_therapy_group_name")
            .as("alt_step_therapy_group_name"),
          x.getField("alt_step_therapy_step_number")
            .as("alt_step_therapy_step_number"),
          x.getField("rec_crt_ts").as("rec_crt_ts"),
          x.getField("rec_crt_user_id").as("rec_crt_user_id"),
          x.getField("rebate_elig_cd").as("rebate_elig_cd"),
          x.getField("tad_eligible_cd").as("tad_eligible_cd"),
          x.getField("alt_qty_adj").as("alt_qty_adj"),
          x.getField("tal_assoc_name").as("tal_assoc_name"),
          x.getField("tala").as("tala"),
          x.getField("alt_udl").as("alt_udl"),
          x.getField("tal_assoc_rank").as("tal_assoc_rank"),
          x.getField("constituent_group").as("constituent_group"),
          x.getField("constituent_reqd").as("constituent_reqd"),
          coalesce(x.getField("newline"),                        lit("""
""")).as("newline")
        )
    )
  }

}
