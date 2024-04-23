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

object Separate_Target_and_Alternative_Load_Ready_FilesReformat_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("tal_id").cast(DecimalType(10, 0)).as("tal_id"),
      col("tal_name"),
      target_dtl(context).as("target_dtl"),
      col("tal_assoc_name"),
      col("tala"),
      col("tar_udl"),
      coalesce(col("ST_flag"), lit(0)).as("ST_flag"),
      col("tar_group_names"),
      transform(
        col("alt_products_def"),
        x =>
          struct(
            x.getField("alt_prd").as("alt_prd"),
            x.getField("alt_rank").as("alt_rank"),
            coalesce(x.getField("rebate_elig_cd"), lit(""))
              .as("rebate_elig_cd"),
            x.getField("tad_eli_code").as("tad_eli_code"),
            x.getField("qty_adjust_factor").as("qty_adjust_factor"),
            x.getField("tal_assoc_name").as("tal_assoc_name"),
            x.getField("tala").as("tala"),
            x.getField("udl_nm").as("udl_nm"),
            x.getField("priority").as("priority"),
            x.getField("constituent_group").as("constituent_group"),
            x.getField("constituent_reqd").as("constituent_reqd")
          )
      ).as("alt_products_def"),
      col("constituent_grp_vec"),
      coalesce(col("is_valid_rec"), lit(1)).as("is_valid_rec")
    )

  def target_dtl(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    struct(
      coalesce(col("target_dtl").getField("alt_run_target_dtl_id"), lit(-1))
        .as("alt_run_target_dtl_id"),
      coalesce(col("target_dtl").getField("alt_run_id"), lit(-1))
        .as("alt_run_id"),
      col("target_dtl").getField("tal_assoc_name").as("tal_assoc_name"),
      col("target_dtl").getField("tala").as("tala"),
      col("target_dtl").getField("tar_udl").as("tar_udl"),
      col("target_dtl").getField("formulary_name").as("formulary_name"),
      col("target_dtl").getField("target_ndc").as("target_ndc"),
      col("target_dtl")
        .getField("target_formulary_tier")
        .as("target_formulary_tier"),
      col("target_dtl")
        .getField("target_formulary_status")
        .as("target_formulary_status"),
      col("target_dtl").getField("target_pa_reqd_ind").as("target_pa_reqd_ind"),
      col("target_dtl")
        .getField("target_step_therapy_ind")
        .as("target_step_therapy_ind"),
      col("target_dtl")
        .getField("target_specialty_ind")
        .as("target_specialty_ind"),
      col("target_dtl")
        .getField("target_multi_src_cd")
        .as("target_multi_src_cd"),
      col("target_dtl").getField("target_roa_cd").as("target_roa_cd"),
      col("target_dtl")
        .getField("target_dosage_form_cd")
        .as("target_dosage_form_cd"),
      col("target_dtl").getField("target_gpi14").as("target_gpi14"),
      col("target_dtl")
        .getField("target_prod_name_ext")
        .as("target_prod_name_ext"),
      col("target_dtl")
        .getField("target_prod_short_desc")
        .as("target_prod_short_desc"),
      col("target_dtl").getField("target_gpi14_desc").as("target_gpi14_desc"),
      col("target_dtl").getField("target_gpi8_desc").as("target_gpi8_desc"),
      col("target_dtl")
        .getField("target_formulary_tier_desc")
        .as("target_formulary_tier_desc"),
      col("target_dtl")
        .getField("target_formulary_status_desc")
        .as("target_formulary_status_desc"),
      col("target_dtl").getField("target_pa_type_cd").as("target_pa_type_cd"),
      col("target_dtl")
        .getField("target_step_therapy_type_cd")
        .as("target_step_therapy_type_cd"),
      col("target_dtl")
        .getField("target_step_therapy_group_name")
        .as("target_step_therapy_group_name"),
      col("target_dtl")
        .getField("target_step_therapy_step_num")
        .as("target_step_therapy_step_num"),
      col("target_dtl").getField("formulary_cd").as("formulary_cd"),
      col("target_dtl").getField("rec_crt_ts").as("rec_crt_ts"),
      col("target_dtl").getField("rec_crt_user_id").as("rec_crt_user_id"),
      col("target_dtl").getField("last_exp_dt").as("last_exp_dt"),
      coalesce(col("target_dtl").getField("newline"),              lit("""
""")).as("newline")
    )
  }

}
