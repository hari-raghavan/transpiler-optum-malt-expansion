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

object Get_Unique_Target_Products {

  def apply(context: Context, left: DataFrame, right: DataFrame): DataFrame =
    left
      .as("left")
      .join(
        right.as("right"),
        (col("left.alt_run_target_dtl_load.tal_assoc_name") === col(
          "right.tal_assoc_name"
        )).and(
          col("left.alt_run_target_dtl_load.target_ndc") === col(
            "right.target_ndc"
          )
        ),
        "inner"
      )
      .select(
        coalesce(
          coalesce(col("right.alt_run_target_dtl_id"),
                   col("left.alt_run_target_dtl_load.alt_run_target_dtl_id")
          ),
          lit(-1)
        ).as("alt_run_target_dtl_id"),
        coalesce(coalesce(col("right.alt_run_id"),
                          col("left.alt_run_target_dtl_load.alt_run_id")
                 ),
                 lit(-1)
        ).as("alt_run_id"),
        coalesce(col("right.tal_assoc_name"),
                 col("left.alt_run_target_dtl_load.tal_assoc_name")
        ).as("tal_assoc_name"),
        coalesce(col("right.tala"), col("left.alt_run_target_dtl_load.tala"))
          .as("tala"),
        coalesce(col("right.tar_udl"),
                 col("left.alt_run_target_dtl_load.tar_udl")
        ).as("tar_udl"),
        coalesce(col("right.formulary_name"),
                 col("left.alt_run_target_dtl_load.formulary_name")
        ).as("formulary_name"),
        coalesce(col("right.target_ndc"),
                 col("left.alt_run_target_dtl_load.target_ndc")
        ).as("target_ndc"),
        coalesce(col("right.target_formulary_tier"),
                 col("left.alt_run_target_dtl_load.target_formulary_tier")
        ).as("target_formulary_tier"),
        coalesce(col("right.target_formulary_status"),
                 col("left.alt_run_target_dtl_load.target_formulary_status")
        ).as("target_formulary_status"),
        coalesce(col("right.target_pa_reqd_ind"),
                 col("left.alt_run_target_dtl_load.target_pa_reqd_ind")
        ).as("target_pa_reqd_ind"),
        coalesce(col("right.target_step_therapy_ind"),
                 col("left.alt_run_target_dtl_load.target_step_therapy_ind")
        ).as("target_step_therapy_ind"),
        coalesce(col("right.target_specialty_ind"),
                 col("left.alt_run_target_dtl_load.target_specialty_ind")
        ).as("target_specialty_ind"),
        coalesce(col("right.target_multi_src_cd"),
                 col("left.alt_run_target_dtl_load.target_multi_src_cd")
        ).as("target_multi_src_cd"),
        coalesce(col("right.target_roa_cd"),
                 col("left.alt_run_target_dtl_load.target_roa_cd")
        ).as("target_roa_cd"),
        coalesce(col("right.target_dosage_form_cd"),
                 col("left.alt_run_target_dtl_load.target_dosage_form_cd")
        ).as("target_dosage_form_cd"),
        coalesce(col("right.target_gpi14"),
                 col("left.alt_run_target_dtl_load.target_gpi14")
        ).as("target_gpi14"),
        coalesce(col("right.target_prod_name_ext"),
                 col("left.alt_run_target_dtl_load.target_prod_name_ext")
        ).as("target_prod_name_ext"),
        coalesce(col("right.target_prod_short_desc"),
                 col("left.alt_run_target_dtl_load.target_prod_short_desc")
        ).as("target_prod_short_desc"),
        coalesce(col("right.target_gpi14_desc"),
                 col("left.alt_run_target_dtl_load.target_gpi14_desc")
        ).as("target_gpi14_desc"),
        coalesce(col("right.target_gpi8_desc"),
                 col("left.alt_run_target_dtl_load.target_gpi8_desc")
        ).as("target_gpi8_desc"),
        coalesce(col("right.target_formulary_tier_desc"),
                 col("left.alt_run_target_dtl_load.target_formulary_tier_desc")
        ).as("target_formulary_tier_desc"),
        coalesce(
          col("right.target_formulary_status_desc"),
          col("left.alt_run_target_dtl_load.target_formulary_status_desc")
        ).as("target_formulary_status_desc"),
        coalesce(col("right.target_pa_type_cd"),
                 col("left.alt_run_target_dtl_load.target_pa_type_cd")
        ).as("target_pa_type_cd"),
        coalesce(col("right.target_step_therapy_type_cd"),
                 col("left.alt_run_target_dtl_load.target_step_therapy_type_cd")
        ).as("target_step_therapy_type_cd"),
        coalesce(
          col("right.target_step_therapy_group_name"),
          col("left.alt_run_target_dtl_load.target_step_therapy_group_name")
        ).as("target_step_therapy_group_name"),
        coalesce(
          col("right.target_step_therapy_step_num"),
          col("left.alt_run_target_dtl_load.target_step_therapy_step_num")
        ).as("target_step_therapy_step_num"),
        coalesce(col("right.formulary_cd"),
                 col("left.alt_run_target_dtl_load.formulary_cd")
        ).as("formulary_cd"),
        coalesce(col("right.rec_crt_ts"),
                 col("left.alt_run_target_dtl_load.rec_crt_ts")
        ).as("rec_crt_ts"),
        coalesce(col("right.rec_crt_user_id"),
                 col("left.alt_run_target_dtl_load.rec_crt_user_id")
        ).as("rec_crt_user_id"),
        coalesce(col("right.last_exp_dt"),
                 col("left.alt_run_target_dtl_load.last_exp_dt")
        ).as("last_exp_dt"),
        coalesce(coalesce(col("right.newline"),
                          col("left.alt_run_target_dtl_load.newline")
                 ),
                 lit("""
""")
        ).as("newline")
      )

}
