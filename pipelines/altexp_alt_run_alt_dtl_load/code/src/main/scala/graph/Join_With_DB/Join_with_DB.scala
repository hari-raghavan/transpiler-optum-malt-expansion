package graph.Join_With_DB

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.Join_With_DB.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Join_with_DB {

  def apply(context: Context, left: DataFrame, right: DataFrame): DataFrame =
    left
      .as("left")
      .join(right.as("right"), lit(true), "left_outer")
      .select(
        col("right.nextval").cast(DecimalType(16,   0)).as("alt_run_alt_dtl_id"),
        col("left.alt_run_id").cast(DecimalType(16, 0)).as("alt_run_id"),
        col("left.alt_run_target_dtl_id").as("alt_run_target_dtl_id"),
        col("left.formulary_name").as("formulary_name"),
        col("left.target_ndc").as("target_ndc"),
        col("left.alt_ndc").as("alt_ndc"),
        col("left.alt_formulary_tier").as("alt_formulary_tier"),
        col("left.alt_multi_src_cd").as("alt_multi_src_cd"),
        col("left.alt_roa_cd").as("alt_roa_cd"),
        col("left.alt_dosage_form_cd").as("alt_dosage_form_cd"),
        col("left.rank").as("rank"),
        col("left.alt_step_therapy_ind").as("alt_step_therapy_ind"),
        col("left.alt_pa_reqd_ind").as("alt_pa_reqd_ind"),
        col("left.alt_formulary_status").as("alt_formulary_status"),
        col("left.alt_specialty_ind").as("alt_specialty_ind"),
        col("left.alt_gpi14").as("alt_gpi14"),
        col("left.alt_prod_name_ext").as("alt_prod_name_ext"),
        col("left.alt_prod_short_desc").as("alt_prod_short_desc"),
        col("left.alt_prod_short_desc_grp").as("alt_prod_short_desc_grp"),
        col("left.alt_gpi14_desc").as("alt_gpi14_desc"),
        col("left.alt_gpi8_desc").as("alt_gpi8_desc"),
        col("left.alt_formulary_tier_desc").as("alt_formulary_tier_desc"),
        col("left.alt_formulary_status_desc").as("alt_formulary_status_desc"),
        col("left.alt_pa_type_cd").as("alt_pa_type_cd"),
        col("left.alt_step_therapy_type_cd").as("alt_step_therapy_type_cd"),
        col("left.alt_step_therapy_group_name")
          .as("alt_step_therapy_group_name"),
        col("left.alt_step_therapy_step_number")
          .as("alt_step_therapy_step_number"),
        col("left.rec_crt_ts").as("rec_crt_ts"),
        col("left.rec_crt_user_id").as("rec_crt_user_id"),
        col("left.rebate_elig_cd").as("rebate_elig_cd"),
        col("left.tad_eligible_cd").as("tad_eligible_cd"),
        col("left.alt_qty_adj").as("alt_qty_adj"),
        col("left.tal_assoc_name").as("tal_assoc_name"),
        col("left.tala").as("tala"),
        col("left.alt_udl").as("alt_udl"),
        col("left.tal_assoc_rank").as("tal_assoc_rank"),
        col("left.constituent_group").as("constituent_group"),
        col("left.constituent_reqd").as("constituent_reqd"),
        col("left.newline").as("newline")
      )

}
