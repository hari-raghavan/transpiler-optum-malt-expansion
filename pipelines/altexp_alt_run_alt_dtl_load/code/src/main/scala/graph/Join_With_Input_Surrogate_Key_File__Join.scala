package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Join_With_Input_Surrogate_Key_File__Join {

  def apply(context: Context, left: DataFrame, right: DataFrame): DataFrame =
    left
      .as("left")
      .join(
        right.as("right"),
        (col("left.formulary_name") === col("right.formulary_name"))
          .and(col("left.tal_assoc_name") === col("right.tal_assoc_name"))
          .and(col("left.target_ndc") === col("right.target_ndc")),
        "inner"
      )
      .select(
        coalesce(col("right.alt_run_alt_dtl_id").cast(DecimalType(16, 0)),
                 lit(-1)
        ).as("alt_run_alt_dtl_id"),
        coalesce(lit(context.config.ALT_RUN_ID).cast(DecimalType(16, 0)),
                 lit(-1)
        ).as("alt_run_id"),
        coalesce(col("left.alt_run_target_dtl_id").cast(DecimalType(16, 0)),
                 lit(-1)
        ).as("alt_run_target_dtl_id"),
        col("right.formulary_name").as("formulary_name"),
        col("right.target_ndc").as("target_ndc"),
        col("right.alt_ndc").as("alt_ndc"),
        col("right.alt_formulary_tier").as("alt_formulary_tier"),
        col("right.alt_multi_src_cd").as("alt_multi_src_cd"),
        col("right.alt_roa_cd").as("alt_roa_cd"),
        col("right.alt_dosage_form_cd").as("alt_dosage_form_cd"),
        col("right.rank").cast(StringType).as("rank"),
        col("right.alt_step_therapy_ind").as("alt_step_therapy_ind"),
        col("right.alt_pa_reqd_ind").as("alt_pa_reqd_ind"),
        col("right.alt_formulary_status").as("alt_formulary_status"),
        col("right.alt_specialty_ind").as("alt_specialty_ind"),
        col("right.alt_gpi14").as("alt_gpi14"),
        col("right.alt_prod_name_ext").as("alt_prod_name_ext"),
        col("right.alt_prod_short_desc").as("alt_prod_short_desc"),
        col("right.alt_prod_short_desc_grp").as("alt_prod_short_desc_grp"),
        col("right.alt_gpi14_desc").as("alt_gpi14_desc"),
        col("right.alt_gpi8_desc").as("alt_gpi8_desc"),
        col("right.alt_formulary_tier_desc").as("alt_formulary_tier_desc"),
        col("right.alt_formulary_status_desc").as("alt_formulary_status_desc"),
        col("right.alt_pa_type_cd").as("alt_pa_type_cd"),
        col("right.alt_step_therapy_type_cd").as("alt_step_therapy_type_cd"),
        col("right.alt_step_therapy_group_name")
          .as("alt_step_therapy_group_name"),
        col("right.alt_step_therapy_step_number")
          .as("alt_step_therapy_step_number"),
        col("right.rec_crt_ts").as("rec_crt_ts"),
        col("right.rec_crt_user_id").as("rec_crt_user_id"),
        col("right.rebate_elig_cd").as("rebate_elig_cd"),
        col("right.tad_eligible_cd").as("tad_eligible_cd"),
        col("right.alt_qty_adj").cast(DecimalType(6, 3)).as("alt_qty_adj"),
        col("right.tal_assoc_name").as("tal_assoc_name"),
        col("right.tala").as("tala"),
        col("right.alt_udl").as("alt_udl"),
        col("right.tal_assoc_rank")
          .cast(DecimalType(38, 6))
          .as("tal_assoc_rank"),
        col("right.constituent_group").as("constituent_group"),
        col("right.constituent_reqd").as("constituent_reqd"),
        coalesce(col("right.newline").cast(StringType), lit("""
""")).as("newline")
      )

}
