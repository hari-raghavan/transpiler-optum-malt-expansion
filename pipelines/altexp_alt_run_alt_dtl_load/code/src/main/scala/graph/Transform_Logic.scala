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

object Transform_Logic {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      coalesce(col("alt_run_alt_dtl_id").cast(DecimalType(16, 0)), lit(-1))
        .as("alt_run_alt_dtl_id"),
      coalesce(col("alt_run_id").cast(DecimalType(16, 0)), lit(-1))
        .as("alt_run_id"),
      coalesce(col("alt_run_target_dtl_id").cast(DecimalType(16, 0)), lit(-1))
        .as("alt_run_target_dtl_id"),
      col("formulary_name"),
      col("target_ndc"),
      col("alt_ndc"),
      col("alt_formulary_tier"),
      col("alt_multi_src_cd"),
      col("alt_roa_cd"),
      col("alt_dosage_form_cd"),
      col("rank").cast(StringType).as("rank"),
      col("alt_step_therapy_ind"),
      col("alt_pa_reqd_ind"),
      col("alt_formulary_status"),
      col("alt_specialty_ind"),
      col("alt_gpi14"),
      col("alt_prod_name_ext"),
      col("alt_prod_short_desc"),
      col("alt_prod_short_desc_grp"),
      col("alt_gpi14_desc"),
      col("alt_gpi8_desc"),
      col("alt_formulary_tier_desc"),
      col("alt_formulary_status_desc"),
      col("alt_pa_type_cd"),
      col("alt_step_therapy_type_cd"),
      col("alt_step_therapy_group_name"),
      col("alt_step_therapy_step_number"),
      col("rec_crt_ts"),
      col("rec_crt_user_id"),
      col("rebate_elig_cd"),
      col("tad_eligible_cd"),
      col("alt_qty_adj").cast(DecimalType(6, 3)).as("alt_qty_adj"),
      col("tal_assoc_name"),
      col("tala"),
      col("alt_udl"),
      col("tal_assoc_rank").cast(DecimalType(38, 6)).as("tal_assoc_rank"),
      col("constituent_group"),
      col("constituent_reqd"),
      coalesce(col("newline"), lit("""
""")).as("newline")
    )

}
