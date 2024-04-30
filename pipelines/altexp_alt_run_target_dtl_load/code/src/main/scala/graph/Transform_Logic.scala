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
      coalesce(col("alt_run_target_dtl_id").cast(DecimalType(16, 0)), lit(-1))
        .as("alt_run_target_dtl_id"),
      coalesce(col("alt_run_id").cast(DecimalType(16, 0)), lit(-1))
        .as("alt_run_id"),
      col("tal_assoc_name"),
      col("tala"),
      col("tar_udl"),
      col("formulary_name"),
      col("target_ndc"),
      col("target_formulary_tier"),
      col("target_formulary_status"),
      col("target_pa_reqd_ind"),
      col("target_step_therapy_ind"),
      col("target_specialty_ind"),
      col("target_multi_src_cd"),
      col("target_roa_cd"),
      col("target_dosage_form_cd"),
      col("target_gpi14"),
      col("target_prod_name_ext"),
      col("target_prod_short_desc"),
      col("target_gpi14_desc"),
      col("target_gpi8_desc"),
      col("target_formulary_tier_desc"),
      col("target_formulary_status_desc"),
      col("target_pa_type_cd"),
      col("target_step_therapy_type_cd"),
      col("target_step_therapy_group_name"),
      col("target_step_therapy_step_num")
        .cast(StringType)
        .as("target_step_therapy_step_num"),
      col("formulary_cd"),
      col("rec_crt_ts"),
      col("rec_crt_user_id"),
      col("last_exp_dt"),
      coalesce(col("newline"), lit("""
""")).as("newline")
    )

}
