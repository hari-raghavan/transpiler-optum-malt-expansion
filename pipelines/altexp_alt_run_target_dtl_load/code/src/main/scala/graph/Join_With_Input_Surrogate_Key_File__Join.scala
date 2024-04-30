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
      .join(right.as("right"),
            col("left.newline") === col("right.newline"),
            "inner"
      )
      .select(
        coalesce(col("right.alt_run_target_dtl_id").cast(DecimalType(16, 0)),
                 lit(-1)
        ).as("alt_run_target_dtl_id"),
        coalesce(col("right.alt_run_id").cast(DecimalType(16, 0)), lit(-1))
          .as("alt_run_id"),
        col("right.tal_assoc_name").as("tal_assoc_name"),
        col("right.tala").as("tala"),
        col("right.tar_udl").as("tar_udl"),
        col("right.formulary_name").as("formulary_name"),
        col("right.target_ndc").as("target_ndc"),
        col("right.target_formulary_tier").as("target_formulary_tier"),
        col("right.target_formulary_status").as("target_formulary_status"),
        col("right.target_pa_reqd_ind").as("target_pa_reqd_ind"),
        col("right.target_step_therapy_ind").as("target_step_therapy_ind"),
        col("right.target_specialty_ind").as("target_specialty_ind"),
        col("right.target_multi_src_cd").as("target_multi_src_cd"),
        col("right.target_roa_cd").as("target_roa_cd"),
        col("right.target_dosage_form_cd").as("target_dosage_form_cd"),
        col("right.target_gpi14").as("target_gpi14"),
        col("right.target_prod_name_ext").as("target_prod_name_ext"),
        col("right.target_prod_short_desc").as("target_prod_short_desc"),
        col("right.target_gpi14_desc").as("target_gpi14_desc"),
        col("right.target_gpi8_desc").as("target_gpi8_desc"),
        col("right.target_formulary_tier_desc")
          .as("target_formulary_tier_desc"),
        col("right.target_formulary_status_desc")
          .as("target_formulary_status_desc"),
        col("right.target_pa_type_cd").as("target_pa_type_cd"),
        col("right.target_step_therapy_type_cd")
          .as("target_step_therapy_type_cd"),
        col("right.target_step_therapy_group_name")
          .as("target_step_therapy_group_name"),
        col("right.target_step_therapy_step_num")
          .cast(StringType)
          .as("target_step_therapy_step_num"),
        col("right.formulary_cd").as("formulary_cd"),
        col("right.rec_crt_ts").as("rec_crt_ts"),
        col("right.rec_crt_user_id").as("rec_crt_user_id"),
        col("right.last_exp_dt").as("last_exp_dt"),
        coalesce(col("left.newline").cast(StringType), lit("""
""")).as("newline")
      )

}
