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
        col("right.nextval")
          .cast(DecimalType(16, 0))
          .as("alt_run_target_dtl_id"),
        lit(context.config.ALT_RUN_ID)
          .cast(DecimalType(16, 0))
          .as("alt_run_id"),
        col("left.tal_assoc_name").as("tal_assoc_name"),
        col("left.tala").as("tala"),
        col("left.tar_udl").as("tar_udl"),
        col("left.formulary_name").as("formulary_name"),
        col("left.target_ndc").as("target_ndc"),
        col("left.target_formulary_tier").as("target_formulary_tier"),
        col("left.target_formulary_status").as("target_formulary_status"),
        col("left.target_pa_reqd_ind").as("target_pa_reqd_ind"),
        col("left.target_step_therapy_ind").as("target_step_therapy_ind"),
        col("left.target_specialty_ind").as("target_specialty_ind"),
        col("left.target_multi_src_cd").as("target_multi_src_cd"),
        col("left.target_roa_cd").as("target_roa_cd"),
        col("left.target_dosage_form_cd").as("target_dosage_form_cd"),
        col("left.target_gpi14").as("target_gpi14"),
        col("left.target_prod_name_ext").as("target_prod_name_ext"),
        col("left.target_prod_short_desc").as("target_prod_short_desc"),
        col("left.target_gpi14_desc").as("target_gpi14_desc"),
        col("left.target_gpi8_desc").as("target_gpi8_desc"),
        col("left.target_formulary_tier_desc").as("target_formulary_tier_desc"),
        col("left.target_formulary_status_desc")
          .as("target_formulary_status_desc"),
        col("left.target_pa_type_cd").as("target_pa_type_cd"),
        col("left.target_step_therapy_type_cd")
          .as("target_step_therapy_type_cd"),
        col("left.target_step_therapy_group_name")
          .as("target_step_therapy_group_name"),
        col("left.target_step_therapy_step_num")
          .as("target_step_therapy_step_num"),
        col("left.formulary_cd").as("formulary_cd"),
        col("left.rec_crt_ts").as("rec_crt_ts"),
        col("left.rec_crt_user_id").as("rec_crt_user_id"),
        col("left.last_exp_dt").as("last_exp_dt"),
        col("left.newline").as("newline")
      )

}
