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

object Data_Cleansing {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("formulary_data_set_id")
        .cast(DecimalType(10, 0))
        .as("formulary_data_set_id"),
      coalesce(trim(col("formulary_name")), col("formulary_name"))
        .as("formulary_name"),
      coalesce(trim(col("formulary_id")), col("formulary_id"))
        .as("formulary_id"),
      coalesce(coalesce(trim(col("formulary_cd")), col("formulary_cd")),
               lit("")
      ).as("formulary_cd"),
      coalesce(trim(col("carrier")),          col("carrier")).as("carrier"),
      coalesce(trim(col("account")),          col("account")).as("account"),
      coalesce(trim(col("group")),            col("group")).as("group"),
      coalesce(trim(col("rxclaim_env_name")), col("rxclaim_env_name"))
        .as("rxclaim_env_name"),
      coalesce(trim(col("customer_name")), col("customer_name"))
        .as("customer_name"),
      col("last_exp_dt"),
      col("run_eff_dt"),
      col("formulary_data_set_dtl_id")
        .cast(DecimalType(10, 0))
        .as("formulary_data_set_dtl_id"),
      coalesce(trim(col("ndc11")),          col("ndc11")).as("ndc11"),
      coalesce(trim(col("formulary_tier")), col("formulary_tier"))
        .as("formulary_tier"),
      coalesce(trim(col("formulary_status")), col("formulary_status"))
        .as("formulary_status"),
      coalesce(trim(col("pa_reqd_ind")),   col("pa_reqd_ind")).as("pa_reqd_ind"),
      coalesce(trim(col("specialty_ind")), col("specialty_ind"))
        .as("specialty_ind"),
      coalesce(trim(col("step_therapy_ind")), col("step_therapy_ind"))
        .as("step_therapy_ind"),
      coalesce(trim(col("formulary_tier_desc")), col("formulary_tier_desc"))
        .as("formulary_tier_desc"),
      coalesce(trim(col("formulary_status_desc")), col("formulary_status_desc"))
        .as("formulary_status_desc"),
      coalesce(trim(col("pa_type_cd")),           col("pa_type_cd")).as("pa_type_cd"),
      coalesce(trim(col("step_therapy_type_cd")), col("step_therapy_type_cd"))
        .as("step_therapy_type_cd"),
      coalesce(trim(col("step_therapy_group_name")),
               col("step_therapy_group_name")
      ).as("step_therapy_group_name"),
      col("step_therapy_step_number")
        .cast(StringType)
        .as("step_therapy_step_number"),
      coalesce(col("newline"), lit("""
""")).as("newline")
    )

}
