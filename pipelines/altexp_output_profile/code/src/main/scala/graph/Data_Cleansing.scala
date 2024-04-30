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
      col("output_profile_id").cast(DecimalType(10, 0)).as("output_profile_id"),
      coalesce(trim(col("rxclaim_env_name")),       col("rxclaim_env_name"))
        .as("rxclaim_env_name"),
      coalesce(trim(col("formulary_name")), col("formulary_name"))
        .as("formulary_name"),
      coalesce(trim(col("formulary_id")), col("formulary_id"))
        .as("formulary_id"),
      col("output_profile_form_dtl_id")
        .cast(DecimalType(10, 0))
        .as("output_profile_form_dtl_id"),
      col("output_profile_job_dtl_id")
        .cast(DecimalType(10, 0))
        .as("output_profile_job_dtl_id"),
      coalesce(trim(col("output_profile_name")), col("output_profile_name"))
        .as("output_profile_name"),
      coalesce(trim(col("alias_name")), col("alias_name")).as("alias_name"),
      col("alias_priority").cast(StringType).as("alias_priority"),
      coalesce(trim(col("carrier")),       col("carrier")).as("carrier"),
      coalesce(trim(col("account")),       col("account")).as("account"),
      coalesce(trim(col("group")),         col("group")).as("group"),
      coalesce(trim(col("tal_name")),      col("tal_name")).as("tal_name"),
      coalesce(trim(col("tac_name")),      col("tac_name")).as("tac_name"),
      coalesce(trim(col("tar_name")),      col("tar_name")).as("tar_name"),
      coalesce(trim(col("tsd_name")),      col("tsd_name")).as("tsd_name"),
      col("job_id").cast(DecimalType(10,   0)).as("job_id"),
      coalesce(trim(col("job_name")),      col("job_name")).as("job_name"),
      coalesce(trim(col("customer_name")), col("customer_name"))
        .as("customer_name"),
      col("run_day").cast(StringType).as("run_day"),
      coalesce(trim(col("lob_name")),            col("lob_name")).as("lob_name"),
      coalesce(trim(col("run_jan1_start_mmdd")), col("run_jan1_start_mmdd"))
        .as("run_jan1_start_mmdd"),
      coalesce(trim(col("run_jan1_end_mmdd")), col("run_jan1_end_mmdd"))
        .as("run_jan1_end_mmdd"),
      coalesce(col("future_flg"),                lit(0)).as("future_flg"),
      coalesce(trim(col("formulary_pseudonym")), col("formulary_pseudonym"))
        .as("formulary_pseudonym"),
      col("notes_id").cast(DecimalType(10,       0)).as("notes_id"),
      coalesce(trim(col("output_profile_desc")), col("output_profile_desc"))
        .as("output_profile_desc"),
      col("formulary_option_cd").cast(StringType).as("formulary_option_cd"),
      coalesce(trim(col("layout_name")), col("layout_name")).as("layout_name"),
      col("as_of_dt"),
      coalesce(trim(col("st_tac_ind")), col("st_tac_ind")).as("st_tac_ind"),
      coalesce(col("newline"),          lit("""
""")).as("newline")
    )

}
