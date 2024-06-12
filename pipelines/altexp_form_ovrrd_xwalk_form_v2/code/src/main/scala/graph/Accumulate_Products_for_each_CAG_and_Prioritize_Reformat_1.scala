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

object Accumulate_Products_for_each_CAG_and_Prioritize_Reformat_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      struct(col("_id"),
             col("cag_priority"),
             col("formulary_name"),
             col("carrier"),
             col("account"),
             col("group"),
             col("customer_name"),
             col("run_eff_dt")
      ).as("primary_data"),
      struct(
        col("_id"),
        col("cag_priority"),
        transform(col("formulary_cd"), x => x.getField("value")).as("value")
      ).as("formulary_cd"),
      struct(col("_id"),
             col("cag_priority"),
             transform(col("ndc11"), x => x.getField("value")).as("value")
      ).as("ndc11"),
      struct(
        col("_id"),
        col("cag_priority"),
        transform(col("formulary_tier"), x => x.getField("value")).as("value")
      ).as("formulary_tier"),
      struct(
        col("_id"),
        col("cag_priority"),
        transform(col("formulary_status"), x => x.getField("value")).as("value")
      ).as("formulary_status"),
      struct(col("_id"),
             col("cag_priority"),
             transform(col("pa_reqd_ind"), x => x.getField("value")).as("value")
      ).as("pa_reqd_ind"),
      struct(
        col("_id"),
        col("cag_priority"),
        transform(col("specialty_ind"), x => x.getField("value")).as("value")
      ).as("specialty_ind"),
      struct(
        col("_id"),
        col("cag_priority"),
        transform(col("step_therapy_ind"), x => x.getField("value")).as("value")
      ).as("step_therapy_ind"),
      struct(col("_id"),
             col("cag_priority"),
             transform(col("formulary_tier_desc"), x => x.getField("value"))
               .as("value")
      ).as("formulary_tier_desc"),
      struct(col("_id"),
             col("cag_priority"),
             transform(col("formulary_status_desc"), x => x.getField("value"))
               .as("value")
      ).as("formulary_status_desc"),
      struct(col("_id"),
             col("cag_priority"),
             transform(col("pa_type_cd"), x => x.getField("value")).as("value")
      ).as("pa_type_cd"),
      struct(col("_id"),
             col("cag_priority"),
             transform(col("step_therapy_type_cd"), x => x.getField("value"))
               .as("value")
      ).as("step_therapy_type_cd"),
      struct(col("_id"),
             col("cag_priority"),
             transform(col("step_therapy_group_name"), x => x.getField("value"))
               .as("value")
      ).as("step_therapy_group_name"),
      struct(col("_id"),
             col("cag_priority"),
             transform(col("step_therapy_step_number"),
                       x => x.getField("value")
             ).as("value")
      ).as("step_therapy_step_number"),
      struct(col("_id"),
             col("cag_priority"),
             transform(col("last_exp_dt"), x => x.getField("value")).as("value")
      ).as("last_exp_dt")
    )

}
