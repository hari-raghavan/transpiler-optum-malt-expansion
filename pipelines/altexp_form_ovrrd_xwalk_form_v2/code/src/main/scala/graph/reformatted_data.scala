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

object reformatted_data {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("primary_data"),
      transform(col("formulary_cd"), x => x.getField("value"))
        .as("formulary_cd"),
      transform(col("ndc11"),          x => x.getField("value")).as("ndc11"),
      transform(col("formulary_tier"), x => x.getField("value"))
        .as("formulary_tier"),
      transform(col("formulary_status"), x => x.getField("value"))
        .as("formulary_status"),
      transform(col("pa_reqd_ind"),   x => x.getField("value")).as("pa_reqd_ind"),
      transform(col("specialty_ind"), x => x.getField("value"))
        .as("specialty_ind"),
      transform(col("formulary_tier_desc"), x => x.getField("value"))
        .as("formulary_tier_desc"),
      transform(col("formulary_status_desc"), x => x.getField("value"))
        .as("formulary_status_desc"),
      transform(col("pa_type_cd"),           x => x.getField("value")).as("pa_type_cd"),
      transform(col("step_therapy_type_cd"), x => x.getField("value"))
        .as("step_therapy_type_cd"),
      transform(col("step_therapy_group_name"), x => x.getField("value"))
        .as("step_therapy_group_name"),
      transform(col("step_therapy_step_number"), x => x.getField("value"))
        .as("step_therapy_step_number"),
      transform(col("last_exp_dt"), x => x.getField("value")).as("last_exp_dt")
    )

}
