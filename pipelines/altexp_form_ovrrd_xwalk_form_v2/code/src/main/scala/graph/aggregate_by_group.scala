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

object aggregate_by_group {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(
        col("primary_data.formulary_name").as("formulary_name"),
        col("primary_data.customer_name").as("customer_name"),
        col("primary_data.run_eff_dt").as(" run_eff_dt")
      )
      .agg(
        array_sort(collect_list(col("primary_data"))).as("primary_data"),
        array_sort(collect_list(col("formulary_cd"))).as("formulary_cd"),
        array_sort(collect_list(col("ndc11"))).as("ndc11"),
        array_sort(collect_list(col("formulary_tier"))).as("formulary_tier"),
        array_sort(collect_list(col("formulary_status")))
          .as("formulary_status"),
        array_sort(collect_list(col("pa_reqd_ind"))).as("pa_reqd_ind"),
        array_sort(collect_list(col("specialty_ind"))).as("specialty_ind"),
        array_sort(collect_list(col("formulary_tier_desc")))
          .as("formulary_tier_desc"),
        array_sort(collect_list(col("formulary_status_desc")))
          .as("formulary_status_desc"),
        array_sort(collect_list(col("pa_type_cd"))).as("pa_type_cd"),
        array_sort(collect_list(col("step_therapy_type_cd")))
          .as("step_therapy_type_cd"),
        array_sort(collect_list(col("step_therapy_group_name")))
          .as("step_therapy_group_name"),
        array_sort(collect_list(col("step_therapy_step_number")))
          .as("step_therapy_step_number"),
        array_sort(collect_list(col("last_exp_dt"))).as("last_exp_dt")
      )

}
