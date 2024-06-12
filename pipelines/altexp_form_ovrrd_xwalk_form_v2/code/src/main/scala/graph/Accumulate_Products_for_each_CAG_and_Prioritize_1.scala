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

object Accumulate_Products_for_each_CAG_and_Prioritize_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("carrier"), col("account"), col("group"))
      .agg(
        last(col("formulary_name")).as("formulary_name"),
        last(col("customer_name")).as("customer_name"),
        last(col("run_eff_dt")).as("run_eff_dt"),
        max(lit("")).as("data_path"),
        when(max(col("carrier").isNull),             max(lit(1)))
          .when(max(col("carrier") === lit("*ALL")), max(lit(2)))
          .when(max(
                  !col("carrier").isNull
                    .and(col("account") === lit("*ALL"))
                    .and(col("group") === lit("*ALL"))
                ),
                max(lit(3))
          )
          .when(max(
                  !col("carrier").isNull
                    .and(!col("account").isNull)
                    .and(col("group") === lit("*ALL"))
                ),
                max(lit(4))
          )
          .otherwise(max(lit(5)))
          .as("cag_priority"),
        sort_array(
          collect_list(struct(col("_id"), col("formulary_cd").as("value")))
        ).as("formulary_cd"),
        sort_array(collect_list(struct(col("_id"), col("ndc11").as("value"))))
          .as("ndc11"),
        sort_array(
          collect_list(struct(col("_id"), col("formulary_tier").as("value")))
        ).as("formulary_tier"),
        sort_array(
          collect_list(struct(col("_id"), col("formulary_status").as("value")))
        ).as("formulary_status"),
        sort_array(
          collect_list(struct(col("_id"), col("pa_reqd_ind").as("value")))
        ).as("pa_reqd_ind"),
        sort_array(
          collect_list(struct(col("_id"), col("specialty_ind").as("value")))
        ).as("specialty_ind"),
        sort_array(
          collect_list(struct(col("_id"), col("step_therapy_ind").as("value")))
        ).as("step_therapy_ind"),
        sort_array(
          collect_list(
            struct(col("_id"), col("formulary_tier_desc").as("value"))
          )
        ).as("formulary_tier_desc"),
        sort_array(
          collect_list(
            struct(col("_id"), col("formulary_status_desc").as("value"))
          )
        ).as("formulary_status_desc"),
        sort_array(
          collect_list(struct(col("_id"), col("pa_type_cd").as("value")))
        ).as("pa_type_cd"),
        sort_array(
          collect_list(
            struct(col("_id"),
                   coalesce(col("step_therapy_type_cd"), lit("")).as("value")
            )
          )
        ).as("step_therapy_type_cd"),
        sort_array(
          collect_list(
            struct(col("_id"),
                   coalesce(col("step_therapy_group_name"), lit("")).as("value")
            )
          )
        ).as("step_therapy_group_name"),
        sort_array(
          collect_list(
            struct(col("_id"),
                   coalesce(col("step_therapy_step_number").cast(StringType),
                            lit("")
                   ).as("value")
            )
          )
        ).as("step_therapy_step_number"),
        sort_array(
          collect_list(struct(col("_id"), col("last_exp_dt").as("value")))
        ).as("last_exp_dt"),
        last(col("_id")).as("_id")
      )

}
