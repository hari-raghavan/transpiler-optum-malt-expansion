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

object Accumulate_Products_for_each_CAG_and_Prioritize {

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
        collect_list(
          struct(
            col("formulary_cd"),
            col("ndc11"),
            col("formulary_tier"),
            col("formulary_status"),
            col("pa_reqd_ind"),
            col("specialty_ind"),
            col("step_therapy_ind"),
            col("formulary_tier_desc"),
            col("formulary_status_desc"),
            col("pa_type_cd"),
            coalesce(col("step_therapy_type_cd"), lit(""))
              .as("step_therapy_type_cd"),
            coalesce(col("step_therapy_group_name"), lit(""))
              .as("step_therapy_group_name"),
            coalesce(col("step_therapy_step_number").cast(StringType), lit(""))
              .as("step_therapy_step_number"),
            col("last_exp_dt")
          )
        ).as("prdcts")
      )

}
