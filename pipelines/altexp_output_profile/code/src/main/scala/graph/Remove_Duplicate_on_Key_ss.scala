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

object Remove_Duplicate_on_Key_ss {

  def apply(context: Context, in: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.Window
    in.withColumn(
        "row_number",
        row_number().over(
          Window
            .partitionBy(
              "output_profile_id",
              "rxclaim_env_name",
              "formulary_name",
              "formulary_id",
              "output_profile_form_dtl_id",
              "output_profile_job_dtl_id",
              "output_profile_name",
              "alias_name",
              "alias_priority",
              "carrier",
              "account",
              "group",
              "tal_name",
              "tac_name",
              "tar_name",
              "tsd_name",
              "job_id",
              "job_name",
              "customer_name",
              "run_day",
              "lob_name",
              "run_jan1_start_mmdd",
              "run_jan1_end_mmdd",
              "future_flg",
              "formulary_pseudonym",
              "notes_id",
              "output_profile_desc",
              "formulary_option_cd",
              "layout_name",
              "as_of_dt",
              "st_tac_ind",
              "newline"
            )
            .orderBy(
              col("carrier").asc,
              col("account").asc,
              col("group").asc,
              col("future_flg").asc,
              to_date(col("as_of_dt"), "yyyyMMdd").asc,
              col("output_profile_id").asc,
              col("alias_priority").asc
            )
        )
      )
      .filter(col("row_number") === lit(1))
      .drop("row_number")
  }

}
