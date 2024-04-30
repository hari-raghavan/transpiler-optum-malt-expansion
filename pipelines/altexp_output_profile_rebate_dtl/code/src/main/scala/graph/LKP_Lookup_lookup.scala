package graph

import io.prophecy.libs._
import config.Context
import udfs.UDFs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object LKP_Lookup_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup(
      "LKP_Lookup",
      in,
      context.spark,
      List("output_profile_id"),
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

}
