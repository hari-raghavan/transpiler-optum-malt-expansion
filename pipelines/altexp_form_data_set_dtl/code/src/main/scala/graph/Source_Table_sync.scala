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

object Source_Table_sync {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
      val out = in.syncDataFrameColumnsWithSchema(columnNames =
        List(
          "formulary_data_set_id",
          "formulary_name",
          "formulary_id",
          "formulary_cd",
          "carrier",
          "account",
          "group",
          "rxclaim_env_name",
          "customer_name",
          "last_exp_dt",
          "run_eff_dt",
          "formulary_data_set_dtl_id",
          "ndc11",
          "formulary_tier",
          "formulary_status",
          "pa_reqd_ind",
          "specialty_ind",
          "step_therapy_ind",
          "formulary_tier_desc",
          "formulary_status_desc",
          "pa_type_cd",
          "step_therapy_type_cd",
          "step_therapy_group_name",
          "step_therapy_step_number",
          "newline"
        )
      )
    out
  }

}
