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

object LKP_Form_CAG_dataset_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup(
      "LKP_Form_CAG_dataset",
      in,
      context.spark,
      List("ndc11"),
      "formulary_name",
      "formulary_cd",
      "carrier",
      "account",
      "group",
      "last_exp_dt",
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

}
