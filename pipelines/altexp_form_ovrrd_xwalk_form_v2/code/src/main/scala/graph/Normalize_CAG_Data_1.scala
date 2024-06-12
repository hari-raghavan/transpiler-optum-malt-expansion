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

object Normalize_CAG_Data_1 {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
      val out = in.select(
        col("formulary_name"),
        col("formulary_cd"),
        col("carrier"),
        col("account"),
        col("group"),
        col("last_exp_dt"),
        col("ndc11"),
        col("formulary_tier"),
        col("formulary_status"),
        col("pa_reqd_ind"),
        col("specialty_ind"),
        col("step_therapy_ind"),
        col("formulary_tier_desc"),
        col("formulary_status_desc"),
        col("pa_type_cd"),
        col("step_therapy_type_cd"),
        col("step_therapy_group_name"),
        col("step_therapy_step_number"),
        lit("\n").as("newline"),
        col("run_eff_dt"),
        col("customer_name"),
        op_fl_nn_condition(Config.AI_SERIAL_HOME, 
                           Config.OUTPUT_FILE_PREFIX,
                           Config.ENV_NM,
                           Config.BUSINESS_DATE).as("data_path"),
      )
    out
  }

}
