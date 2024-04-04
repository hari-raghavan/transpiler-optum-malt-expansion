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

object Normalize_CAG_Data {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
      val normalize_out_DF = in.normalize(
        lengthExpression = Some(size(col("prdcts"))),
        finishedExpression = None,
        finishedCondition = None,
        alias = "index",
        colsToSelect = List(
          col("formulary_name").as("formulary_name"),
          col("carrier").as("carrier"),
          col("account").as("account"),
          col("group").as("group"),
          col("customer_name").as("customer_name"),
          col("run_eff_dt").as("run_eff_dt"),
          col("data_path").as("data_path"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("formulary_cd")).as("formulary_cd"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("ndc11")).as("ndc11"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("formulary_tier")).as("formulary_tier"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("formulary_status")).as("formulary_status"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("pa_reqd_ind")).as("pa_reqd_ind"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("specialty_ind")).as("specialty_ind"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("step_therapy_ind")).as("step_therapy_ind"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("formulary_tier_desc")).as("formulary_tier_desc"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("formulary_status_desc")).as("formulary_status_desc"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("pa_type_cd")).as("pa_type_cd"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("step_therapy_type_cd")).as("step_therapy_type_cd"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("step_therapy_group_name")).as("step_therapy_group_name"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("step_therapy_step_number")).as("step_therapy_step_number"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("last_exp_dt")).as("last_exp_dt")
        ),
        lengthRelatedGlobalExpressions = Map(),
        tempWindowExpr = Map()
      )
    
      val simpleSelect_in_DF = normalize_out_DF.select((col("run_eff_dt")).as("run_eff_dt"),
                                                       (col("customer_name")).as("customer_name"),
                                                       (col("data_path")).as("data_path")
      )
    
      val out = simpleSelect_in_DF
    out
  }

}
