package graph.Association_Processing.Alternative_Rollup_Processing

import io.prophecy.libs._
import graph.Association_Processing.Alternative_Rollup_Processing.config.Context
import udfs.UDFs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Normalize_Alt_Proxy_Products {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
      val normalize_out_DF = in.normalize(
        lengthExpression = Some(size(col("alt_run_alt_dtl_load_vec"))),
        finishedExpression = None,
        finishedCondition = None,
        alias = "index",
        colsToSelect = List(
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1))
            .getField("alt_run_alt_dtl_id")
            .cast(DecimalType(16, 0)))
            .as("alt_run_alt_dtl_id"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1))
            .getField("alt_run_id")
            .cast(DecimalType(16, 0)))
            .as("alt_run_id"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1))
            .getField("*")
            .cast(DecimalType(16, 0)))
            .as("alt_run_target_dtl_id"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("formulary_name")).as("formulary_name"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("target_ndc")).as("target_ndc"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("alt_ndc")).as("alt_ndc"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("alt_formulary_tier")).as("alt_formulary_tier"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("alt_multi_src_cd")).as("alt_multi_src_cd"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("alt_roa_cd")).as("alt_roa_cd"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("alt_dosage_form_cd")).as("alt_dosage_form_cd"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("rank").cast(StringType)).as("rank"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("alt_step_therapy_ind")).as("alt_step_therapy_ind"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("alt_pa_reqd_ind")).as("alt_pa_reqd_ind"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("alt_formulary_status")).as("alt_formulary_status"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("alt_specialty_ind")).as("alt_specialty_ind"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("alt_gpi14")).as("alt_gpi14"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("alt_prod_name_ext")).as("alt_prod_name_ext"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("alt_prod_short_desc")).as("alt_prod_short_desc"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("alt_prod_short_desc_grp")).as("alt_prod_short_desc_grp"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("alt_gpi14_desc")).as("alt_gpi14_desc"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("alt_gpi8_desc")).as("alt_gpi8_desc"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("alt_formulary_tier_desc")).as("alt_formulary_tier_desc"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1))
            .getField("alt_formulary_status_desc"))
            .as("alt_formulary_status_desc"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("alt_pa_type_cd")).as("alt_pa_type_cd"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("alt_step_therapy_type_cd")).as("alt_step_therapy_type_cd"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1))
            .getField("alt_step_therapy_group_name"))
            .as("alt_step_therapy_group_name"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1))
            .getField("alt_step_therapy_step_number"))
            .as("alt_step_therapy_step_number"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("rec_crt_ts")).as("rec_crt_ts"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("rec_crt_user_id")).as("rec_crt_user_id"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("rebate_elig_cd")).as("rebate_elig_cd"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("tad_eligible_cd")).as("tad_eligible_cd"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1))
            .getField("*")
            .cast(DecimalType(6, 3)))
            .as("alt_qty_adj"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("tal_assoc_name")).as("tal_assoc_name"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("tala")).as("tala"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("alt_udl")).as("alt_udl"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1))
            .getField("tal_assoc_rank")
            .cast(DecimalType(38, 6)))
            .as("tal_assoc_rank"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("constituent_group")).as("constituent_group"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("constituent_reqd")).as("constituent_reqd"),
          (element_at(col("alt_run_alt_dtl_load_vec"), col("index") + lit(1)).getField("newline").cast(StringType)).as("newline")
        ),
        lengthRelatedGlobalExpressions = Map(),
        tempWindowExpr = Map()
      )
    
      val simpleSelect_in_DF = normalize_out_DF.select(
        (col("alt_run_alt_dtl_id")).as("alt_run_alt_dtl_id"),
        (col("alt_run_id")).as("alt_run_id"),
        (col("alt_run_target_dtl_id")).as("alt_run_target_dtl_id"),
        (col("formulary_name")).as("formulary_name"),
        (col("target_ndc")).as("target_ndc"),
        (col("alt_ndc")).as("alt_ndc"),
        (col("alt_formulary_tier")).as("alt_formulary_tier"),
        (col("alt_multi_src_cd")).as("alt_multi_src_cd"),
        (col("alt_roa_cd")).as("alt_roa_cd"),
        (col("alt_dosage_form_cd")).as("alt_dosage_form_cd"),
        (col("rank")).as("rank"),
        (col("alt_step_therapy_ind")).as("alt_step_therapy_ind"),
        (col("alt_pa_reqd_ind")).as("alt_pa_reqd_ind"),
        (col("alt_formulary_status")).as("alt_formulary_status"),
        (col("alt_specialty_ind")).as("alt_specialty_ind"),
        (col("alt_gpi14")).as("alt_gpi14"),
        (col("alt_prod_name_ext")).as("alt_prod_name_ext"),
        (col("alt_prod_short_desc")).as("alt_prod_short_desc"),
        (col("alt_prod_short_desc_grp")).as("alt_prod_short_desc_grp"),
        (col("alt_gpi14_desc")).as("alt_gpi14_desc"),
        (col("alt_gpi8_desc")).as("alt_gpi8_desc"),
        (col("alt_formulary_tier_desc")).as("alt_formulary_tier_desc"),
        (col("alt_formulary_status_desc")).as("alt_formulary_status_desc"),
        (col("alt_pa_type_cd")).as("alt_pa_type_cd"),
        (col("alt_step_therapy_type_cd")).as("alt_step_therapy_type_cd"),
        (col("alt_step_therapy_group_name")).as("alt_step_therapy_group_name"),
        (col("alt_step_therapy_step_number")).as("alt_step_therapy_step_number"),
        (col("rec_crt_ts")).as("rec_crt_ts"),
        (col("rec_crt_user_id")).as("rec_crt_user_id"),
        (col("rebate_elig_cd")).as("rebate_elig_cd"),
        (col("tad_eligible_cd")).as("tad_eligible_cd"),
        (col("alt_qty_adj")).as("alt_qty_adj"),
        (col("tal_assoc_name")).as("tal_assoc_name"),
        (col("tala")).as("tala"),
        (col("alt_udl")).as("alt_udl"),
        (col("tal_assoc_rank")).as("tal_assoc_rank"),
        (col("constituent_group")).as("constituent_group"),
        (col("constituent_reqd")).as("constituent_reqd"),
        (col("newline")).as("newline")
      )
    
      val out = simpleSelect_in_DF
    out
  }

}
