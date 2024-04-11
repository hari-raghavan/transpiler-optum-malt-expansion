package graph.Association_Processing

import io.prophecy.libs._
import graph.Association_Processing.config.Context
import udfs.UDFs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Normalize_TAL_Target_Drugs_Without_Alternatives {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
      val normalizeDF = in.normalize(
        lengthExpression = Some(size(col("ta_prdct_dtls_wo_alt"))),
        finishedExpression = None,
        finishedCondition = None,
        alias = "index",
        colsToSelect = List(
          (
            struct(
              col("tal_assoc_name").as("tal_assoc_name"),
              col("tar_udl_nm").as("tar_udl"),
              lit(Config.FORMULARY_NM).as("formulary_name"),
              lookup("LKP_Prod", element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit"))
                .getField("ndc11")
                .as("target_ndc"),
              when(
                !isnull(
                  lookup("LKP_Prod",
                         element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                  )
                ),
                lookup("LKP_Form_CAG_dataset",
                       lookup("LKP_Prod",
                              element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                       ).getField("ndc11")
                ).getField("formulary_tier")
              ).otherwise(lit(null)).as("target_formulary_tier"),
              when(
                !isnull(
                  lookup("LKP_Prod",
                         element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                  )
                ),
                lookup("LKP_Form_CAG_dataset",
                       lookup("LKP_Prod",
                              element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                       ).getField("ndc11")
                ).getField("formulary_status")
              ).otherwise(lit(null)).as("target_formulary_status"),
              when(
                !isnull(
                  lookup("LKP_Prod",
                         element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                  )
                ),
                lookup("LKP_Form_CAG_dataset",
                       lookup("LKP_Prod",
                              element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                       ).getField("ndc11")
                ).getField("pa_reqd_ind")
              ).otherwise(lit(null)).as("target_pa_reqd_ind"),
              when(
                !isnull(
                  lookup("LKP_Prod",
                         element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                  )
                ),
                lookup("LKP_Form_CAG_dataset",
                       lookup("LKP_Prod",
                              element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                       ).getField("ndc11")
                ).getField("step_therapy_ind")
              ).otherwise(lit(null)).as("target_step_therapy_ind"),
              when(
                !isnull(
                  lookup("LKP_Prod",
                         element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                  )
                ),
                lookup("LKP_Form_CAG_dataset",
                       lookup("LKP_Prod",
                              element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                       ).getField("ndc11")
                ).getField("specialty_ind")
              ).otherwise(lit(null)).as("target_specialty_ind"),
              lookup("LKP_Prod", element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit"))
                .getField("msc")
                .as("target_multi_src_cd"),
              lookup("LKP_Prod", element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit"))
                .getField("roa_cd")
                .as("target_roa_cd"),
              lookup("LKP_Prod", element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit"))
                .getField("dosage_form_cd")
                .as("target_dosage_form_cd"),
              when(is_blank(
                     lookup("LKP_Prod",
                            element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                     ).getField("gpi14")
                   ),
                   lit(" ")
              ).otherwise(
                  lookup("LKP_Prod",
                         element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                  ).getField("gpi14")
                )
                .as("target_gpi14"),
              when(is_blank(
                     lookup("LKP_Prod",
                            element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                     ).getField("drug_name")
                   ),
                   lit(" ")
              ).otherwise(
                  lookup("LKP_Prod",
                         element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                  ).getField("drug_name")
                )
                .as("target_prod_name_ext"),
              lookup("LKP_Prod", element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit"))
                .getField("prod_short_desc")
                .as("target_prod_short_desc"),
              lookup("LKP_Prod", element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit"))
                .getField("gpi14_desc")
                .as("target_gpi14_desc"),
              lookup("LKP_Prod", element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit"))
                .getField("gpi8_desc")
                .as("target_gpi8_desc"),
              when(
                !isnull(
                  lookup("LKP_Prod",
                         element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                  )
                ),
                lookup("LKP_Form_CAG_dataset",
                       lookup("LKP_Prod",
                              element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                       ).getField("ndc11")
                ).getField("formulary_tier_desc")
              ).otherwise(lit(null)).as("target_formulary_tier_desc"),
              when(
                !isnull(
                  lookup("LKP_Prod",
                         element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                  )
                ),
                lookup("LKP_Form_CAG_dataset",
                       lookup("LKP_Prod",
                              element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                       ).getField("ndc11")
                ).getField("formulary_status_desc")
              ).otherwise(lit(null)).as("target_formulary_status_desc"),
              when(
                !isnull(
                  lookup("LKP_Prod",
                         element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                  )
                ),
                lookup("LKP_Form_CAG_dataset",
                       lookup("LKP_Prod",
                              element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                       ).getField("ndc11")
                ).getField("pa_type_cd")
              ).otherwise(lit(null)).as("target_pa_type_cd"),
              when(
                !isnull(
                  lookup("LKP_Prod",
                         element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                  )
                ),
                lookup("LKP_Form_CAG_dataset",
                       lookup("LKP_Prod",
                              element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                       ).getField("ndc11")
                ).getField("step_therapy_type_cd")
              ).otherwise(lit(null)).as("target_step_therapy_type_cd"),
              when(
                !isnull(
                  lookup("LKP_Prod",
                         element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                  )
                ),
                lookup("LKP_Form_CAG_dataset",
                       lookup("LKP_Prod",
                              element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                       ).getField("ndc11")
                ).getField("step_therapy_group_name")
              ).otherwise(lit(null)).as("target_step_therapy_group_name"),
              when(
                !isnull(
                  lookup("LKP_Prod",
                         element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                  )
                ),
                lookup("LKP_Form_CAG_dataset",
                       lookup("LKP_Prod",
                              element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                       ).getField("ndc11")
                ).getField("step_therapy_step_number")
              ).otherwise(lit(null)).cast(StringType).as("target_step_therapy_step_num"),
              when(
                !isnull(
                  lookup("LKP_Prod",
                         element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                  )
                ),
                lookup("LKP_Form_CAG_dataset",
                       lookup("LKP_Prod",
                              element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                       ).getField("ndc11")
                ).getField("formulary_cd")
              ).otherwise(lit("")).as("formulary_cd"),
              lit(Config.RUN_TS).as("rec_crt_ts"),
              lit(Config.USER_ID).as("rec_crt_user_id"),
              when(
                !isnull(
                  lookup("LKP_Prod",
                         element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                  )
                ),
                lookup("LKP_Form_CAG_dataset",
                       lookup("LKP_Prod",
                              element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                       ).getField("ndc11")
                ).getField("last_exp_dt")
              ).otherwise(lit(null)).as("last_exp_dt")
            )
          ).as("alt_run_target_dtl_load"),
          (when(
            !isnull(
              lookup("LKP_Prod", element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit"))
            ),
            when(
              isnull(
                lookup("LKP_Form_CAG_dataset",
                       lookup("LKP_Prod",
                              element_at(col("ta_prdct_dtls_wo_alt"), col("index") + lit(1)).getField("target_dl_bit")
                       ).getField("ndc11")
                )
              ),
              lit(0)
            ).otherwise(lit(1))
          ).otherwise(lit(0)).cast(DecimalType(1, 0))).as("is_valid_rec")
        ),
        lengthRelatedGlobalExpressions = Map(),
        tempWindowExpr = Map()
      )
    
      val simpleSelect_in_DF = normalizeDF.select((col("alt_run_target_dtl_load")).as("alt_run_target_dtl_load"),
                                                  (col("is_valid_rec")).as("is_valid_rec")
      )
    
      val normalize_out_DF = simpleSelect_in_DF.filter(col("is_valid_rec"))
    
      val out = normalize_out_DF
    out
  }

}
