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

object Normalize_TAL_Target_Drugs {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
      val normalizeDF = in.normalize(
        lengthExpression = Some(size(col("ta_prdct_dtls"))),
        finishedExpression = None,
        finishedCondition = None,
        alias = "index",
        colsToSelect = List(
          (
            struct(
              lit(Config.FORMULARY_NM).as("formulary_name"),
              lookup("LKP_Prod", element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("target_dl_bit"))
                .getField("ndc11")
                .as("target_ndc"),
              lookup("LKP_Form_CAG_dataset",
                     lookup("LKP_Prod", element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("target_dl_bit"))
                       .getField("ndc11")
              ).getField("formulary_tier").as("target_formulary_tier"),
              lookup("LKP_Form_CAG_dataset",
                     lookup("LKP_Prod", element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("target_dl_bit"))
                       .getField("ndc11")
              ).getField("formulary_status").as("target_formulary_status"),
              lookup("LKP_Form_CAG_dataset",
                     lookup("LKP_Prod", element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("target_dl_bit"))
                       .getField("ndc11")
              ).getField("pa_reqd_ind").as("target_pa_reqd_ind"),
              lookup("LKP_Form_CAG_dataset",
                     lookup("LKP_Prod", element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("target_dl_bit"))
                       .getField("ndc11")
              ).getField("step_therapy_ind").as("target_step_therapy_ind"),
              lookup("LKP_Form_CAG_dataset",
                     lookup("LKP_Prod", element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("target_dl_bit"))
                       .getField("ndc11")
              ).getField("specialty_ind").as("target_specialty_ind"),
              lookup("LKP_Prod", element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("target_dl_bit"))
                .getField("msc")
                .as("target_multi_src_cd"),
              lookup("LKP_Prod", element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("target_dl_bit"))
                .getField("roa_cd")
                .as("target_roa_cd"),
              lookup("LKP_Prod", element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("target_dl_bit"))
                .getField("dosage_form_cd")
                .as("target_dosage_form_cd"),
              when(is_blank(
                     lookup("LKP_Prod", element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("target_dl_bit"))
                       .getField("gpi14")
                   ),
                   lit(" ")
              ).otherwise(
                  lookup("LKP_Prod", element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("target_dl_bit"))
                    .getField("gpi14")
                )
                .as("target_gpi14"),
              when(is_blank(
                     lookup("LKP_Prod", element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("target_dl_bit"))
                       .getField("drug_name")
                   ),
                   lit(" ")
              ).otherwise(
                  lookup("LKP_Prod", element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("target_dl_bit"))
                    .getField("drug_name")
                )
                .as("target_prod_name_ext"),
              lookup("LKP_Prod", element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("target_dl_bit"))
                .getField("prod_short_desc")
                .as("target_prod_short_desc"),
              lookup("LKP_Prod", element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("target_dl_bit"))
                .getField("gpi14_desc")
                .as("target_gpi14_desc"),
              lookup("LKP_Prod", element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("target_dl_bit"))
                .getField("gpi8_desc")
                .as("target_gpi8_desc"),
              when(
                length(
                  concat(
                    when(!isnull(lookup(
                        "LKP_Prod",
                        element_at(col("ta_prdct_dtls"), col("index") + lit(1))
                          .getField("target_dl_bit")
                      )),
                      lookup(
                        "LKP_Form_CAG_dataset",
                        lookup(
                          "LKP_Prod",
                          element_at(col("ta_prdct_dtls"), col("index") + lit(1))
                            .getField("target_dl_bit")
                        ).getField("ndc11")
                      ).getField("formulary_tier_desc")).otherwise(lit(null)),
                    lit("\\|"),
                    when(!isnull(lookup(
                        "LKP_Prod",
                        element_at(col("ta_prdct_dtls"), col("index") + lit(1))
                          .getField("target_dl_bit")
                      )),
                      lookup(
                        "LKP_Form_CAG_dataset",
                        lookup(
                          "LKP_Prod",
                          element_at(col("ta_prdct_dtls"), col("index") + lit(1))
                            .getField("target_dl_bit")
                        ).getField("ndc11")
                      ).getField("formulary_tier_desc")).otherwise(lit(null))
                  )
                ) <= lit(49),
                concat(
                  when(!isnull(lookup(
                        "LKP_Prod",
                        element_at(col("ta_prdct_dtls"), col("index") + lit(1))
                          .getField("target_dl_bit")
                      )),
                      lookup(
                        "LKP_Form_CAG_dataset",
                        lookup(
                          "LKP_Prod",
                          element_at(col("ta_prdct_dtls"), col("index") + lit(1))
                            .getField("target_dl_bit")
                        ).getField("ndc11")
                      ).getField("formulary_tier_desc")).otherwise(lit(null)),
                  lit("\\|"),
                  when(!isnull(lookup(
                        "LKP_Prod",
                        element_at(col("ta_prdct_dtls"), col("index") + lit(1))
                          .getField("target_dl_bit")
                      )),
                      lookup(
                        "LKP_Form_CAG_dataset",
                        lookup(
                          "LKP_Prod",
                          element_at(col("ta_prdct_dtls"), col("index") + lit(1))
                            .getField("target_dl_bit")
                        ).getField("ndc11")
                      ).getField("formulary_tier_desc")).otherwise(lit(null))
                )
              ).otherwise(
                concat(
                  string_substring(
                    concat(
                      when(!isnull(lookup(
                        "LKP_Prod",
                        element_at(col("ta_prdct_dtls"), col("index") + lit(1))
                          .getField("target_dl_bit")
                      )),
                      lookup(
                        "LKP_Form_CAG_dataset",
                        lookup(
                          "LKP_Prod",
                          element_at(col("ta_prdct_dtls"), col("index") + lit(1))
                            .getField("target_dl_bit")
                        ).getField("ndc11")
                      ).getField("formulary_tier_desc")).otherwise(lit(null)),
                      lit("\\|"),
                      when(!isnull(lookup(
                        "LKP_Prod",
                        element_at(col("ta_prdct_dtls"), col("index") + lit(1))
                          .getField("target_dl_bit")
                        )),
                        lookup(
                          "LKP_Form_CAG_dataset",
                          lookup(
                            "LKP_Prod",
                            element_at(col("ta_prdct_dtls"), col("index") + lit(1))
                              .getField("target_dl_bit")
                          ).getField("ndc11")
                        ).getField("formulary_tier_desc")).otherwise(lit(null))
                    ),
                    lit(1),
                    lit(49)
                  ),
                  lit(">")
                )
              ).as("target_formulary_tier_desc"),
              lookup("LKP_Form_CAG_dataset",
                     lookup("LKP_Prod", element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("target_dl_bit"))
                       .getField("ndc11")
              ).getField("formulary_status_desc").as("target_formulary_status_desc"),
              lookup("LKP_Form_CAG_dataset",
                     lookup("LKP_Prod", element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("target_dl_bit"))
                       .getField("ndc11")
              ).getField("pa_type_cd").as("target_pa_type_cd"),
              lookup("LKP_Form_CAG_dataset",
                     lookup("LKP_Prod", element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("target_dl_bit"))
                       .getField("ndc11")
              ).getField("step_therapy_type_cd").as("target_step_therapy_type_cd"),
              when(
                element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("ST_flag").cast(BooleanType),
                array_join(
                  lookup(
                    "LKP_Step_Grp_NM",
                    lookup("LKP_Prod", element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("target_dl_bit"))
                      .getField("ndc11"),
                    lookup("LKP_ST_GRP_NUM", lit(0)).getField("_target_st_grp_num")
                  ).getField("step_therapy_group_names"),
                  "|"
                )
              ).otherwise(lit("")).as("target_step_therapy_group_name"),
              when(
                element_at(col("ta_prdct_dtls"),  col("index") + lit(1)).getField("ST_flag"),
                lookup("LKP_ST_GRP_NUM", lit(0)).getField("_target_st_grp_num")
              ).otherwise(lit(0.0)).cast(StringType).as("target_step_therapy_step_num"),
              lookup("LKP_Form_CAG_dataset",
                     lookup("LKP_Prod", element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("target_dl_bit"))
                       .getField("ndc11")
              ).getField("formulary_cd").as("formulary_cd"),
              lit(Config.RUN_TS).as("rec_crt_ts"),
              lit(Config.USER_ID).as("rec_crt_user_id"),
              lookup("LKP_Form_CAG_dataset",
                     lookup("LKP_Prod", element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("target_dl_bit"))
                       .getField("ndc11")
              ).getField("last_exp_dt").as("last_exp_dt")
            )
          ).as("target_dtl"),
          (when(
            is_blank(
              when(
                element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("ST_flag").cast(BooleanType),
                array_join(
                  lookup(
                    "LKP_Step_Grp_NM",
                    lookup("LKP_Prod", element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("target_dl_bit"))
                      .getField("ndc11"),
                    lookup("LKP_ST_GRP_NUM", lit(0)).getField("_target_st_grp_num")
                  ).getField("step_therapy_group_names"),
                  "|"
                )
              ).otherwise(lit(""))
            ),
            lit(" ")
          ).when(
              size(
                element_at(element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("alt_prdcts"), lit(1))
                  .getField("tala")
              ) > lit(20),
              element_at(element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("alt_prdcts"), lit(1))
                .getField("tala")
            )
            .otherwise(col("tal_assoc_name")))
            .as("tala"),
          (element_at(col("ta_prdct_dtls"), col("index") + lit(1))
            .getField("ST_flag")
            .cast(DecimalType(1, 0)))
            .as("ST_flag"),
          (when(
            element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("ST_flag").cast(BooleanType),
            lookup(
              "LKP_Step_Grp_NM",
              lookup("LKP_Prod", element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("target_dl_bit"))
                .getField("ndc11"),
              lookup("LKP_ST_GRP_NUM", lit(0)).getField("_target_st_grp_num")
            ).getField("step_therapy_group_names")
          ).otherwise(array())).as("tar_group_names"),
          (element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("alt_prdcts")).as("alt_products_def"),
          (when(
            !isnull(lookup("LKP_Prod", element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("target_dl_bit"))),
            when(
              isnull(
                lookup("LKP_Form_CAG_dataset",
                       lookup("LKP_Prod", element_at(col("ta_prdct_dtls"), col("index") + lit(1)).getField("target_dl_bit"))
                         .getField("ndc11")
                )
              ),
              lit(0)
            ).otherwise(lit(1))
          ).otherwise(lit(0)).cast(DecimalType(1, 0))).as("is_valid_rec")
        ),
        lengthRelatedGlobalExpressions = Map(),
        tempWindowExpr = Map()
      )
    
      val simpleSelect_in_DF = normalizeDF.select(
        (col("tal_id").cast(DecimalType(10, 0))).as("tal_id"),
        (col("tal_name")).as("tal_name"),
        (col("target_dtl")).as("target_dtl"),
        (col("tal_assoc_name")).as("tal_assoc_name"),
        (col("tala")).as("tala"),
        (col("tar_udl_nm")).as("tar_udl"),
        (col("ST_flag")).as("ST_flag"),
        (col("tar_group_names")).as("tar_group_names"),
        (col("alt_products_def")).as("alt_products_def"),
        (col("constituent_grp_vec")).as("constituent_grp_vec"),
        (col("is_valid_rec")).as("is_valid_rec")
      )
    
      val normalize_out_DF = simpleSelect_in_DF.filter(col("is_valid_rec"))
    
      val out = normalize_out_DF
    out
  }

}
