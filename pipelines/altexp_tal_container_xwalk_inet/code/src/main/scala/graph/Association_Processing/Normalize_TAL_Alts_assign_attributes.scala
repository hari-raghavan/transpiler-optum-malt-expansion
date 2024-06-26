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

object Normalize_TAL_Alts_assign_attributes {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    def temp202006_UDF(alt_prod_attrs: org.apache.spark.sql.Column) = {
      val _alt_group_names_list = flatten(
        transform(
          lookup("LKP_ST_GRP_NUM", lit(0)).getField("_alt_st_grp_nums"),
          xx =>
            array_intersect(
              coalesce(
                lookup("LKP_Step_Grp_NM", alt_prod_attrs.getField("ndc11"), xx)
                  .getField("step_therapy_group_names"),
                array()
              ),
              col("tar_group_names")
            )
        )
      )
    
      val _alt_group_num = filter(
        transform(
          lookup("LKP_ST_GRP_NUM", lit(0)).getField("_alt_st_grp_nums"),
          xx =>
            when(
              size(
                array_intersect(
                  coalesce(
                    lookup("LKP_Step_Grp_NM", alt_prod_attrs.getField("ndc11"), xx)
                      .getField("step_therapy_group_names"),
                    array()
                  ),
                  col("tar_group_names")
                )
              ) > lit(0),
              xx
            )
        ),
        xx => !isnull(xx)
      )
    
      struct(
        _alt_group_names_list.as("_alt_group_names_list"),
        _alt_group_num("_alt_group_num")
      )
    }
    
    def lkp_prod_null() = {
      struct(
            lit(null).as("alt_run_alt_dtl_id"),
            lit(null).as("alt_run_id"),
            lit(null).as("alt_run_target_dtl_id"),
            lit(null).as("formulary_name"),
            lit(null).as("target_ndc"),
            lit(null).as("alt_ndc"),
            lit(null).as("alt_formulary_tier"),
            lit(null).as("alt_multi_src_cd"),
            lit(null).as("alt_roa_cd"),
            lit(null).as("alt_dosage_form_cd"),
            lit(null).as("rank"),
            lit(null).as("alt_step_therapy_ind"),
            lit(null).as("alt_pa_reqd_ind"),
            lit(null).as("alt_formulary_status"),
            lit(null).as("alt_specialty_ind"),
            lit(null).as("alt_gpi14"),
            lit(null).as("alt_prod_name_ext"),
            lit(null).as("alt_prod_short_desc"),
            lit(null).as("alt_prod_short_desc_grp"),
            lit(null).as("alt_gpi14_desc"),
            lit(null).as("alt_gpi8_desc"),
            lit(null).as("alt_formulary_tier_desc"),
            lit(null).as("alt_formulary_status_desc"),
            lit(null).as("alt_pa_type_cd"),
            lit(null).as("alt_step_therapy_type_cd"),
            lit(null).as("alt_step_therapy_group_name"),
            lit(null).as("alt_step_therapy_step_number"),
            lit(null).as("rec_crt_ts"),
            lit(null).as("rec_crt_user_id"),
            lit(null).as("rebate_elig_cd"),
            lit(null).as("tad_eligible_cd"),
            lit(null).as("alt_qty_adj"),
            lit(null).as("tal_assoc_name"),
            lit(null).as("tala"),
            lit(null).as("alt_udl"),
            lit(null).as("tal_assoc_rank"),
            lit(null).as("constituent_group"),
            lit(null).as("constituent_reqd"),
            lit(null).as("newline")
          )
    }
    
      
      val normalizeDF = in.normalize(
        lengthExpression = Some(size(col("alt_products_def"))),
        finishedExpression = None,
        finishedCondition = None,
        alias = "index",
        colsToSelect = List(
          (
            struct(
              lit(-1).cast(DecimalType(16, 0)).as("alt_run_alt_dtl_id"),
              lit(-1).cast(DecimalType(16, 0)).as("alt_run_id"),
              lit(-1).cast(DecimalType(16, 0)).as("alt_run_target_dtl_id"),
              lit(Config.FORMULARY_NM).as("formulary_name"),
              col("target_dtl.target_ndc").as("target_ndc"),
              coalesce(
                lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                  lkp_prod_null()
              ).getField("ndc11").as("alt_ndc"),
              when(
                !isnull(
                  coalesce(
                    lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                    lkp_prod_null()
                  ).getField("ndc11")
                ),
                lookup(
                  "LKP_Form_CAG_dataset",
                  coalesce(
                    lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                    lkp_prod_null()
                  ).getField("ndc11")
                ).getField("formulary_tier")
              ).otherwise(lit(null)).as("alt_formulary_tier"),
              coalesce(
                lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                  lkp_prod_null()
              ).getField("msc").as("alt_multi_src_cd"),
              coalesce(
                lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                  lkp_prod_null()
              ).getField("roa_cd").as("alt_roa_cd"),
              coalesce(
                lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                  lkp_prod_null()
              ).getField("dosage_form_cd").as("alt_dosage_form_cd"),
              (col("index") + lit(1)).cast(StringType).as("rank"),
              when(
                !isnull(
                  coalesce(
                    lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                    lkp_prod_null()
                  ).getField("ndc11")
                ),
                lookup(
                  "LKP_Form_CAG_dataset",
                  coalesce(
                    lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                    lkp_prod_null()
                  ).getField("ndc11")
                ).getField("step_therapy_ind")
              ).otherwise(lit(null)).as("alt_step_therapy_ind"),
              when(
                !isnull(
                  coalesce(
                    lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                    lkp_prod_null()
                  ).getField("ndc11")
                ),
                lookup(
                  "LKP_Form_CAG_dataset",
                  coalesce(
                    lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                    lkp_prod_null()
                  ).getField("ndc11")
                ).getField("pa_reqd_ind")
              ).otherwise(lit(null)).as("alt_pa_reqd_ind"),
              when(
                !isnull(
                  coalesce(
                    lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                    lkp_prod_null()
                  ).getField("ndc11")
                ),
                lookup(
                  "LKP_Form_CAG_dataset",
                  coalesce(
                    lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                    lkp_prod_null()
                  ).getField("ndc11")
                ).getField("formulary_status")
              ).otherwise(lit(null)).as("alt_formulary_status"),
              when(
                !isnull(
                  coalesce(
                    lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                    lkp_prod_null()
                  ).getField("ndc11")
                ),
                lookup(
                  "LKP_Form_CAG_dataset",
                  coalesce(
                    lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                    lkp_prod_null()
                  ).getField("ndc11")
                ).getField("specialty_ind")
              ).otherwise(lit(null)).as("alt_specialty_ind"),
              when(
                is_blank(
                  coalesce(
                    lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                    lkp_prod_null()
                  ).getField("gpi14")
                ),
                lit(" ")
              ).otherwise(
                  coalesce(
                    lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                    lkp_prod_null()
                  ).getField("gpi14")
                )
                .as("alt_gpi14"),
              when(
                is_blank(
                  coalesce(
                    lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                    lkp_prod_null()
                  ).getField("drug_name")
                ),
                lit(" ")
              ).otherwise(
                  coalesce(
                    lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                    lkp_prod_null()
                  ).getField("drug_name")
                )
                .as("alt_prod_name_ext"),
              coalesce(
                lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                  lkp_prod_null()
              ).getField("prod_short_desc").as("alt_prod_short_desc"),
              string_substring(
                when(
                  when(file_information(lit(Config.ALIAS_PRODUCT_FILE)).getField("size").cast(BooleanType),
                       lit(1).cast(BooleanType)
                  ).otherwise(lit(0).cast(BooleanType)),
                  lookup(
                    "LKP_Alias_Prod",
                    coalesce(
                      lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                        lkp_prod_null()
                    ).getField("ndc11")
                  ).getField("alias_label_nm")
                ).otherwise(
                  coalesce(
                    lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                    lkp_prod_null()
                  ).getField("prod_short_desc")
                ),
                lit(1),
                lit(12)
              ).as("alt_prod_short_desc_grp"),
              coalesce(
                lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                  lkp_prod_null()
              ).getField("gpi14_desc").as("alt_gpi14_desc"),
              coalesce(
                lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                  lkp_prod_null()
              ).getField("gpi8_desc").as("alt_gpi8_desc"),
              when(
                !isnull(
                  coalesce(
                    lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                    lkp_prod_null()
                  ).getField("ndc11")
                ),
                lookup(
                  "LKP_Form_CAG_dataset",
                  coalesce(
                    lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                    lkp_prod_null()
                  ).getField("ndc11")
                ).getField("formulary_tier_desc")
              ).otherwise(lit(null)).as("alt_formulary_tier_desc"),
              when(
                !isnull(
                  coalesce(
                    lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                    lkp_prod_null()
                  ).getField("ndc11")
                ),
                lookup(
                  "LKP_Form_CAG_dataset",
                  coalesce(
                    lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                    lkp_prod_null()
                  ).getField("ndc11")
                ).getField("formulary_status_desc")
              ).otherwise(lit(null)).as("alt_formulary_status_desc"),
              when(
                !isnull(
                  coalesce(
                    lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                    lkp_prod_null()
                  ).getField("ndc11")
                ),
                lookup(
                  "LKP_Form_CAG_dataset",
                  coalesce(
                    lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                    lkp_prod_null()
                  ).getField("ndc11")
                ).getField("pa_type_cd")
              ).otherwise(lit(null)).as("alt_pa_type_cd"),
              when(
                !isnull(
                  coalesce(
                    lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                    lkp_prod_null()
                  ).getField("ndc11")
                ),
                lookup(
                  "LKP_Form_CAG_dataset",
                  coalesce(
                    lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                    lkp_prod_null()
                  ).getField("ndc11")
                ).getField("step_therapy_type_cd")
              ).otherwise(lit(null)).as("alt_step_therapy_type_cd"),
              when(
                size(
                  when(
                    !isnull(
                      coalesce(
                        lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                          lkp_prod_null()
                      ).getField("ndc11")
                    ),
                    when(
                      col("ST_flag").cast(BooleanType),
                      temp202006_UDF(
                        coalesce(
                          lookup("LKP_Prod",
                                 element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")
                          ),
                            lkp_prod_null()
                        )
                      ).getField("_alt_group_names_list")
                    ).otherwise(
                      array()
                    )
                  ).otherwise(array())
                ).cast(BooleanType),
                array_join(
                  when(
                    !isnull(
                      coalesce(
                        lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                          lkp_prod_null()
                      ).getField("ndc11")
                    ),
                    when(
                      col("ST_flag").cast(BooleanType),
                      temp202006_UDF(
                        coalesce(
                          lookup("LKP_Prod",
                                 element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")
                          ),
                            lkp_prod_null()
                        )
                      ).getField("_alt_group_names_list")
                    ).otherwise(
                      array()
                    )
                  ).otherwise(array()),
                  "|"
                )
              ).otherwise(lit("")).as("alt_step_therapy_group_name"),
              when(
                size(
                  when(
                    !isnull(
                      coalesce(
                        lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                          lkp_prod_null()
                      ).getField("ndc11")
                    ),
                    when(
                      col("ST_flag").cast(BooleanType),
                      temp202006_UDF(
                        coalesce(
                          lookup("LKP_Prod",
                                 element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")
                          ),
                            lkp_prod_null()
                        )
                      ).getField("_alt_group_names_list")
                    ).otherwise(
                      array()
                    )
                  ).otherwise(array())
                ).cast(BooleanType),
                array_join(
                  when(
                    !isnull(
                      coalesce(
                        lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                          lkp_prod_null()
                      ).getField("ndc11")
                    ),
                    when(
                      col("ST_flag").cast(BooleanType),
                      temp202006_UDF(
                        coalesce(
                          lookup("LKP_Prod",
                                 element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")
                          ),
                            lkp_prod_null()
                        )
                      ).getField("_alt_group_num")
                    ).otherwise(
                      array()
                    )
                  ).otherwise(array()),
                  "|"
                )
              ).otherwise(lit(0.0)).as("alt_step_therapy_step_number"),
              lit(Config.RUN_TS).as("rec_crt_ts"),
              lit(Config.USER_ID).as("rec_crt_user_id"),
              element_at(col("alt_products_def"), col("index") + lit(1)).getField("rebate_elig_cd").as("rebate_elig_cd"),
              element_at(col("alt_products_def"), col("index") + lit(1)).getField("tad_eli_code").as("tad_eligible_cd"),
              element_at(col("alt_products_def"), col("index") + lit(1))
                .getField("qty_adjust_factor")
                .cast(DecimalType(6, 3))
                .as("alt_qty_adj"),
              element_at(col("alt_products_def"), col("index") + lit(1)).getField("tal_assoc_name").as("tal_assoc_name"),
              element_at(col("alt_products_def"), col("index") + lit(1)).getField("tala").as("tala"),
              element_at(col("alt_products_def"), col("index") + lit(1)).getField("udl_nm").as("alt_udl"),
              element_at(col("alt_products_def"), col("index") + lit(1))
                .getField("priority")
                .cast(DecimalType(38, 6))
                .as("tal_assoc_rank"),
              element_at(col("alt_products_def"), col("index") + lit(1))
                .getField("constituent_group")
                .as("constituent_group"),
              element_at(col("alt_products_def"), col("index") + lit(1))
                .getField("constituent_reqd")
                .as("constituent_reqd"),
              lit("\n").cast(StringType).as("newline")
            )
          ).as("alt_run_alt_dtl_load"),
          (when(
            !isnull(
              lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd"))
                .getField("ndc11")
            ),
            when(
              col("ST_flag").cast(BooleanType),
              when(
                !size(
                  when(
                    !isnull(
                      lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd"))
                        .getField("ndc11")
                    ),
                    when(
                      col("ST_flag").cast(BooleanType),
                      temp202006_UDF(
                        lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")),
                      ).getField("_alt_group_names_list")
                    ).otherwise(
                      array()
                    )
                  ).otherwise(array())
                ).cast(BooleanType),
                lit(0)
              ).when(
                  isnull(
                    lookup("LKP_Form_CAG_dataset",
                           lookup("LKP_Prod",
                                  element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd")
                           ).getField("ndc11")
                    )
                  ),
                  lit(0)
                )
                .otherwise(lit(1))
            ).when(
                isnull(
                  lookup("LKP_Form_CAG_dataset",
                         lookup("LKP_Prod", element_at(col("alt_products_def"), col("index") + lit(1)).getField("alt_prd"))
                           .getField("ndc11")
                  )
                ),
                lit(0)
              )
              .otherwise(lit(1))
          ).otherwise(lit(0))).as("is_valid_rec")
        ),
        lengthRelatedGlobalExpressions = Map(),
        tempWindowExpr = Map()
      )
    
      val simpleSelect_in_DF = normalizeDF.select(
        (col("alt_run_alt_dtl_load")).as("alt_run_alt_dtl_load"),
        (col("constituent_grp_vec")).as("constituent_grp_vec"),
        (col("is_valid_rec")).as("is_valid_rec")
      )
    
      val normalize_out_DF = simpleSelect_in_DF.filter(col("is_valid_rec"))
    
      val out = normalize_out_DF
    out
  }

}
