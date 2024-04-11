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

object Association_Processing__Alternative_Rollup_Processing__Identify_Alt_Proxy_Targets_w_Constituents {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    val process_udf = udf(
      { (inputRows: Seq[Row]) =>
        {
    
          var alt_run_alt_dtl_load_vec = Array[Row]()
          var alt_prod_short_desc_grp_vec = Array[String]()
          var lv_constituent_grp_vec = Array[String]()
          var _constituent_Y_alt_dtl_vec = Array[Row]()
          var _constituent_N_alt_dtl_vec = Array[Row]()
    
          inputRows.zipWithIndex.foreach {
            case (in, jdx) => {
              var idx = -1
              if (
                !alt_prod_short_desc_grp_vec.contains(
                  in.getAs[Row]("alt_run_alt_dtl_load")
                    .getAs[String]("alt_prod_short_desc_grp")
                )
              ) {
                alt_prod_short_desc_grp_vec = Array.concat(
                  alt_prod_short_desc_grp_vec,
                  Array.fill(1)(
                    in.getAs[Row]("alt_run_alt_dtl_load")
                      .getAs[String]("alt_prod_short_desc_grp")
                  )
                )
    
                if (
                  !_isnull(
                    in.getAs[Row]("alt_run_alt_dtl_load")
                      .getAs[String]("constituent_reqd")
                  )
                ) {
    
                  if (
                    in.getAs[Row]("alt_run_alt_dtl_load")
                      .getAs[String]("constituent_reqd") == "Y"
                  ) {
                    _constituent_Y_alt_dtl_vec = Array.concat(
                      _constituent_Y_alt_dtl_vec,
                      Array.fill(1)(in.getAs[Row]("alt_run_alt_dtl_load"))
                    )
                  } else {
                    _constituent_N_alt_dtl_vec = Array.concat(
                      _constituent_N_alt_dtl_vec,
                      Array.fill(1)(in.getAs[Row]("alt_run_alt_dtl_load"))
                    )
                  }
                } else {
                  alt_run_alt_dtl_load_vec = Array.concat(
                    alt_run_alt_dtl_load_vec,
                    Array.fill(1)(in.getAs[Row]("alt_run_alt_dtl_load"))
                  )
                }
              }
            }
          }
          if (
            in.getAs[Seq[String]]("constituent_grp_vec")
              .diff(lv_constituent_grp_vec)
              .isEmpty
          ) {
            alt_run_alt_dtl_load_vec =
              Array.concat(alt_run_alt_dtl_load_vec, _constituent_Y_alt_dtl_vec)
            alt_run_alt_dtl_load_vec =
              Array.concat(alt_run_alt_dtl_load_vec, _constituent_N_alt_dtl_vec)
          }
        }
        Row(alt_run_alt_dtl_load_vec)
      },
      StructType(
                  List(
                    StructField("alt_run_alt_dtl_id",           DecimalType(16, 0)),
                    StructField("alt_run_id",                   DecimalType(16, 0)),
                    StructField("alt_run_target_dtl_id",        DecimalType(16, 0)),
                    StructField("formulary_name",               StringType),
                    StructField("target_ndc",                   StringType),
                    StructField("alt_ndc",                      StringType),
                    StructField("alt_formulary_tier",           StringType),
                    StructField("alt_multi_src_cd",             StringType),
                    StructField("alt_roa_cd",                   StringType),
                    StructField("alt_dosage_form_cd",           StringType),
                    StructField("rank",                         StringType),
                    StructField("alt_step_therapy_ind",         StringType),
                    StructField("alt_pa_reqd_ind",              StringType),
                    StructField("alt_formulary_status",         StringType),
                    StructField("alt_specialty_ind",            StringType),
                    StructField("alt_gpi14",                    StringType),
                    StructField("alt_prod_name_ext",            StringType),
                    StructField("alt_prod_short_desc",          StringType),
                    StructField("alt_prod_short_desc_grp",      StringType),
                    StructField("alt_gpi14_desc",               StringType),
                    StructField("alt_gpi8_desc",                StringType),
                    StructField("alt_formulary_tier_desc",      StringType),
                    StructField("alt_formulary_status_desc",    StringType),
                    StructField("alt_pa_type_cd",               StringType),
                    StructField("alt_step_therapy_type_cd",     StringType),
                    StructField("alt_step_therapy_group_name",  StringType),
                    StructField("alt_step_therapy_step_number", StringType),
                    StructField("rec_crt_ts",                   StringType),
                    StructField("rec_crt_user_id",              StringType),
                    StructField("rebate_elig_cd",               StringType),
                    StructField("tad_eligible_cd",              StringType),
                    StructField("alt_qty_adj",                  DecimalType(6, 3)),
                    StructField("tal_assoc_name",               StringType),
                    StructField("tala",                         StringType),
                    StructField("alt_udl",                      StringType),
                    StructField("tal_assoc_rank",               DecimalType(38, 6)),
                    StructField("constituent_group",            StringType),
                    StructField("constituent_reqd",             StringType),
                    StructField("newline",                      StringType)
                  )
                )
    )
    
    val out0 = in0
      .groupBy(
        "alt_run_alt_dtl_load.tal_assoc_name",
        "alt_run_alt_dtl_load.target_ndc"
      )
      .agg(collect_list(col("tmp")).as("input"))
      .select(process_udf(col("input")).as("output"))
      .select(col("output.*"))
    out0
  }

}
