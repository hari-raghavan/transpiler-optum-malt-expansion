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

object Populate_TAR_Target_Alternate_Crosswalk {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import _root_.io.prophecy.abinitio.ScalaFunctions._
    import java.math.BigDecimal
    
    def lv_prdcts(rule: org.apache.spark.sql.Column) = {
      transform(rule,
        rec =>
          when(
            rec.getField("qualifier_cd") === lit("TSD"),
            coalesce(lookup("TSD", rec.getField("compare_value")).getField("products"), bv_all_zeros())
          ).when(
            array_contains(array(lit("PA"), lit("ST"), lit("SPECIALTY"), lit("ST_STEP_NUM")), rec.getField("qualifier_cd")),
            coalesce(
              lookup(
                "Formulary_Rule_Prdcts",
                rec.getField("qualifier_cd"),
                rec.getField("operator"),
                rec.getField("compare_value")
              ).getField("products"),
              bv_all_zeros()
            )
          ).otherwise(
            coalesce(lookup("Expanded_UDL", rec.getField("qualifier_cd")).getField("products"), bv_all_zeros())
          )
      )
    }
    
    def get_products(rule: Seq[Row], lv_prdcts_all: Array[Array[Byte]]) = {
      var inclusion_prdcts = _bv_all_zeros()
      var exclusion_prdcts = _bv_all_zeros()
      var lv_prdcts = _bv_all_zeros()
      var or_products = _bv_all_zeros()
      var no_ands = 1
      var is_hrm = 0
    
      rule.zipWithIndex.foreach {
        case (rec, i) ⇒
          var lv_prdcts = lv_prdcts_all(i)
          if (
            !(rec.getAs[String](0) == "TSD" || rec.getAs[String](0) == "PA" || rec.getAs[String](0) == "ST" || rec.getAs[String](0) == "SPECIALTY" || rec.getAs[String](0) == "ST_STEP_NUM")
          ) {
    
            if (rec.getAs[String](2) == "N") {
              exclusion_prdcts = _bv_or(exclusion_prdcts, lv_prdcts)
              is_hrm = 1
            }
          }
          if (is_hrm == 0) {
            or_products = _bv_or(or_products, lv_prdcts)
            if (rec.getAs[String](3) != "0") {
              if (no_ands > 0) {
                inclusion_prdcts = or_products
                no_ands = 0
              } else {
                inclusion_prdcts = _bv_and(inclusion_prdcts, or_products)
              }
              or_products = _bv_all_zeros()
            }
          } else {
            is_hrm = 0
          }
      }
      _bv_difference(inclusion_prdcts, exclusion_prdcts)
    }
    
    def get_final_products(roa_cd: String, df_cd: String, roa_prdts: Array[Byte], df_prdts: Array[Byte]) = {
    
      if (roa_cd != '*' && df_cd != '*') _bv_and(roa_prdts, df_prdts)
      else if (roa_cd != '*' && df_cd == '*') roa_prdts
      else if (roa_cd == '*' && df_cd != '*') df_prdts
      else _bv_all_zeros()
    }
    
    def _to_array_of_arrays(target_prdcts: Seq[Seq[Byte]]) = {
      target_prdcts.map(_.toArray).toArray
    }
    
    val process_udf = udf(
      { (inputRows: Seq[Row]) ⇒
        var tar_content_vec   = Array[Row]()
        var roa_df_prdts_vec  = Array[Row]()
        var all_target_prdcts = _bv_all_zeros()
        var all_alt_prdcts    = _bv_all_zeros()
        var _keep_all_targets = 0
        var rebate_prdcts_t   = _bv_all_zeros()
    
        def get_roa_df_products(
          in_compare_value: String,
          rule_prdt_lkp:    Array[Byte]
        ) = {
          var prd_idx = roa_df_prdts_vec.indexWhere(_.getAs[String](0) == in_compare_value)
          var prdcts  = _bv_all_zeros()
    
          if (prd_idx > -1) {
            prdcts = roa_df_prdts_vec(prd_idx).getAs[Array[Byte]](1)
         } else {
            prdcts =
              try {
                val res = rule_prdt_lkp
                if (res == null || res.nonEmpty) {
                  _bv_all_zeros()
                } else {
                  res
                }
              } catch {
                case e ⇒ _bv_all_zeros()
              }
            roa_df_prdts_vec = Array.concat(
              roa_df_prdts_vec,
              Array.fill(1)(
                Row(
                  in_compare_value,
                  prdcts
                )
              )
            )
          }
          prdcts
        }
    
        inputRows.foreach { row ⇒
          var roa_prdcts          = _bv_all_zeros()
          var df_prdcts           = _bv_all_zeros()
          var alt_prdcts          = _bv_all_zeros()
          var alt_prdcts_all_prio = _bv_all_zeros()
          var alt_prdcts_p        = _bv_all_zeros()
          var rebate_prdcts       = _bv_all_zeros()
          var target_prdcts       = _bv_all_zeros()
          var alt_prdcts_vec      = Array[Array[Byte]]()
          var alt_prdcts_vec1     = Array[Array[Byte]]()
          var alt_prdcts_vec2     = Array[Array[Byte]]()
          var _processed_prio     = Array[Array[Array[Byte]]]()
          var lkp_tar_roa_df_vec  = Array[Row]()
          var _processed_roa_df   = Array[Int]()
          var index               = -1
          var is_empty            = 1
          var reb_udl_cnt         = 0
          var rebate_udl          = ""
          var p_check             = -1
    
          val _roa_prdcts1 = row.getSeq[Array[Byte]](18).toArray
          val _df_prdcts1 =  row.getSeq[Array[Byte]](19).toArray
          val _roa_prdcts2 = row.getSeq[Array[Byte]](20).toArray
          val _df_prdcts2 =  row.getSeq[Array[Byte]](21).toArray
          if (!_isnull(row.getAs[String]("tar_roa_df_set_id"))) {
            var roa_df_tar_content = Row(Array[Array[Byte]](), Array[Row]())
            var prev_pri           = 0
    
            lkp_tar_roa_df_vec =
              row.getAs[Seq[Row]]("lkp_tar_roa_df_vec").toArray
            lkp_tar_roa_df_vec.zipWithIndex.foreach { case (lv_tar_roa_df, idx) ⇒
              var roa_df       = lv_tar_roa_df.getAs[String]("target_roa_cd").toInt + lv_tar_roa_df.getAs[String]("target_dosage_form_cd").toInt
              var roa_priority = 0
              if (p_check == -1) {
                prev_pri = lv_tar_roa_df.getAs[String]("priority").toInt
                p_check = 0
              }
              roa_priority = lv_tar_roa_df.getAs[String]("priority").toInt
    
              if (_processed_roa_df.indexOf(roa_df) == -1) {
                if (!_isnull(_processed_roa_df)) {
                  if (alt_prdcts_vec.nonEmpty && _bv_count_one_bits(target_prdcts) > 0) {
                    if (is_empty == 0) {
                      alt_prdcts_vec1 = Array.concat(alt_prdcts_vec1, Array.fill(1)(alt_prdcts_p))
                      _processed_prio = Array.concat(_processed_prio, Array.fill(1)(alt_prdcts_vec))
                      roa_df_tar_content = updateIndexInRow(
                        roa_df_tar_content,
                        0,
                        Array.concat(roa_df_tar_content.getSeq[Array[Byte]](0).toArray, Array.fill(1)(target_prdcts))
                      )
                      roa_df_tar_content = updateIndexInRow(
                        roa_df_tar_content,
                        1,
                        Array.concat(
                          roa_df_tar_content.getAs[Array[Row]](1),
                          Array.fill(1)(
                            Row(
                              alt_prdcts_vec1,
                              _processed_prio,
                              _bv_vector_or(alt_prdcts_vec2)
                            )
                          )
                        )
                      )
                      prev_pri = roa_priority
                    } else {
                      alt_prdcts_vec1 = Array.concat(alt_prdcts_vec1, Array.fill(1)(alt_prdcts_p))
                      _processed_prio = Array.concat(_processed_prio, Array.fill(1)(alt_prdcts_vec))
                      roa_df_tar_content = Row(
                                                Array.fill(1)(target_prdcts),
                                                Array.fill(1)(
                                                  Row(
                                                    alt_prdcts_vec1,
                                                    _processed_prio,
                                                    _bv_vector_or(alt_prdcts_vec2)
                                                  )
                                                )
                                              )
    
                      is_empty = 0
                      prev_pri = roa_priority
                    }
                  }
    
                  target_prdcts = _bv_all_zeros()
                  alt_prdcts = _bv_all_zeros()
                  alt_prdcts_all_prio = _bv_all_zeros()
                  alt_prdcts_p = _bv_all_zeros()
                  alt_prdcts_vec = Array[Array[Byte]]()
                  alt_prdcts_vec1 = Array[Array[Byte]]()
                  alt_prdcts_vec2 = Array[Array[Byte]]()
                  _processed_prio = Array[Array[Array[Byte]]]()
                }
                _processed_roa_df = Array.concat(_processed_roa_df, Array.fill(1)(roa_df))
                roa_prdcts =
                  if (lv_tar_roa_df.getAs[String]("target_roa_cd") != '*')
                    get_roa_df_products(lv_tar_roa_df.getAs[String]("target_roa_cd"), _roa_prdcts1(idx))
                  else _bv_all_zeros()
                df_prdcts =
                  if (lv_tar_roa_df.getAs[String]("target_dosage_form_cd") != '*')
                    get_roa_df_products(lv_tar_roa_df.getAs[String]("target_dosage_form_cd"), _df_prdcts1(idx))
                  else _bv_all_zeros()
                target_prdcts =
                  get_final_products(lv_tar_roa_df.getAs[String]("target_roa_cd"), lv_tar_roa_df.getAs[String]("target_dosage_form_cd"), roa_prdcts, df_prdcts)
                all_target_prdcts = _bv_or(all_target_prdcts,        target_prdcts)
              }
    
              if (_bv_count_one_bits(target_prdcts) > 0 && (roa_priority == prev_pri)) {
                roa_prdcts =
                  if (lv_tar_roa_df.getAs[String]("alt_roa_cd") != '*')
                    get_roa_df_products(lv_tar_roa_df.getAs[String]("alt_roa_cd"), _roa_prdcts2(idx))
                  else _bv_all_zeros()
                df_prdcts =
                  if (lv_tar_roa_df.getAs[String]("alt_dosage_form_cd") != '*')
                    get_roa_df_products(lv_tar_roa_df.getAs[String]("alt_dosage_form_cd"), _df_prdcts2(idx))
                  else _bv_all_zeros()
                alt_prdcts =
                  get_final_products(lv_tar_roa_df.getAs[String]("alt_roa_cd"), lv_tar_roa_df.getAs[String]("alt_dosage_form_cd"), roa_prdcts, df_prdcts)
                alt_prdcts_p = _bv_or(alt_prdcts_p,                  alt_prdcts)
                all_alt_prdcts = _bv_or(all_alt_prdcts,              alt_prdcts)
                if (_bv_count_one_bits(alt_prdcts) > 0)
                  alt_prdcts_vec = Array.concat(alt_prdcts_vec, Array.fill(1)(alt_prdcts))
                alt_prdcts_vec2 = Array.concat(alt_prdcts_vec2, Array.fill(1)(alt_prdcts))
                prev_pri = roa_priority
              }
    
              if (_bv_count_one_bits(target_prdcts) > 0 && roa_priority != prev_pri) {
                _processed_prio = Array.concat(_processed_prio, Array.fill(1)(alt_prdcts_vec))
                alt_prdcts_vec = Array[Array[Byte]]()
                alt_prdcts_vec1 = Array.concat(alt_prdcts_vec1, Array.fill(1)(alt_prdcts_p))
                prev_pri = prev_pri + 1
                alt_prdcts_p = _bv_all_zeros()
                roa_prdcts =
                  if (lv_tar_roa_df.getAs[String]("alt_roa_cd") != '*')
                    get_roa_df_products(lv_tar_roa_df.getAs[String]("alt_roa_cd"), _roa_prdcts2(idx))
                  else _bv_all_zeros()
                df_prdcts =
                  if (lv_tar_roa_df.getAs[String]("alt_dosage_form_cd") != '*')
                    get_roa_df_products(lv_tar_roa_df.getAs[String]("alt_dosage_form_cd"), _df_prdcts2(idx))
                  else _bv_all_zeros()
                alt_prdcts =
                  get_final_products(lv_tar_roa_df.getAs[String]("alt_roa_cd"), lv_tar_roa_df.getAs[String]("alt_dosage_form_cd"), roa_prdcts, df_prdcts)
                alt_prdcts_p = _bv_or(alt_prdcts_p,                  alt_prdcts)
                all_alt_prdcts = _bv_or(all_alt_prdcts,              alt_prdcts)
                if (_bv_count_one_bits(alt_prdcts) > 0)
                  alt_prdcts_vec = Array.concat(alt_prdcts_vec, Array.fill(1)(alt_prdcts))
                alt_prdcts_vec2 = Array.concat(alt_prdcts_vec2, Array.fill(1)(alt_prdcts))
                prev_pri = roa_priority
              }
            }
            if (_bv_count_one_bits(target_prdcts) > 0 && !_isnull(roa_df_tar_content)) {
              _processed_prio = Array.concat(_processed_prio, Array.fill(1)(alt_prdcts_vec))
              alt_prdcts_vec1 = Array.concat(alt_prdcts_vec1, Array.fill(1)(alt_prdcts_p))
              roa_df_tar_content = updateIndexInRow(
                roa_df_tar_content,
                0,
                Array.concat(roa_df_tar_content.getSeq[Array[Byte]](0).toArray, Array.fill(1)(target_prdcts))
              )
              roa_df_tar_content = updateIndexInRow(
                roa_df_tar_content,
                1,
                Array.concat(
                  roa_df_tar_content.getAs[Array[Row]](1),
                  Array.fill(1)(
                    Row(
                      alt_prdcts_vec1,
                      _processed_prio,
                      _bv_vector_or(alt_prdcts_vec2)
                    )
                  )
                )
              )
            } else {
              roa_df_tar_content = Array.fill(1)(
                Row(
                  Array.fill(1)(target_prdcts),
                  Array.fill(1)(
                    Row(
                      alt_prdcts_vec1,
                      _processed_prio,
                      _bv_vector_or(alt_prdcts_vec2)
                    )
                  )
                )
              )
            }
            tar_content_vec = Array.concat(tar_content_vec, Array.fill(1)(roa_df_tar_content))
          } else {
            if (row.getAs[Seq[Row]]("target_rule_def").head.getAs[String]("qualifier_cd") == "ALL") {
              _keep_all_targets = 1
              all_target_prdcts = _bv_all_zeros()
            } else {
              target_prdcts = try {
                  val res = get_products(row.getAs[Seq[Row]]("target_rule_def"), row.getSeq[Array[Byte]](14).toArray)
                  if (res == null || res.nonEmpty) {
                    _bv_all_zeros()
                  } else {
                    res
                  }
                } catch {
                  case e ⇒ _bv_all_zeros()
                }
              all_target_prdcts = if (_keep_all_targets != 1) _bv_or(all_target_prdcts, target_prdcts) else _bv_all_zeros()
            }
    
            alt_prdcts =
                try {
                  val res = get_products(row.getAs[Seq[Row]]("alt_rule_def"), row.getSeq[Array[Byte]](15).toArray)
                  if (res == null || res.nonEmpty) {
                    _bv_all_zeros()
                  } else {
                    res
                  }
                } catch {
                  case e ⇒ _bv_all_zeros()
                }
            if (_isnull(row.getAs[String]("rebate_elig_cd"))) {
              alt_prdcts = _bv_difference(alt_prdcts, rebate_prdcts_t)
              rebate_prdcts_t = _bv_all_zeros()
            }
            if (row.getAs[String]("filter_ind") == 'Y')
              all_alt_prdcts = _bv_or(all_alt_prdcts, alt_prdcts)
    
            if ( _bv_count_one_bits(alt_prdcts) > 0) {
              if (!_isnull(row.getAs[String]("rebate_elig_cd"))) {
                row.getSeq[Array[Byte]](17).toArray.foreach {
                  rebate_udl_prdcts ⇒
                    rebate_prdcts = _bv_or(rebate_prdcts, rebate_udl_prdcts)
                }
                alt_prdcts = _bv_and(alt_prdcts, rebate_prdcts)
                rebate_prdcts_t = rebate_prdcts
              } else {
    
                tar_content_vec = Array.concat(tar_content_vec,
                                               Array.fill(1)(
                                                 Row(
                                                   Array.fill(1)(target_prdcts),
                                                   Array.fill(1)(
                                                     Row(
                                                       alt_prdcts,
                                                       Array.fill(0)(Row()),
                                                       _bv_all_zeros()
                                                     )
                                                   )
                                                 )
                                               )
                                              )
              }
            }
          }
        }
        var tar_id          = inputRows.toArray.last.getAs[java.math.BigDecimal](0)
        var tar_dtl_id      = inputRows.toArray.last.getAs[java.math.BigDecimal](1)
        var tar_name        = inputRows.toArray.last.getAs[String](2)
        var tar_dtl_type_cd = inputRows.toArray.last.getAs[String](4)
        var newline         = inputRows.toArray.last.getAs[String](13)
        Row(
          tar_id,
          tar_dtl_id,
          tar_name,
          tar_dtl_type_cd,
          tar_content_vec,
          all_alt_prdcts,
          all_target_prdcts,
          new java.math.BigDecimal(_keep_all_targets),
          newline
        )
      },
      StructType(List(
        StructField("tar_id",          DecimalType(10, 0), true),
        StructField("tar_dtl_id",      DecimalType(10, 0), true),
        StructField("tar_name",        StringType, true),
        StructField("tar_dtl_type_cd", StringType, true),
        StructField(
          "contents",
          ArrayType(
            StructType(List(
              StructField("target_prdcts", ArrayType(BinaryType, true), true),
              StructField(
                "alt_contents",
                ArrayType(
                  StructType(List(
                    StructField("alt_prdcts",          ArrayType(BinaryType, true),                  true),
                    StructField("alt_prdcts_all_prio", ArrayType(ArrayType(BinaryType, true), true), true),
                    StructField("common_prdcts",       BinaryType,                                   true)
                  )),
                  true
                ),
                true
              )
            )),
            true
          ),
          true
        ),
        StructField("common_alt_prdcts",    BinaryType,        true),
        StructField("common_target_prdcts", BinaryType,        true),
        StructField("keep_all_targets",     DecimalType(1, 0), true),
        StructField("newline",              StringType,        true)
      ))
    )
    
    def rule_prdcts_lkp(in_compare_value: String, qualifier_cd: String) = {
      transform(
        lookup_row("LKP_TAR_ROA_DF", col("tar_roa_df_set_id")),
        lv_tar_roa_df =>         
          lookup("Rule_Prdcts", lit(qualifier_cd), lit("eq"), lv_tar_roa_df.getField(in_compare_value)).getField("products")
      )
    }
    
    def rebate_udl_vec() = {
      transform(
        lookup_row("Rebate_UDL", col("rebate_elig_cd")),
        xx =>
          lookup("Expanded_UDL", xx.getField("udl_name")).getField("products")
      )
    }
    
    val origColumns = in.columns.map(col)
    val out = in
      .groupBy("tar_id")
      .agg(
        collect_list(
          struct(
            (origColumns 
            :+ lv_prdcts(col("target_rule_def")).alias("target_rule_def_val") // 14
            :+ lv_prdcts(col("alt_rule_def")).alias("alt_rule_def_val") // 15
            :+ lookup_row("LKP_TAR_ROA_DF", col("tar_roa_df_set_id")).alias("lkp_tar_roa_df_vec") // 16
            :+ rebate_udl_vec().alias("rebate_udl_vec") // 17
            :+ rule_prdcts_lkp("target_roa_cd", "ROA") // 18
            :+ rule_prdcts_lkp("target_dosage_form_cd", "DOSAGE_FORM") // 19
            :+ rule_prdcts_lkp("alt_roa_cd", "ROA") // 20 
            :+ rule_prdcts_lkp("alt_dosage_form_cd", "DOSAGE_FORM") // 21
            ): _*
          )
        ).alias("inputRows")
      )
      .select((process_udf(col("inputRows"))).alias("output"))
      .select(col("output.*"))
    out
  }

}
