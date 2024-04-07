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

object Populate_TAC_Target_Alternatives_Crosswalk {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import _root_.io.prophecy.abinitio.ScalaFunctions._
    import java.math.BigDecimal
    
    def lv_prdcts(rec: org.apache.spark.sql.Column) = {
      when(rec.getField("qualifier_cd") == lit("TSD"),
           coalesce(lookup("TSD", rec.getField("compare_value")).getField("products"), bv_all_zeros())
      ).when(
        array_contains(array(lit("PA"), lit("ST"), lit("SPECIALTY"), lit("ST_STEP_NUM")), rec.getField("qualifier_cd")),
        coalesce(
          lookup("Formulary_Rule_Prdcts",
                 rec.getField("qualifier_cd"),
                 rec.getField("operator"),
                 rec.getField("compare_value")
          ).getField("products"),
          bv_all_zeros()
        )
      ).otherwise(
        coalesce(lookup("Expanded_UDL", rec.getField("qualifier_cd")).getField("products"), bv_all_zeros())
      )
    }
    
    def get_products(rule: Seq[Row], lv_prdcts_all: Seq[Array[Byte]]) = {
      var inclusion_prdcts = _bv_all_zeros()
      var exclusion_prdcts = _bv_all_zeros()
      var lv_prdcts        = _bv_all_zeros()
      var or_products      = _bv_all_zeros()
      var no_ands          = 1
      var is_hrm           = 0
    
      rule.zipWithIndex.foreach {
        case (rec, i) ⇒
          var lv_prdcts = lv_prdcts_all.toArray(i)
          if (
            !(rec.getAs[String](0) == "TSD" || rec.getAs[String](0) == "PA"
 rec.getAs[String](0) == "ST" || rec
                .getAs[String](0) == "SPECIALTY" || rec.getAs[String](0) == "ST_STEP_NUM")
          ) {
    
            if (rec.getAs[String](2) == "N") {
              exclusion_prdcts = _bv_or(exclusion_prdcts, lv_prdcts);
              is_hrm = 1;
            }
          }
          if (is_hrm == 0) {
            or_products = _bv_or(or_products, lv_prdcts)
            if (rec.getAs[String](3) != "0") {
              if (no_ands) {
                inclusion_prdcts = or_products
                no_ands = 0
              } else {
                inclusion_prdcts = _bv_and(inclusion_prdcts, or_products)
              }
              or_products = bv_all_zeros()
            }
          } else {
            is_hrm = 0
          }
      }
      _bv_difference(inclusion_prdcts, exclusion_prdcts)
    }
    
    val process_udf = udf(
      { (inputRows: Seq[Row]) ⇒
        var priority      = inputRows.head.getAs[String](2);
        var target_prdcts = _bv_all_zeros()
        var alt_prdcts    = _bv_all_zeros()
        var contents      = Array[Row]()
        var tac_contents  = Array[Row]()
        var _ST_flag      = BigDecimal(0);
    
        inputRows.foreach { row ⇒
          var _target_prdcts = _bv_all_zeros()
          var _alt_prdcts    = bv_all_zeros()
          var index          = 0
    
          if (priority != row.getAs[String](2)) {
            if (_bv_count_one_bits(target_prdcts) > 0 && _bv_count_one_bits(alt_prdcts) > 0) {
              if (length_of(contents) > 1) {
                tac_contents = Array.concat(
                  tac_contents,
                  Array.fill(1)(
                    Row(
                      target_prdcts,
                      alt_prdcts,
                      contents,
                      BigDecimal(1)
                    )
                  )
                )
              } else {
                tac_contents = Array.concat(
                  tac_contents,
                  Array.fill(1)(
                    Row(
                      target_prdcts,
                      alt_prdcts,
                      contents,
                      BigDecimal(0)
                    )
                  )
                )
              }
            }
            target_prdcts = _bv_all_zeros()
            alt_prdcts = _bv_all_zeros()
            contents = Array[Row]()
            _ST_flag = BigDecimal(0)
          }
    
          _target_prdcts = get_products(row.getAs[Seq[Row]](5), row.getAs[Seq[Row]](8))
          if (_bv_count_one_bits(_target_prdcts) > 0) {
            if (row.getAs[Seq[Row]](5).toArray.indexWhere(r ⇒ r.getAs[String](0) == "ST_STEP_NUM") > -1) {
              _ST_flag = BigDecimal(1)
            }
            _alt_rule_def = get_products(row.getAs[Seq[Row]](6), row.getAs[Seq[Row]](9))
    
            if (_bv_count_one_bits(_alt_prdcts) > 0) {
              target_prdcts = _bv_or(target_prdcts, _target_prdcts)
              alt_prdcts = _bv_or(alt_prdcts,       _alt_prdcts)
              contents = Array.concat(
                contents,
                Array.fill(1)(
                  Row(
                    _target_prdcts,
                    _alt_prdcts,
                    _ST_flag,
                    BigDecimal(row.getAs[String](2))
                  )
                )
              )
            }
            _ST_flag = BigDecimal(0);
          }
        }
    
        if (_bv_count_one_bits(target_prdcts) > 0 && _bv_count_one_bits(alt_prdcts) > 0) {
          if (length_of(contents) > 1) {
            tac_contents = Array.concat(
              tac_contents,
              Array.fill(1)(
                Row(
                  target_prdcts,
                  alt_prdcts,
                  contents,
                  BigDecimal(1)
                )
              )
            )
          } else {
            tac_contents = Array.concat(
              tac_contents,
              Array.fill(1)(
                Row(
                  target_prdcts,
                  alt_prdcts,
                  contents,
                  BigDecimal(0)
                )
              )
            )
          }
        }
    
        var tac_id   = inputRows.toArray.last.getAs[BigDecimal](0)
        var tac_name = inputRows.toArray.last.getAs[String](1)
        var eff_dt   = inputRows.toArray.last.getAs[String](3)
        var term_dt  = inputRows.toArray.last.getAs[String](4)
        var newline  = inputRows.toArray.last.getAs[String](7)
    
        Row(
          tac_id,
          tac_name,
          tac_contents,
          eff_dt,
          term_dt,
          newline
        )
      },
    
      StructType(
        StructField("tac_id",   DecimalType(10, 0), true),
        StructField("tac_name", StringType, true),
        StructField(
          "tac_contents",
          ArrayType(
            StructType(
              StructField("target_prdcts", BinaryType, true),
              StructField("alt_prdcts",    BinaryType, true),
              StructField(
                "contents",
                ArrayType(
                  StructType(
                    StructField("target_prdcts", BinaryType,        true),
                    StructField("alt_prdcts",    BinaryType,        true),
                    StructField("ST_flag",       DecimalType(1, 0), true),
                    StructField("priority",      DecimalType(1, 0), true)
                  ),
                  true
                ),
                true
              ),
              StructField("xtra_proc_flg", DecimalType(1, 0), true)
            ),
            true
          ),
          true
        ),
        StructField("eff_dt",  StringType, true),
        StructField("term_dt", StringType, true),
        StructField("newline", StringType, true)
      )
    )
    
    val origColumns = in0.columns.map(col)
    val out0 = in0
      .groupBy("tac_id")
      .agg(
        collect_list(
          struct(
            (origColumns :+ lv_prdcts(col("target_rule_def")) :+ lv_prdcts(col("alt_rule_def"))): _*
          )
        ).alias("inputRows")
      )
      .select((process_udf(col("inputRows"))).alias("output"))
      .select(col("output.*"))
    out0
  }

}
