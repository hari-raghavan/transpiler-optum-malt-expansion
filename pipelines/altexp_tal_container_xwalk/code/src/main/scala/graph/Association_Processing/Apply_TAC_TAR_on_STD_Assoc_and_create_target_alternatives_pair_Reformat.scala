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

object Apply_TAC_TAR_on_STD_Assoc_and_create_target_alternatives_pair_Reformat {
  def apply(context: Context, in: DataFrame, prod_dtl_df: DataFrame, expanded_UDL_wt_rl_priority_df: DataFrame, rule_prod_dtl_df: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import _root_.io.prophecy.abinitio.ScalaFunctions._
    import scala.util.control.Breaks
    
    val prod_dtl_var = spark.sparkContext.broadcast(
      prod_dtl_df.select(col("dl_bit"), col("gpi14")).collect().map(r ⇒ (r.getAs[Int](0) → r.getAs[String](1))).toMap
    )
    
    val expanded_UDL_wt_rl_priority_var = spark.sparkContext.broadcast(
      expanded_UDL_wt_rl_priority_df
        .select(col("udl_nm"), col("contents"))
        .collect()
        .map(r ⇒ (r.getAs[String](0) → r.getSeq[Array[Byte]](1)))
        .toMap
    )
    val rule_prod_dtl_var = spark.sparkContext.broadcast(
      rule_prod_dtl_df
        .filter((col("qualifer_cd") === lit("GPI14")) && (col("operator") === lit("eq")))
        .select(col("compare_value"), col("products"))
        .collect()
        .map(r ⇒ (r.getAs[String](0) → r.getAs[Array[Byte]](1)))
        .toMap
    )
    
    def apply_tar(
                    udl_prdcts: Array[Array[Byte]],
                    alt_udl_prdcts: Array[Row],
                    tar_prdcts: Row,
                    is_alt: Int,
                    target_tar_prdcts_mapping: Array[Row],
                    shared_qualifier: String,
                    is_override_incl: Int
                  ) = {
      var tar_prdcts_mapping = Array[Row]()
      var _prdcts = Array[Byte]()
      var lv_prdcts = Array[Byte]()
      var left_over_prdcts = Array[Byte]()
      var contents = Array[Row]()
      var len = 0
      var udl_len = 0
      if (is_alt > 0) {
        val tar_prdcts_contents = tar_prdcts.getAs[Seq[Row]](4).toArray
        len = tar_prdcts_contents(0).getAs[Seq[Row]](1).length
        udl_len = alt_udl_prdcts.length
        tar_prdcts_contents.foreach { c ⇒
          contents = Array.concat(contents, Array.fill(1)(Row(c.getAs[Seq[Row]](1).head.getSeq[Array[Byte]](0))))
        }
      } else {
        val tar_prdcts_contents = tar_prdcts.getAs[Seq[Row]](4).toArray
        len = tar_prdcts_contents(0).getSeq[Array[Byte]](0).length
        udl_len = alt_udl_prdcts.length
        contents = tar_prdcts_contents.slice(1, tar_prdcts_contents.length)
      }
    
      for (i ← 0 until udl_len) {
        _prdcts = _bv_all_zeros()
        tar_prdcts_mapping = Array.concat(tar_prdcts_mapping, Array.fill(1)(Row(Array())))
        if (is_alt > 0) {
          if (shared_qualifier == "N/A") {
            var udl_prdts = alt_udl_prdcts(i).getAs[Array[Byte]](0)
            for (p ← target_tar_prdcts_mapping.indices) {
              val tar_prdcts_contents = tar_prdcts.getAs[Seq[Row]](4).toArray
              val alt_contents = tar_prdcts_contents(0).getAs[Seq[Row]](1).toArray
              target_tar_prdcts_mapping(p).getAs[Seq[Int]](1).foreach { idx ⇒
                if (is_override_incl > 0) {
                  _prdcts = _bv_and(udl_prdts, alt_contents(idx).getAs[Array[Byte]](2))
                  udl_prdts = _bv_difference(udl_prdts, _prdcts)
                } else {
                  _prdcts = _bv_and(alt_udl_prdcts(i).getAs[Array[Byte]](0), alt_contents(idx).getAs[Array[Byte]](2))
                }
    
                if (_bv_count_one_bits(_prdcts) > 0) {
                  var alt_prds = alt_contents(idx).getSeq[Array[Byte]](0).toArray
                  var alt_len = alt_prds.length
                  val loop = new Breaks
                  loop.breakable {
                    for (n ← 0 until alt_len) {
                      lv_prdcts = _bv_and(_prdcts, alt_prds(n))
                      if (_bv_count_one_bits(lv_prdcts) > 0) {
                        tar_prdcts_mapping(i) = updateIndexInRow(
                          tar_prdcts_mapping(i),
                          1,
                          Array.concat(tar_prdcts_mapping(i).getAs[Seq[Int]](1).toArray, Array.fill(1)(idx))
                        )
                        tar_prdcts_mapping(i) = updateIndexInRow(
                          tar_prdcts_mapping(i),
                          0,
                          Array.concat(
                            tar_prdcts_mapping(i).getAs[Seq[Row]](0).toArray,
                            Array.fill(1)(
                              Row(
                                idx,
                                n,
                                i,
                                lv_prdcts,
                                _bv_all_zeros(),
                                _bv_all_zeros(),
                                Array[Array[Byte]](),
                                Row(_bv_all_zeros(),
                                  Array[Array[Byte]](),
                                  _bv_all_zeros(),
                                  Array[Array[Byte]](),
                                  _bv_all_zeros(),
                                  Array[Array[Byte]](),
                                  _bv_all_zeros(),
                                  Array[Array[Byte]]()
                                ),
                                Row(_bv_all_zeros(),
                                  Array[Array[Byte]](),
                                  _bv_all_zeros(),
                                  Array[Array[Byte]](),
                                  _bv_all_zeros(),
                                  Array[Array[Byte]](),
                                  _bv_all_zeros(),
                                  Array[Array[Byte]]()
                                ),
                                alt_udl_prdcts(i).getAs[String](1),
                                alt_udl_prdcts(i).getAs[String](2),
                                alt_udl_prdcts(i).getAs[String](3)
                              )
                            )
                          )
                        )
    
                        _prdcts = _bv_difference(_prdcts, lv_prdcts)
                        if (_bv_count_one_bits(_prdcts) == 0) {
                          loop.break()
                        }
                      }
                    }
                  }
                }
              }
            }
            if (is_override_incl > 0 && _bv_count_one_bits(udl_prdts) > 0) {
              tar_prdcts_mapping(i) = updateIndexInRow(
                tar_prdcts_mapping(i),
                0,
                Array.concat(
                  tar_prdcts_mapping(i).getAs[Seq[Row]](0).toArray,
                  Array.fill(1)(
                    Row(
                      tar_prdcts_mapping(i).getAs[Seq[Row]](1).length,
                      0,
                      i,
                      lv_prdcts,
                      _bv_all_zeros(),
                      _bv_all_zeros(),
                      Array[Array[Byte]](),
                      Row(_bv_all_zeros(),
                        Array[Array[Byte]](),
                        _bv_all_zeros(),
                        Array[Array[Byte]](),
                        _bv_all_zeros(),
                        Array[Array[Byte]](),
                        _bv_all_zeros(),
                        Array[Array[Byte]]()
                      ),
                      Row(_bv_all_zeros(),
                        Array[Array[Byte]](),
                        _bv_all_zeros(),
                        Array[Array[Byte]](),
                        _bv_all_zeros(),
                        Array[Array[Byte]](),
                        _bv_all_zeros(),
                        Array[Array[Byte]]()
                      ),
                      alt_udl_prdcts(i).getAs[String](1),
                      alt_udl_prdcts(i).getAs[String](2),
                      alt_udl_prdcts(i).getAs[String](3)
                    )
                  )
                )
              )
            }
          } else {
            var udl_prdts = alt_udl_prdcts(i).getAs[Array[Byte]](0)
            target_tar_prdcts_mapping(i).getAs[Seq[Int]](1).foreach { idx ⇒
              val tar_prdcts_contents = tar_prdcts.getAs[Seq[Row]](4).toArray
              val alt_contents = tar_prdcts_contents(0).getAs[Seq[Row]](1).toArray
              if (is_override_incl > 0) {
                _prdcts = _bv_and(udl_prdts, alt_contents(idx).getAs[Array[Byte]](2))
                udl_prdts = _bv_difference(udl_prdts, _prdcts)
              } else {
                _prdcts = _bv_and(alt_udl_prdcts(i).getAs[Array[Byte]](0), alt_contents(idx).getAs[Array[Byte]](2))
              }
    
              if (_bv_count_one_bits(_prdcts) > 0) {
                var alt_prds = alt_contents(idx).getSeq[Array[Byte]](0).toArray
                var alt_len = alt_prds.length
                val loop = new Breaks
                loop.breakable {
                  for (n ← 0 until alt_len) {
                    lv_prdcts = _bv_and(_prdcts, alt_prds(n))
                    if (_bv_count_one_bits(lv_prdcts) > 0) {
                      tar_prdcts_mapping(i) =
                        updateIndexInRow(tar_prdcts_mapping(i),
                          1,
                          Array.concat(tar_prdcts_mapping(i).getAs[Seq[Int]](1).toArray, Array.fill(1)(idx))
                        )
                      tar_prdcts_mapping(i) = updateIndexInRow(
                        tar_prdcts_mapping(i),
                        0,
                        Array.concat(
                          tar_prdcts_mapping(i).getAs[Seq[Row]](0).toArray,
                          Array.fill(1)(
                            Row(
                              idx,
                              n,
                              i,
                              lv_prdcts,
                              _bv_all_zeros(),
                              _bv_all_zeros(),
                              Array[Array[Byte]](),
                              Row(_bv_all_zeros(),
                                Array[Array[Byte]](),
                                _bv_all_zeros(),
                                Array[Array[Byte]](),
                                _bv_all_zeros(),
                                Array[Array[Byte]](),
                                _bv_all_zeros(),
                                Array[Array[Byte]]()
                              ),
                              Row(_bv_all_zeros(),
                                Array[Array[Byte]](),
                                _bv_all_zeros(),
                                Array[Array[Byte]](),
                                _bv_all_zeros(),
                                Array[Array[Byte]](),
                                _bv_all_zeros(),
                                Array[Array[Byte]]()
                              ),
                              alt_udl_prdcts(i).getAs[String](1),
                              alt_udl_prdcts(i).getAs[String](2),
                              alt_udl_prdcts(i).getAs[String](3)
                            )
                          )
                        )
                      )
    
                      _prdcts = _bv_difference(_prdcts, lv_prdcts)
                      if (_bv_count_one_bits(_prdcts) == 0) {
                        loop.break()
                      }
                    }
                  }
                }
              }
            }
            if (is_override_incl > 0 && _bv_count_one_bits(udl_prdts) > 0) {
              tar_prdcts_mapping(i) = updateIndexInRow(
                tar_prdcts_mapping(i),
                0,
                Array.concat(
                  tar_prdcts_mapping(i).getAs[Seq[Row]](0).toArray,
                  Array.fill(1)(
                    Row(
                      tar_prdcts_mapping(i).getAs[Seq[Row]](1).length,
                      0,
                      i,
                      lv_prdcts,
                      _bv_all_zeros(),
                      _bv_all_zeros(),
                      Array[Array[Byte]](),
                      Row(_bv_all_zeros(),
                        Array[Array[Byte]](),
                        _bv_all_zeros(),
                        Array[Array[Byte]](),
                        _bv_all_zeros(),
                        Array[Array[Byte]](),
                        _bv_all_zeros(),
                        Array[Array[Byte]]()
                      ),
                      Row(_bv_all_zeros(),
                        Array[Array[Byte]](),
                        _bv_all_zeros(),
                        Array[Array[Byte]](),
                        _bv_all_zeros(),
                        Array[Array[Byte]](),
                        _bv_all_zeros(),
                        Array[Array[Byte]]()
                      ),
                      alt_udl_prdcts(i).getAs[String](1),
                      alt_udl_prdcts(i).getAs[String](2),
                      alt_udl_prdcts(i).getAs[String](3)
                    )
                  )
                )
              )
            }
          }
        } else {
          _prdcts = udl_prdcts(i)
          val loop = new Breaks
          loop.breakable {
            for (m ← 0 until len) {
              val tar_prdcts_contents = tar_prdcts.getAs[Seq[Row]](4).toArray
              val target_prdcts = tar_prdcts_contents(0).getSeq[Array[Byte]](0).toArray
              lv_prdcts = _bv_and(_prdcts, target_prdcts(m))
              if (_bv_count_one_bits(lv_prdcts) > 0) {
                tar_prdcts_mapping(i) =
                  updateIndexInRow(tar_prdcts_mapping(i),
                    1,
                    Array.concat(tar_prdcts_mapping(i).getAs[Seq[Int]](1).toArray, Array.fill(1)(m))
                  )
                tar_prdcts_mapping(i) = updateIndexInRow(
                  tar_prdcts_mapping(i),
                  0,
                  Array.concat(
                    tar_prdcts_mapping(i).getAs[Seq[Row]](0).toArray,
                    Array.fill(1)(
                      Row(
                        m,
                        0,
                        i,
                        lv_prdcts,
                        _bv_all_zeros(),
                        _bv_all_zeros(),
                        Array[Array[Byte]](),
                        Row(_bv_all_zeros(),
                          Array[Array[Byte]](),
                          _bv_all_zeros(),
                          Array[Array[Byte]](),
                          _bv_all_zeros(),
                          Array[Array[Byte]](),
                          _bv_all_zeros(),
                          Array[Array[Byte]]()
                        ),
                        Row(_bv_all_zeros(),
                          Array[Array[Byte]](),
                          _bv_all_zeros(),
                          Array[Array[Byte]](),
                          _bv_all_zeros(),
                          Array[Array[Byte]](),
                          _bv_all_zeros(),
                          Array[Array[Byte]]()
                        ),
                        "",
                        "",
                        ""
                      )
                    )
                  )
                )
    
                _prdcts = _bv_difference(_prdcts, lv_prdcts)
                if (_bv_count_one_bits(_prdcts) == 0) {
                  loop.break()
                }
              }
            }
            if (_bv_count_one_bits(_prdcts) > 0)
              left_over_prdcts = _prdcts
            _prdcts =
              if (_bv_count_one_bits(contents(0).getSeq[Array[Byte]](0).head) > 0)
                _bv_and(left_over_prdcts, contents(0).getSeq[Array[Byte]](0).head)
              else left_over_prdcts
    
            if (_bv_count_one_bits(left_over_prdcts) > 0) {
              tar_prdcts_mapping(i) = updateIndexInRow(
                tar_prdcts_mapping(i),
                0,
                Array.concat(
                  tar_prdcts_mapping(i).getAs[Seq[Row]](0).toArray,
                  Array.fill(1)(
                    Row(
                      len,
                      0,
                      i,
                      lv_prdcts,
                      _bv_all_zeros(),
                      _bv_all_zeros(),
                      Array[Array[Byte]](),
                      Row(_bv_all_zeros(),
                        Array[Array[Byte]](),
                        _bv_all_zeros(),
                        Array[Array[Byte]](),
                        _bv_all_zeros(),
                        Array[Array[Byte]](),
                        _bv_all_zeros(),
                        Array[Array[Byte]]()
                      ),
                      Row(_bv_all_zeros(),
                        Array[Array[Byte]](),
                        _bv_all_zeros(),
                        Array[Array[Byte]](),
                        _bv_all_zeros(),
                        Array[Array[Byte]](),
                        _bv_all_zeros(),
                        Array[Array[Byte]]()
                      ),
                      "",
                      "",
                      ""
                    )
                  )
                )
              )
            }
          }
        }
        val buckets = tar_prdcts_mapping(i).getAs[Seq[Row]](0).toArray
        for (k ← 0 until tar_prdcts_mapping(i).getAs[Seq[Row]](0).length) {
          for (j ← 0 until contents.length) {
            val target_prdcts = contents(j).getSeq[Array[Byte]](0).head
            if (_bv_count_one_bits(target_prdcts) > 0) {
              if (
                _bv_count_one_bits(buckets(k).getAs[Array[Byte]](4)) == 0 || _bv_count_one_bits(
                  buckets(k).getAs[Array[Byte]](5)
                ) > 0
              ) {
                buckets(k) = updateIndexInRow(buckets(k), 4, _bv_and(buckets(k).getAs[Array[Byte]](3), target_prdcts))
                buckets(k) = updateIndexInRow(
                  buckets(k),
                  5,
                  _bv_difference(buckets(k).getAs[Array[Byte]](3), buckets(k).getAs[Array[Byte]](4))
                )
              } else if (
                !(_bv_count_one_bits(buckets(k).getAs[Row](7).getAs[Array[Byte]](0)) > 0 ||
                  _bv_count_one_bits(buckets(k).getAs[Row](7).getAs[Array[Byte]](4)) > 0 ||
                  _bv_count_one_bits(buckets(k).getAs[Row](8).getAs[Array[Byte]](0)) > 0 ||
                  _bv_count_one_bits(buckets(k).getAs[Row](8).getAs[Array[Byte]](4)) > 0 ||
                  _bv_count_one_bits(buckets(k).getAs[Row](7).getAs[Array[Byte]](2)) > 0 ||
                  _bv_count_one_bits(buckets(k).getAs[Row](7).getAs[Array[Byte]](6)) > 0 ||
                  _bv_count_one_bits(buckets(k).getAs[Row](8).getAs[Array[Byte]](2)) > 0 ||
                  _bv_count_one_bits(buckets(k).getAs[Row](8).getAs[Array[Byte]](6)) > 0)
              ) {
                if (
                  _bv_count_one_bits(buckets(k).getAs[Array[Byte]](4)) > 0 && !(_bv_count_one_bits(
                    buckets(k).getAs[Row](7).getAs[Array[Byte]](2)
                  ) > 0
                    && _bv_count_one_bits(buckets(k).getAs[Row](7).getAs[Array[Byte]](6)) > 0) &&
                    !(buckets(k).getAs[Row](7).getSeq[Array[Byte]](1).nonEmpty || buckets(k)
                      .getAs[Row](7)
                      .getSeq[Array[Byte]](3)
                      .nonEmpty || buckets(k).getAs[Row](7).getSeq[Array[Byte]](5).nonEmpty || buckets(k)
                      .getAs[Row](7)
                      .getSeq[Array[Byte]](7)
                      .nonEmpty)
                ) {
                  var sorted_prdcts = buckets(k).getAs[Row](7)
                  sorted_prdcts =
                    updateIndexInRow(sorted_prdcts, 2, _bv_and(buckets(k).getAs[Array[Byte]](4), target_prdcts))
                  sorted_prdcts = updateIndexInRow(
                    sorted_prdcts,
                    6,
                    _bv_difference(buckets(k).getAs[Array[Byte]](4), sorted_prdcts.getAs[Array[Byte]](2))
                  )
                  buckets(k) = updateIndexInRow(buckets(k), 7, sorted_prdcts)
                }
    
                if (
                  _bv_count_one_bits(buckets(k).getAs[Array[Byte]](5)) > 0 && !(_bv_count_one_bits(
                    buckets(k).getAs[Row](8).getAs[Array[Byte]](2)
                  ) > 0
                    && _bv_count_one_bits(buckets(k).getAs[Row](8).getAs[Array[Byte]](6)) > 0) &&
                    !(buckets(k).getAs[Row](8).getSeq[Array[Byte]](1).nonEmpty || buckets(k)
                      .getAs[Row](8)
                      .getSeq[Array[Byte]](3)
                      .nonEmpty || buckets(k).getAs[Row](8).getSeq[Array[Byte]](5).nonEmpty || buckets(k)
                      .getAs[Row](8)
                      .getSeq[Array[Byte]](7)
                      .nonEmpty)
                ) {
                  var unsorted_prdcts = buckets(k).getAs[Row](8)
                  unsorted_prdcts =
                    updateIndexInRow(unsorted_prdcts, 2, _bv_and(buckets(k).getAs[Array[Byte]](5), target_prdcts))
                  unsorted_prdcts = updateIndexInRow(
                    unsorted_prdcts,
                    6,
                    _bv_difference(buckets(k).getAs[Array[Byte]](5), unsorted_prdcts.getAs[Array[Byte]](2))
                  )
                  buckets(k) = updateIndexInRow(buckets(k), 8, unsorted_prdcts)
                }
              } else if (
                !(_bv_count_one_bits(buckets(k).getAs[Row](7).getAs[Array[Byte]](0)) > 0 ||
                  _bv_count_one_bits(buckets(k).getAs[Row](7).getAs[Array[Byte]](4)) > 0 ||
                  _bv_count_one_bits(buckets(k).getAs[Row](8).getAs[Array[Byte]](0)) > 0 ||
                  _bv_count_one_bits(buckets(k).getAs[Row](8).getAs[Array[Byte]](4)) > 0)
              ) {
    
                if (
                  (_bv_count_one_bits(
                    buckets(k).getAs[Row](7).getAs[Array[Byte]](2)
                  ) > 0 || _bv_count_one_bits(buckets(k).getAs[Row](7).getAs[Array[Byte]](6)) > 0) && !(_bv_count_one_bits(
                    buckets(k).getAs[Row](7).getAs[Array[Byte]](0)
                  ) > 0 || _bv_count_one_bits(buckets(k).getAs[Row](7).getAs[Array[Byte]](4)) > 0)
                ) {
                  var sorted_prdcts = buckets(k).getAs[Row](7)
                  sorted_prdcts =
                    updateIndexInRow(sorted_prdcts, 0, _bv_and(buckets(k).getAs[Array[Byte]](2), target_prdcts))
                  sorted_prdcts = updateIndexInRow(
                    sorted_prdcts,
                    2,
                    _bv_difference(buckets(k).getAs[Array[Byte]](2), sorted_prdcts.getAs[Array[Byte]](0))
                  )
                  sorted_prdcts =
                    updateIndexInRow(sorted_prdcts, 4, _bv_and(buckets(k).getAs[Array[Byte]](6), target_prdcts))
                  sorted_prdcts = updateIndexInRow(
                    sorted_prdcts,
                    6,
                    _bv_difference(buckets(k).getAs[Array[Byte]](6), sorted_prdcts.getAs[Array[Byte]](4))
                  )
                  buckets(k) = updateIndexInRow(buckets(k), 7, sorted_prdcts)
                }
    
                if (
                  (_bv_count_one_bits(
                    buckets(k).getAs[Row](8).getAs[Array[Byte]](2)
                  ) > 0 || _bv_count_one_bits(buckets(k).getAs[Row](8).getAs[Array[Byte]](6)) > 0) && !(_bv_count_one_bits(
                    buckets(k).getAs[Row](8).getAs[Array[Byte]](0)
                  ) > 0 || _bv_count_one_bits(buckets(k).getAs[Row](8).getAs[Array[Byte]](4)) > 0)
                ) {
                  var unsorted_prdcts = buckets(k).getAs[Row](8)
                  unsorted_prdcts =
                    updateIndexInRow(unsorted_prdcts, 0, _bv_and(buckets(k).getAs[Array[Byte]](2), target_prdcts))
                  unsorted_prdcts = updateIndexInRow(
                    unsorted_prdcts,
                    2,
                    _bv_difference(buckets(k).getAs[Array[Byte]](2), unsorted_prdcts.getAs[Array[Byte]](0))
                  )
                  unsorted_prdcts =
                    updateIndexInRow(unsorted_prdcts, 4, _bv_and(buckets(k).getAs[Array[Byte]](6), target_prdcts))
                  unsorted_prdcts = updateIndexInRow(
                    unsorted_prdcts,
                    6,
                    _bv_difference(buckets(k).getAs[Array[Byte]](6), unsorted_prdcts.getAs[Array[Byte]](4))
                  )
                  buckets(k) = updateIndexInRow(buckets(k), 8, unsorted_prdcts)
                }
    
              } else {
                if (_bv_count_one_bits(buckets(k).getAs[Row](7).getAs[Array[Byte]](0)) > 0) {
                  _prdcts = _bv_and(buckets(k).getAs[Row](7).getAs[Array[Byte]](0), target_prdcts)
                  if (_bv_count_one_bits(_prdcts) > 0) {
                    var sorted_prdcts = buckets(k).getAs[Row](7)
                    sorted_prdcts = updateIndexInRow(sorted_prdcts,
                      1,
                      Array.concat(sorted_prdcts.getSeq[Array[Byte]](1).toArray,
                        Array.fill(1)(_prdcts))
                    )
                    sorted_prdcts = updateIndexInRow(sorted_prdcts,
                      0,
                      _bv_difference(sorted_prdcts.getAs[Array[Byte]](0).toArray, _prdcts)
                    )
                    buckets(k) = updateIndexInRow(buckets(k), 7, sorted_prdcts)
                    _prdcts = _bv_all_zeros()
                  }
                }
    
                if (_bv_count_one_bits(buckets(k).getAs[Row](7).getAs[Array[Byte]](2)) > 0) {
                  _prdcts = _bv_and(buckets(k).getAs[Row](7).getAs[Array[Byte]](2), target_prdcts)
                  if (_bv_count_one_bits(_prdcts) > 0) {
                    var sorted_prdcts = buckets(k).getAs[Row](7)
                    sorted_prdcts = updateIndexInRow(sorted_prdcts,
                      3,
                      Array.concat(sorted_prdcts.getSeq[Array[Byte]](3).toArray,
                        Array.fill(1)(_prdcts))
                    )
                    sorted_prdcts = updateIndexInRow(sorted_prdcts,
                      2,
                      _bv_difference(sorted_prdcts.getAs[Array[Byte]](2).toArray, _prdcts)
                    )
                    buckets(k) = updateIndexInRow(buckets(k), 7, sorted_prdcts)
                    _prdcts = _bv_all_zeros()
                  }
                }
    
                if (_bv_count_one_bits(buckets(k).getAs[Row](7).getAs[Array[Byte]](4)) > 0) {
                  _prdcts = _bv_and(buckets(k).getAs[Row](7).getAs[Array[Byte]](4), target_prdcts)
                  if (_bv_count_one_bits(_prdcts) > 0) {
                    var sorted_prdcts = buckets(k).getAs[Row](7)
                    sorted_prdcts = updateIndexInRow(sorted_prdcts,
                      5,
                      Array.concat(sorted_prdcts.getSeq[Array[Byte]](5).toArray,
                        Array.fill(1)(_prdcts))
                    )
                    sorted_prdcts = updateIndexInRow(sorted_prdcts,
                      4,
                      _bv_difference(sorted_prdcts.getAs[Array[Byte]](4).toArray, _prdcts)
                    )
                    buckets(k) = updateIndexInRow(buckets(k), 7, sorted_prdcts)
                    _prdcts = _bv_all_zeros()
                  }
                }
    
                if (_bv_count_one_bits(buckets(k).getAs[Row](7).getAs[Array[Byte]](6)) > 0) {
                  _prdcts = _bv_and(buckets(k).getAs[Row](7).getAs[Array[Byte]](6), target_prdcts)
                  if (_bv_count_one_bits(_prdcts) > 0) {
                    var sorted_prdcts = buckets(k).getAs[Row](7)
                    sorted_prdcts = updateIndexInRow(sorted_prdcts,
                      7,
                      Array.concat(sorted_prdcts.getSeq[Array[Byte]](7).toArray,
                        Array.fill(1)(_prdcts))
                    )
                    sorted_prdcts = updateIndexInRow(sorted_prdcts,
                      6,
                      _bv_difference(sorted_prdcts.getAs[Array[Byte]](6).toArray, _prdcts)
                    )
                    buckets(k) = updateIndexInRow(buckets(k), 7, sorted_prdcts)
                    _prdcts = _bv_all_zeros()
                  }
                }
    
                if (_bv_count_one_bits(buckets(k).getAs[Row](8).getAs[Array[Byte]](0)) > 0) {
                  _prdcts = _bv_and(buckets(k).getAs[Row](8).getAs[Array[Byte]](0), target_prdcts)
                  if (_bv_count_one_bits(_prdcts) > 0) {
                    var unsorted_prdcts = buckets(k).getAs[Row](8)
                    unsorted_prdcts = updateIndexInRow(unsorted_prdcts,
                      1,
                      Array.concat(unsorted_prdcts.getSeq[Array[Byte]](1).toArray,
                        Array.fill(1)(_prdcts))
                    )
                    unsorted_prdcts = updateIndexInRow(
                      unsorted_prdcts,
                      0,
                      _bv_difference(unsorted_prdcts.getAs[Array[Byte]](0).toArray, _prdcts)
                    )
                    buckets(k) = updateIndexInRow(buckets(k), 8, unsorted_prdcts)
                    _prdcts = _bv_all_zeros()
                  }
                }
    
                if (_bv_count_one_bits(buckets(k).getAs[Row](8).getAs[Array[Byte]](2)) > 0) {
                  _prdcts = _bv_and(buckets(k).getAs[Row](8).getAs[Array[Byte]](2), target_prdcts)
                  if (_bv_count_one_bits(_prdcts) > 0) {
                    var unsorted_prdcts = buckets(k).getAs[Row](8)
                    unsorted_prdcts = updateIndexInRow(unsorted_prdcts,
                      3,
                      Array.concat(unsorted_prdcts.getSeq[Array[Byte]](3).toArray,
                        Array.fill(1)(_prdcts))
                    )
                    unsorted_prdcts = updateIndexInRow(
                      unsorted_prdcts,
                      2,
                      _bv_difference(unsorted_prdcts.getAs[Array[Byte]](2).toArray, _prdcts)
                    )
                    buckets(k) = updateIndexInRow(buckets(k), 8, unsorted_prdcts)
                    _prdcts = _bv_all_zeros()
                  }
                }
    
                if (_bv_count_one_bits(buckets(k).getAs[Row](8).getAs[Array[Byte]](4)) > 0) {
                  _prdcts = _bv_and(buckets(k).getAs[Row](8).getAs[Array[Byte]](4), target_prdcts)
                  if (_bv_count_one_bits(_prdcts) > 0) {
                    var unsorted_prdcts = buckets(k).getAs[Row](8)
                    unsorted_prdcts = updateIndexInRow(unsorted_prdcts,
                      5,
                      Array.concat(unsorted_prdcts.getSeq[Array[Byte]](5).toArray,
                        Array.fill(1)(_prdcts))
                    )
                    unsorted_prdcts = updateIndexInRow(
                      unsorted_prdcts,
                      4,
                      _bv_difference(unsorted_prdcts.getAs[Array[Byte]](4).toArray, _prdcts)
                    )
                    buckets(k) = updateIndexInRow(buckets(k), 8, unsorted_prdcts)
                    _prdcts = _bv_all_zeros()
                  }
                }
    
                if (_bv_count_one_bits(buckets(k).getAs[Row](8).getAs[Array[Byte]](6)) > 0) {
                  _prdcts = _bv_and(buckets(k).getAs[Row](8).getAs[Array[Byte]](6), target_prdcts)
                  if (_bv_count_one_bits(_prdcts) > 0) {
                    var unsorted_prdcts = buckets(k).getAs[Row](8)
                    unsorted_prdcts = updateIndexInRow(unsorted_prdcts,
                      7,
                      Array.concat(unsorted_prdcts.getSeq[Array[Byte]](7).toArray,
                        Array.fill(1)(_prdcts))
                    )
                    unsorted_prdcts = updateIndexInRow(
                      unsorted_prdcts,
                      6,
                      _bv_difference(unsorted_prdcts.getAs[Array[Byte]](6).toArray, _prdcts)
                    )
                    buckets(k) = updateIndexInRow(buckets(k), 8, unsorted_prdcts)
                    _prdcts = _bv_all_zeros()
                  }
                }
              }
            }
          }
          if (_bv_count_one_bits(buckets(k).getAs[Array[Byte]](4)) > 0) {
            var sorted_prdcts = buckets(k).getAs[Row](7)
            if (_bv_count_one_bits(buckets(k).getAs[Row](7).getAs[Array[Byte]](0)) > 0) {
              sorted_prdcts = updateIndexInRow(sorted_prdcts,
                1,
                Array.concat(sorted_prdcts.getAs[Array[Byte]](1).toArray,
                  buckets(k).getAs[Row](7).getAs[Array[Byte]](0)
                )
              )
            }
            if (_bv_count_one_bits(buckets(k).getAs[Row](7).getAs[Array[Byte]](2)) > 0) {
              sorted_prdcts = updateIndexInRow(sorted_prdcts,
                3,
                Array.concat(sorted_prdcts.getAs[Array[Byte]](3).toArray,
                  buckets(k).getAs[Row](7).getAs[Array[Byte]](2)
                )
              )
            }
            if (_bv_count_one_bits(buckets(k).getAs[Row](7).getAs[Array[Byte]](4)) > 0) {
              sorted_prdcts = updateIndexInRow(sorted_prdcts,
                5,
                Array.concat(sorted_prdcts.getAs[Array[Byte]](5).toArray,
                  buckets(k).getAs[Row](7).getAs[Array[Byte]](4)
                )
              )
            }
            if (_bv_count_one_bits(buckets(k).getAs[Row](7).getAs[Array[Byte]](6)) > 0) {
              sorted_prdcts = updateIndexInRow(sorted_prdcts,
                7,
                Array.concat(sorted_prdcts.getAs[Array[Byte]](7).toArray,
                  buckets(k).getAs[Row](7).getAs[Array[Byte]](6)
                )
              )
            }
            buckets(k) = updateIndexInRow(buckets(k), 7, sorted_prdcts)
            buckets(k) = updateIndexInRow(
              buckets(k),
              6,
              Array.concat(
                sorted_prdcts.getSeq[Array[Byte]](6).toArray,
                Array(
                  sorted_prdcts.getAs[Array[Byte]](1).toArray,
                  sorted_prdcts.getAs[Array[Byte]](3).toArray,
                  sorted_prdcts.getAs[Array[Byte]](5).toArray,
                  sorted_prdcts.getAs[Array[Byte]](7).toArray
                )
              )
            )
    
            if (
              sorted_prdcts.getAs[Array[Byte]](1).isEmpty && sorted_prdcts.getAs[Array[Byte]](3).isEmpty && sorted_prdcts
                .getAs[Array[Byte]](5)
                .isEmpty && sorted_prdcts.getAs[Array[Byte]](7).isEmpty
            ) {
              buckets(k) = updateIndexInRow(
                buckets(k),
                6,
                Array.concat(sorted_prdcts.getSeq[Array[Byte]](6).toArray, Array.fill(1)(buckets(k).getAs[Array[Byte]](4)))
              )
            }
          }
    
          if (_bv_count_one_bits(buckets(k).getAs[Array[Byte]](5)) > 0) {
            var unsorted_prdcts = buckets(k).getAs[Row](8)
            if (_bv_count_one_bits(buckets(k).getAs[Row](8).getAs[Array[Byte]](0)) > 0) {
              unsorted_prdcts = updateIndexInRow(unsorted_prdcts,
                1,
                Array.concat(unsorted_prdcts.getAs[Array[Byte]](1).toArray,
                  buckets(k).getAs[Row](8).getAs[Array[Byte]](0)
                )
              )
            }
            if (_bv_count_one_bits(buckets(k).getAs[Row](8).getAs[Array[Byte]](2)) > 0) {
              unsorted_prdcts = updateIndexInRow(unsorted_prdcts,
                3,
                Array.concat(unsorted_prdcts.getAs[Array[Byte]](3).toArray,
                  buckets(k).getAs[Row](8).getAs[Array[Byte]](2)
                )
              )
            }
            if (_bv_count_one_bits(buckets(k).getAs[Row](8).getAs[Array[Byte]](4)) > 0) {
              unsorted_prdcts = updateIndexInRow(unsorted_prdcts,
                5,
                Array.concat(unsorted_prdcts.getAs[Array[Byte]](5).toArray,
                  buckets(k).getAs[Row](8).getAs[Array[Byte]](4)
                )
              )
            }
            if (_bv_count_one_bits(buckets(k).getAs[Row](8).getAs[Array[Byte]](6)) > 0) {
              unsorted_prdcts = updateIndexInRow(unsorted_prdcts,
                7,
                Array.concat(unsorted_prdcts.getAs[Array[Byte]](7).toArray,
                  buckets(k).getAs[Row](8).getAs[Array[Byte]](6)
                )
              )
            }
            buckets(k) = updateIndexInRow(buckets(k), 8, unsorted_prdcts)
            buckets(k) = updateIndexInRow(
              buckets(k),
              6,
              Array.concat(
                unsorted_prdcts.getSeq[Array[Byte]](6).toArray,
                Array(
                  unsorted_prdcts.getAs[Array[Byte]](1).toArray,
                  unsorted_prdcts.getAs[Array[Byte]](3).toArray,
                  unsorted_prdcts.getAs[Array[Byte]](5).toArray,
                  unsorted_prdcts.getAs[Array[Byte]](7).toArray
                )
              )
            )
    
            if (
              unsorted_prdcts
                .getAs[Array[Byte]](1)
                .isEmpty && unsorted_prdcts.getAs[Array[Byte]](3).isEmpty && unsorted_prdcts
                .getAs[Array[Byte]](5)
                .isEmpty && unsorted_prdcts.getAs[Array[Byte]](7).isEmpty
            ) {
              buckets(k) = updateIndexInRow(buckets(k),
                6,
                Array.concat(unsorted_prdcts.getSeq[Array[Byte]](6).toArray,
                  Array.fill(1)(buckets(k).getAs[Array[Byte]](5))
                )
              )
            }
          }
    
          if (
            _bv_count_one_bits(buckets(k).getAs[Array[Byte]](4)) == 0 && _bv_count_one_bits(
              buckets(k).getAs[Array[Byte]](5)
            ) == 0 && _bv_count_one_bits(buckets(k).getAs[Array[Byte]](3)) > 0
          ) {
            buckets(k) = updateIndexInRow(buckets(k), 6, 
              Array.concat(buckets(k).getSeq[Array[Byte]](6).toArray, Array.fill(1)(buckets(k).getAs[Array[Byte]](3)))
            )
          }
        }
        tar_prdcts_mapping(i) = updateIndexInRow(tar_prdcts_mapping(i), 0, buckets)
      }
      tar_prdcts_mapping
    }
    
    def gpi14_sorting(
                        dl_bits: Array[Int],
                        gpi14_container: Array[Int],
                        alt_rank: Int,
                        in_constituent_group: String,
                        in_constituent_reqd: String,
                        udl_nm: String
                      ) = {
      var gpi14_matching_dl_bits = Array[Int]()
      var gpi14_non_matching_dl_bits = Array[Int]()
      var lv_dl_bit = 0
      var alt_rank_vec = Array[Row]()
      var len = 0
      var lv_alt_rank = alt_rank
    
      gpi14_matching_dl_bits = dl_bits.intersect(gpi14_container)
      dl_bits.foreach { _dlbit ⇒
        if (!gpi14_matching_dl_bits.contains(_dlbit)) {
          gpi14_non_matching_dl_bits = Array.concat(gpi14_non_matching_dl_bits, Array.fill(1)(_dlbit))
        }
      }
      len = gpi14_matching_dl_bits.length
      if (len > 0) {
        alt_rank_vec = Array.concat(
          alt_rank_vec,
          Array.fill(1)(Row(lv_alt_rank, gpi14_matching_dl_bits, in_constituent_group, in_constituent_reqd, udl_nm))
        )
        lv_alt_rank = lv_alt_rank + len
      }
      len = gpi14_non_matching_dl_bits.length
      if (len > 0) {
        alt_rank_vec = Array.concat(
          alt_rank_vec,
          Array.fill(1)(Row(lv_alt_rank, gpi14_non_matching_dl_bits, in_constituent_group, in_constituent_reqd, udl_nm))
        )
        lv_alt_rank = lv_alt_rank + len
      }
      alt_rank_vec
    }
    
    def apply_gpi14_sorting(
                              target_dl_bits: Array[Int],
                              bucket_index: Int,
                              udl_index: Int,
                              products: Array[Byte],
                              alt_bucket_vec: Array[Row],
                              tac_contents: Row,
                              tar_roa_df_alt: Array[Row],
                              steptal_inp: Array[Row],
                              tar_udl_nm: String,
                              final_sorted_prdcts_step1: Array[Array[Byte]]
                            ) = {
      var unsorted_target_prdct_dtl = Array[Row]()
      var alt_rank_vec = Array[Row]()
      var lv_alt_rank_vec = Array[Row]()
      var alt_dl_bits = Array[Int]()
      var lv_target_dl_bits = Array[Int]()
      var lv_gpi14_dl_bits = Array[Int]()
      var lv_target_dl_bits1 = Array[Int]()
      var lv_target_prdcts = _bv_all_zeros()
      var lv_target_prdcts_sec = _bv_all_zeros()
      var tac_target_dl_bits = Array[Int]()
      var lv_tac_target_dl_bits = Array[Int]()
    
      var tar_idx = 0
      var alt_idx = 0
      var tar_roa_df_alt_cntnt = Array[Array[Byte]]()
      var tar_roa_df_alt_cntnt1 = Array[Array[Byte]]()
      var tar_roa_df_alt_cntnt2 = Array[Array[Byte]]()
      var lv_udl_index = BigDecimal(0)
      var udl_prdct_count = 0
      var _gpi14 = ""
      var unmatching_udl_prd = _bv_all_zeros()
      var tac_index = 0
      var target_tac_index = Array[Int]()
      var in_tar_vec = Array[Array[Byte]]()
      var tar_bits = Array[Int]()
      var final_sorted_prdcts1 = Array[Array[Byte]]()
      var final_sorted_prdcts2 = Array[Array[Byte]]()
      var st_len = 0
      var len_now = 0
      var len_alt = 0
      var length_overlapping = 0
      var alt_idx1 = 0
      var alt_idx2 = Array[Int]()
      val prod_dtl = prod_dtl_var.value
      val rule_prod_dtl = rule_prod_dtl_var.value
      target_dl_bits.foreach { dl_bit ⇒
        var wc = 0
        if (!lv_target_dl_bits.contains(dl_bit)) {
          var alt_rank = 1
          var tac_idx = -1
          var idx = -1
          var idx_ord = -1
          var first_hit_priority = 0
          target_tac_index = Array[Int]()
          lv_target_prdcts_sec = _bv_all_zeros()
          final_sorted_prdcts1 = Array[Array[Byte]]()
          final_sorted_prdcts2 = Array[Array[Byte]]()
          len_now = alt_bucket_vec.length
          st_len = steptal_inp.length
          length_overlapping = final_sorted_prdcts_step1.length
          tar_bits = Array[Int]()
          _gpi14 = prod_dtl(dl_bit)
          lv_target_prdcts = _bv_and(rule_prod_dtl(_gpi14), products)
          val loop = new Breaks
          loop.breakable {
            tac_contents.getAs[Seq[Row]](2).foreach { rec ⇒
              var lv_target_prdcts1 = _bv_and(lv_target_prdcts, rec.getAs[Array[Byte]](0))
              target_tac_index = Array[Int]()
              lv_target_prdcts_sec = _bv_all_zeros()
              lv_tac_target_dl_bits = _bv_indices(lv_target_prdcts1)
              tac_idx = tac_idx + 1
              if (lv_tac_target_dl_bits.length > 0) {
                target_tac_index = Array.concat(target_tac_index, Array.fill(1)(tac_idx))
                first_hit_priority = rec.getAs[String](2).toInt
    
                var i = tac_idx + 1
                val contents = tac_contents.getAs[Seq[Row]](2).toArray
                while (i < contents.length && first_hit_priority == contents(i).getAs[String](2).toInt) {
                  lv_target_prdcts_sec = _bv_and(lv_target_prdcts1, contents(i).getAs[Array[Byte]](0))
                  if (_bv_count_one_bits(lv_target_prdcts_sec) > 0)
                    target_tac_index = Array.concat(target_tac_index, Array.fill(1)(i))
                  i = i + 1
                }
    
                alt_bucket_vec.foreach { bucket1 ⇒
                  tar_idx = bucket1.getAs[String](0).toInt
                  alt_idx = bucket1.getAs[String](1).toInt
                  val alt_prdcts_all_prio = tar_roa_df_alt(tar_idx).getAs[Seq[Seq[Array[Byte]]]](1).toArray.map(_.toArray)
                  tar_roa_df_alt_cntnt = alt_prdcts_all_prio(alt_idx)
    
                  unmatching_udl_prd = _bv_all_zeros()
                  if (
                    _bv_count_one_bits(bucket1.getAs[Array[Byte]](4)) > 0 ||
                      _bv_count_one_bits(bucket1.getAs[Array[Byte]](5)) > 0 ||
                      _bv_count_one_bits(bucket1.getAs[Array[Byte]](3)) > 0
                  ) {
                    bucket1.getSeq[Array[Byte]](6).foreach { alt_prds ⇒
                      unmatching_udl_prd = _bv_all_zeros()
                      target_tac_index.foreach { alt_tac_idx ⇒
                        val contents = tac_contents.getAs[Seq[Row]](2).toArray
                        unmatching_udl_prd =
                          _bv_or(unmatching_udl_prd, _bv_and(alt_prds, contents(alt_tac_idx).getAs[Array[Byte]](1)))
                      }
    
                      alt_dl_bits = apply_udl_sort(unmatching_udl_prd, bucket1.getAs[String](11), tar_roa_df_alt_cntnt)
    
                      if (alt_dl_bits.nonEmpty) {
                        lv_alt_rank_vec =
                          if (alt_dl_bits.length > 1)
                            gpi14_sorting(alt_dl_bits,
                              lv_gpi14_dl_bits,
                              alt_rank,
                              bucket1.getAs[String](9),
                              bucket1.getAs[String](10),
                              bucket1.getAs[String](11)
                            )
                          else
                            Array.fill(1)(
                              Row(alt_rank,
                                alt_dl_bits,
                                bucket1.getAs[String](9),
                                bucket1.getAs[String](10),
                                bucket1.getAs[String](11)
                              )
                            )
    
                        alt_rank_vec = Array.concat(alt_rank_vec, lv_alt_rank_vec)
                        udl_prdct_count = udl_prdct_count + alt_dl_bits.length
                        alt_rank =
                          if (lv_udl_index != BigDecimal(bucket1.getAs[String](3))) udl_prdct_count.toInt + 1
                          else alt_rank + alt_dl_bits.length
                        lv_udl_index = BigDecimal(bucket1.getAs[String](3))
                      }
                    }
                  }
                  len_now = len_alt - 1
                }
                udl_prdct_count = 0
                if (alt_rank_vec.nonEmpty) {
                  tac_index = tac_index + 1
                  lv_tac_target_dl_bits.foreach { p ⇒
                    unsorted_target_prdct_dtl = Array.concat(
                      unsorted_target_prdct_dtl,
                      Array.fill(1)(
                        Row(p,
                          bucket_index,
                          udl_index,
                          "",
                          tac_index,
                          alt_rank_vec,
                          "Y",
                          rec.getAs[String](3)
                        )
                      )
                    )
                  }
                  alt_rank_vec = Array[Row]()
                  lv_target_dl_bits = Array.concat(lv_target_dl_bits, lv_tac_target_dl_bits)
                  lv_target_prdcts = _bv_difference(lv_target_prdcts, lv_target_prdcts1)
                  if (_bv_count_one_bits(lv_target_prdcts) == 0) {
                    loop.break()
                  }
                }
              }
            }
          }
          lv_target_dl_bits1 = _bv_indices(lv_target_prdcts)
          lv_target_dl_bits1.foreach { p ⇒
            unsorted_target_prdct_dtl = Array.concat(
              unsorted_target_prdct_dtl,
              Array.fill(1)(Row(p, "0", "0", "", "0", Array[Row](), "N", "0"))
            )
          }
          lv_target_dl_bits = Array.concat(lv_target_dl_bits, lv_target_dl_bits1)
          lv_target_dl_bits1 = Array[Int]()
        }
      }
      unsorted_target_prdct_dtl
    }
    
    def apply_udl_sort(unmatching_udl_prd: Array[Byte], udl_nm: String, tar_roa_df_alt_cntnt: Array[Array[Byte]]) = {
    
      var matching_udl_prd = Array[Byte]()
      var lv_unmatching_udl_prd = Array[Byte]()
      var _roa_df_match = Array[Byte]()
      var lv_alt_dl_bits = Array[Int]()
      var alt_udl_content_lkp = Array[Array[Byte]]()
      var roa_df_alt_dtl = tar_roa_df_alt_cntnt
      var udl_rule_prd = Array[Row]()
    
      var lv_udl_nm = _ltrim(udl_nm)
      val prod_dtl = prod_dtl_var.value
    
      if ((lv_udl_nm == "N/A")) {
        _roa_df_match = unmatching_udl_prd
        val loop = new Breaks
        loop.breakable {
          roa_df_alt_dtl.foreach { k ⇒
            udl_rule_prd = Array[Row]()
            lv_unmatching_udl_prd = _bv_and(_roa_df_match, k)
            _roa_df_match = _bv_difference(_roa_df_match, lv_unmatching_udl_prd)
            if (_bv_count_one_bits(lv_unmatching_udl_prd) > 0) {
              _bv_indices(lv_unmatching_udl_prd).foreach { p ⇒
                udl_rule_prd = Array.concat(udl_rule_prd, Array.fill(1)(Row(p, prod_dtl(p))))
              }
              udl_rule_prd = udl_rule_prd.sortBy(_.getAs[String](1))
              lv_alt_dl_bits = Array.concat(lv_alt_dl_bits, udl_rule_prd.map(_.getAs[Int](0)))
            }
            if (_bv_count_one_bits(_roa_df_match) == 0) {
              loop.break()
            }
          }
        }
      } else {
        val expanded_UDL_wt_rl_priority = expanded_UDL_wt_rl_priority_var.value
        alt_udl_content_lkp = expanded_UDL_wt_rl_priority.get(lv_udl_nm).map(_.toArray).getOrElse(Array[Array[Byte]]())
        _roa_df_match = unmatching_udl_prd
        val loop = new Breaks
        loop.breakable {
          roa_df_alt_dtl.foreach { k ⇒
            lv_unmatching_udl_prd = _bv_and(_roa_df_match, k)
            _roa_df_match = _bv_difference(_roa_df_match, lv_unmatching_udl_prd)
    
            val innerLoop = new Breaks
            innerLoop.breakable {
              alt_udl_content_lkp.foreach { c ⇒
                udl_rule_prd = Array[Row]()
                matching_udl_prd = _bv_and(lv_unmatching_udl_prd, c)
                lv_unmatching_udl_prd = _bv_difference(lv_unmatching_udl_prd, matching_udl_prd)
                if (_bv_count_one_bits(matching_udl_prd) > 0) {
                  _bv_indices(matching_udl_prd).foreach { p ⇒
                    udl_rule_prd = Array.concat(udl_rule_prd, Array.fill(1)(Row(p, prod_dtl(p))))
                  }
                  udl_rule_prd = udl_rule_prd.sortBy(_.getAs[String](1))
                  lv_alt_dl_bits = Array.concat(lv_alt_dl_bits, udl_rule_prd.map(_.getAs[Int](0)))
                }
                if (_bv_count_one_bits(lv_unmatching_udl_prd) == 0) {
                  innerLoop.break()
                }
              }
            }
    
            if (_bv_count_one_bits(_roa_df_match) == 0) {
              loop.break()
            }
          }
        }
      }
      lv_alt_dl_bits
    }
    
    val process_udf = udf({ (in: Row) =>
      var tac_dtls = Row()
      var target_prdcts = Array[Array[Byte]]()
      var alt_prdcts = Array[Array[Byte]]()
      var indx = -1
      var tar_prdcts = Row()
      var target_tar_prdcts_mapping = Array[Row]()
      var alt_tar_prdcts_mapping = Array[Row]()
      var target_prdct_dtl = Array[Row]()
      var unsorted_target_prdct_dtl = Array[Row]()
      var alt_bucket_vec = Array[Row]()
      var alt_bucket_vec_step = Array[Row]()
      var target_dl_bits = Array[Int]()
      var alt_dl_bits = Array[Int]()
      var tac_target_dl_bits = Array[Int]()
      var lv_tac_target_dl_bits = Array[Int]()
      var alt_rank_vec = Array[Row]()
      var contents = Array[Row]()
      var lv_tac_contents =
        Row(Array[Byte](), Array[Byte](), Row(Array[Byte](), Array[Byte](), "0", "0"), "0")
      var lv_target_prdcts = _bv_all_zeros()
      var lv_alt_prdcts = _bv_all_zeros()
      var _target_prdcts = _bv_all_zeros()
      var _alt_prdcts = _bv_all_zeros()
      var _tac_target_prdcts = _bv_all_zeros()
      var _tac_alt_prdcts = _bv_all_zeros()
      var pos = 0
      var tar_idx_vec = Array[Int]()
      var _all_tac_target_prdcts = _bv_all_zeros()
      var _all_tac_alt_prdcts = _bv_all_zeros()
      var store_bucket_idx_t = Array[Row]()
      var index_vec = Array[Int]()
      var alt_prdcts_rank_vec = Array[Row]()
      var target_wo_alt_prdct_dtl = Array[Row]()
      var alt_constituent_prdcts_vec = Array[Row]()
      var step_alt_prdcts = Array[Byte]()
      var length_of_contents = 0
      var length_of_alt_contents = 0
      var tar_roa_df_alt = Array[Row]()
      var steptal_prdcts = Array[Row]()
      var steptal_inp = Array[Row]()
      var tar_st_target = _bv_all_zeros()
      var tar_prds = _bv_all_zeros()
      var step_alt_prds = _bv_all_zeros()
      var in_tar = _bv_all_zeros()
      var in_alt_step = _bv_all_zeros()
      var step_tar = _bv_all_zeros()
      var step_tar1 = _bv_all_zeros()
      var final_sorted_prdcts1 = Array[Array[Byte]]()
      var final_sorted_prdcts2 = Array[Array[Byte]]()
      var final_sorted_prdcts_step = Array[Array[Byte]]()
      var final_sorted_prdcts_step1 = Array[Array[Byte]]()
      var final_sorted_prdcts_overlapping = Array[Array[Byte]]()
      var number_of_alt_bucket = 0
      var st_len = 0
      var length_overlapping = 0
      var step_join_alternate = _bv_all_zeros()
      var step_alt_prdcts_tala = Array[Array[Byte]]()
    
      tar_prdcts = in.getAs[Row]("lkp_tar_exp_override_tar_name")
      tar_roa_df_alt = tar_prdcts.getAs[Seq[Row]]("contents").head.getAs[Seq[Row]]("alt_contents").toArray
    
      if (in.getAs[String]("tal_id").toInt == 0) {
        step_alt_prdcts = in.getAs[Seq[Row]]("alt_constituent_prdcts").head.getAs[Array[Byte]]("alt_prdcts")
        length_of_contents = tar_prdcts.getAs[Seq[Row]](4).length
        var contents = tar_prdcts.getAs[Seq[Row]](4).toArray
        for (c ← 0 until length_of_contents) {
          var alt_contents = contents(c).getAs[Seq[Row]](1).toArray
          length_of_alt_contents = alt_contents.length
          for (a ← 0 until length_of_alt_contents) {
            alt_contents(a) = updateIndexInRow(alt_contents(a),
              0,
              Array.concat(
                alt_contents(a).getSeq[Array[Byte]](0).toArray,
                Array.fill(1)(step_alt_prdcts)
              )
            )
            alt_contents(a) =
              updateIndexInRow(alt_contents(a), 2, _bv_or(alt_contents(a).getAs[Array[Byte]](2), step_alt_prdcts))
          }
          contents(c) = updateIndexInRow(contents(c), 1, alt_contents)
        }
        tar_prdcts = updateIndexInRow(tar_prdcts, 4, contents)
      }
    
      if (in.getAs[String]("tal_assoc_type_cd").toInt == 1) {
        tac_dtls = in.getAs[Row]("lkp_tac_exp_override_tar_name")
    
        var tac_contents = tac_dtls.getAs[Seq[Row]]("tac_contents").toArray
        for (i ← 0 until tac_contents.length) {
          lv_target_prdcts =
            if (tar_prdcts.getAs[String](7).toInt > 0) tac_contents(i).getAs[Array[Byte]]("target_prdcts")
            else _bv_and(tac_contents(i).getAs[Array[Byte]]("target_prdcts"), tar_prdcts.getAs[Array[Byte]](6))
    
          lv_alt_prdcts = _bv_and(tac_contents(i).getAs[Array[Byte]]("alt_prdcts"), tar_prdcts.getAs[Array[Byte]](5))
    
          if (_bv_count_one_bits(lv_target_prdcts) > 0 && _bv_count_one_bits(lv_alt_prdcts) > 0) {
            if (tac_contents(i).getAs[String]("xtra_proc_flg").toInt > 0) {
              tac_contents(i).getAs[Seq[Row]]("contents").foreach { rec ⇒
                _target_prdcts = _bv_and(lv_target_prdcts, rec.getAs[Array[Byte]]("target_prdcts"))
                _alt_prdcts = _bv_and(lv_alt_prdcts, rec.getAs[Array[Byte]]("alt_prdcts"))
                if (_bv_count_one_bits(_target_prdcts) > 0 && _bv_count_one_bits(_alt_prdcts) > 0) {
                  contents = Array.concat(
                    contents,
                    Array.fill(1)(
                      Row(_target_prdcts, _alt_prdcts, rec.getAs[String]("ST_flag"), rec.getAs[String]("priority"))
                    )
                  )
                  _tac_target_prdcts = _bv_or(_tac_target_prdcts, _target_prdcts)
                  _tac_alt_prdcts = _bv_or(_tac_alt_prdcts, _alt_prdcts)
                }
              }
    
              if (_bv_count_one_bits(_tac_target_prdcts) > 0 && _bv_count_one_bits(_tac_alt_prdcts) > 0) {
                _all_tac_target_prdcts = _bv_or(_all_tac_target_prdcts, _tac_target_prdcts)
                _all_tac_alt_prdcts = _bv_or(_all_tac_alt_prdcts, _tac_alt_prdcts)
                _tac_target_prdcts = _bv_all_zeros()
                _tac_alt_prdcts = _bv_all_zeros()
              }
            } else {
              contents = Array.concat(
                contents,
                Array.fill(1)(
                  Row(lv_target_prdcts,
                    lv_alt_prdcts,
                    tac_contents(i).getAs[Seq[Row]](2).head.getAs[String](2),
                    tac_contents(i).getAs[Seq[Row]](2).head.getAs[String](3)
                  )
                )
              )
              _all_tac_target_prdcts = _bv_or(_all_tac_target_prdcts, lv_target_prdcts)
              _all_tac_alt_prdcts = _bv_or(_all_tac_alt_prdcts, lv_alt_prdcts)
            }
          }
        }
        if (_bv_count_one_bits(_all_tac_target_prdcts) > 0) {
          lv_tac_contents = updateIndexInRow(lv_tac_contents, 0, _all_tac_target_prdcts)
          lv_tac_contents = updateIndexInRow(lv_tac_contents, 1, _all_tac_alt_prdcts)
          lv_tac_contents = updateIndexInRow(lv_tac_contents, 2, contents)
          lv_tac_contents = updateIndexInRow(lv_tac_contents, 3, (length_of(contents) > 1).toString)
        } else {
          _force_error("Applied TAR on TAC - No Products Left For Processing")
        }
        in.getSeq[Array[Byte]](7).foreach { target_prds ⇒
          target_prdcts = Array.concat(target_prdcts, Array.fill(1)(_bv_and(target_prds, _all_tac_target_prdcts)))
        }
        if (_bv_count_one_bits(_bv_vector_or(target_prdcts)) > 0)
          _force_error("TAC Applied - No Target Products Left For Processing")
    
        if (in.getAs[Seq[Row]]("alt_constituent_prdcts").length > 0) {
          in.getAs[Seq[Row]]("alt_constituent_prdcts").foreach { alt_consti_prdcts ⇒
            alt_constituent_prdcts_vec = Array.concat(
              alt_constituent_prdcts_vec,
              Array.fill(1)(
                Row(
                  _bv_and(alt_consti_prdcts.getAs[Array[Byte]]("alt_prdcts"), _all_tac_alt_prdcts),
                  alt_consti_prdcts.getAs[String]("constituent_group"),
                  alt_consti_prdcts.getAs[String]("constituent_reqd"),
                  alt_consti_prdcts.getAs[String]("udl_nm")
                )
              )
            )
          }
    
          target_tar_prdcts_mapping =
            apply_tar(target_prdcts, alt_constituent_prdcts_vec, tar_prdcts, 0, target_tar_prdcts_mapping, "", 0)
          alt_tar_prdcts_mapping = apply_tar(Array(),
            alt_constituent_prdcts_vec,
            tar_prdcts,
            1,
            target_tar_prdcts_mapping,
            in.getAs[String]("shared_qual"),
            0
          )
    
          if (in.getAs[String]("shared_qual") != "N/A") {
    
            target_tar_prdcts_mapping.foreach { target_prd ⇒
              var bucket_index = 0
              target_prd.getAs[Seq[Row]](0).foreach { bucket ⇒
                bucket_index = bucket_index + 1
                number_of_alt_bucket = 0
                length_overlapping = 0
                step_join_alternate = _bv_all_zeros()
                final_sorted_prdcts_step = Array[Array[Byte]]()
                alt_tar_prdcts_mapping.foreach { alt_prd ⇒
                  alt_bucket_vec = Array.concat(
                    alt_bucket_vec,
                    alt_prd.getAs[Seq[Row]](0).filter(r ⇒ r.getAs[String](0) == bucket.getAs[String](0)).toArray
                  )
                }
    
                bucket.getSeq[Array[Byte]](6).foreach { prds ⇒
                  target_dl_bits = Array.concat(target_dl_bits, _bv_indices(prds))
                }
                if (alt_bucket_vec.length > 0) {
                  alt_bucket_vec = alt_bucket_vec
                    .sortBy(r ⇒ (r.getAs[String](0).toInt, r.getAs[String](1).toInt, r.getAs[String](2).toInt))
                  unsorted_target_prdct_dtl = apply_gpi14_sorting(
                    target_dl_bits,
                    bucket_index,
                    bucket.getAs[String](2).toInt,
                    bucket.getAs[Array[Byte]](3),
                    alt_bucket_vec,
                    lv_tac_contents,
                    tar_roa_df_alt,
                    steptal_inp,
                    in.getAs[String]("tar_udl_nm"),
                    final_sorted_prdcts_step
                  )
    
                  target_dl_bits.foreach { p ⇒
                    val elem = unsorted_target_prdct_dtl.find(r ⇒ r.getAs[Int](0) == p).get
                    if (elem.getAs[String](6) == "Y") {
                      target_prdct_dtl = Array.concat(target_prdct_dtl, Array.fill(1)(elem))
                    } else {
                      target_wo_alt_prdct_dtl = Array.concat(target_wo_alt_prdct_dtl, Array.fill(1)(elem))
                    }
                  }
                  alt_bucket_vec = Array[Row]()
                } else {
                  target_dl_bits.foreach { p ⇒
                    target_wo_alt_prdct_dtl = Array.concat(target_wo_alt_prdct_dtl,
                      Array.fill(1)(
                        Row(
                          p,
                          "0",
                          "0",
                          "",
                          "0",
                          Array[Row](),
                          "N",
                          "0"
                        )
                      )
                    )
                  }
                }
                target_dl_bits = Array[Int]()
              }
            }
          } else {
            for (y ← 0 until alt_tar_prdcts_mapping.length) {
              tar_idx_vec = alt_tar_prdcts_mapping(y).getSeq[Int](1).toArray.sorted.distinct
              store_bucket_idx_t = Array.concat(store_bucket_idx_t, Array.fill(1)(Row(y, Array[String]())))
              index_vec = Array.concat(index_vec, Array.fill(1)(y))
              for (z ← 0 until tar_idx_vec.length) {
                pos = target_tar_prdcts_mapping(y).getSeq[Int](1).indexOf(tar_idx_vec(z))
                target_dl_bits = Array[Int]()
                alt_bucket_vec = alt_tar_prdcts_mapping(y)
                  .getAs[Seq[Row]](0)
                  .filter(r ⇒ r.getAs[String](0).toInt == tar_idx_vec(z) && r.getAs[String](2).toInt == y)
                  .toArray
    
                val buckets = target_tar_prdcts_mapping(y).getAs[Seq[Row]](0).toArray
                buckets(pos).getSeq[Array[Byte]](6).foreach { prds ⇒
                  target_dl_bits = Array.concat(target_dl_bits, _bv_indices(prds))
                  store_bucket_idx_t(y) = updateIndexInRow(
                    store_bucket_idx_t(y),
                    1,
                    Array.concat(store_bucket_idx_t(y).getSeq[String](1).toArray, Array.fill(1)(pos.toString))
                  )
                }
                if (in.getAs[String]("shared_qual") != "GPI14") {
                  unsorted_target_prdct_dtl = apply_gpi14_sorting(
                    target_dl_bits,
                    pos + 1,
                    buckets(pos).getAs[String](2).toInt,
                    buckets(pos).getAs[Array[Byte]](3),
                    alt_bucket_vec,
                    lv_tac_contents,
                    tar_roa_df_alt,
                    steptal_inp,
                    in.getAs[String]("tar_udl_nm"),
                    final_sorted_prdcts_step
                  )
                  target_dl_bits.foreach { p ⇒
                    val elem = unsorted_target_prdct_dtl.find(r ⇒ r.getAs[Int](0) == p).get
                    if (elem.getAs[String](6) == "Y") {
                      target_prdct_dtl = Array.concat(target_prdct_dtl, Array.fill(1)(elem))
                    } else {
                      target_wo_alt_prdct_dtl = Array.concat(target_wo_alt_prdct_dtl, Array.fill(1)(elem))
                    }
                  }
                  alt_bucket_vec = Array[Row]()
                } else {
                  var alt_rank = 1
                  var tac_index = 0
                  var unmatching_udl_prd = Array[Byte]()
                  tac_target_dl_bits = target_dl_bits
                  val loop = new Breaks
                  loop.breakable {
                    lv_tac_contents.getAs[Seq[Row]](2).foreach { rec ⇒
                      if (tac_target_dl_bits.length > 0) {
                        lv_tac_target_dl_bits =
                          _bv_indices(_bv_and(buckets(pos).getAs[Array[Byte]](3), rec.getAs[Array[Byte]]("target_prdcts")))
                        if (lv_tac_target_dl_bits.length > 0) {
                          alt_bucket_vec.foreach { bucket1 ⇒
                            if (_bv_count_one_bits(bucket1.getAs[Array[Byte]](4)) > 0 || _bv_count_one_bits(bucket1.getAs[Array[Byte]](5)) > 0) {
                              bucket1.getSeq[Array[Byte]](6).foreach { alt_prds ⇒
                                val alt_prdcts_all_prio_seq = tar_roa_df_alt(bucket1.getAs[String](0).toInt).getSeq[Seq[Array[Byte]]](2)
                                val alt_prdcts_all_prio =
                                  alt_prdcts_all_prio_seq.map(_.toArray).toArray
                                var tar_roa_df_alt_cntnt = alt_prdcts_all_prio(bucket1.getAs[String](1).toInt)
                                unmatching_udl_prd = _bv_and(alt_prds, rec.getAs[Array[Byte]]("alt_prdcts"))
                                alt_dl_bits = apply_udl_sort(unmatching_udl_prd, bucket1.getAs[String](11), tar_roa_df_alt_cntnt)
                                if (alt_dl_bits.length > 0) {
                                  alt_rank_vec =
                                    Array.concat(alt_rank_vec,
                                      Array.fill(1)(Row(alt_rank, alt_dl_bits, bucket1.getAs[String](11)))
                                    )
    
                                  alt_rank = alt_rank + bucket1.getSeq[Array[Byte]](6).length
                                }
                              }
                            }
                          }
                          if (alt_rank_vec.length > 0) {
                            tac_index = tac_index + 1
                            lv_tac_target_dl_bits.foreach { p ⇒
                              unsorted_target_prdct_dtl = Array.concat(unsorted_target_prdct_dtl,
                                Array.fill(1)(
                                  Row(
                                    p,
                                    (pos + 1).toString,
                                    buckets(pos).getAs[String](2),
                                    "",
                                    tac_index.toString,
                                    alt_rank_vec,
                                    "Y",
                                    "0"
                                  )
                                )
                              )
                            }
                            alt_rank_vec = Array[Row]()
                            tac_target_dl_bits = tac_target_dl_bits.diff(lv_tac_target_dl_bits)
                            alt_rank = 1
                          }
                        }
                      } else
                        loop.break()
                    }
                  }
                  tac_target_dl_bits.foreach { p =>
                    target_wo_alt_prdct_dtl = Array.concat(target_wo_alt_prdct_dtl, Array.fill(1)(Row(
                      p,
                      (pos + 1).toString,
                      buckets(pos).getAs[String](2),
                      "0",
                      "",
                      Array[Row](),
                      "N",
                      "0"
                    )))
                  }
    
                  target_dl_bits.foreach { p ⇒
                    target_prdct_dtl =
                      Array.concat(target_prdct_dtl, unsorted_target_prdct_dtl.filter(r ⇒ r.getAs[Int](0) == p))
                  }
                  alt_bucket_vec = Array[Row]()
                }
              }
            }
            for (i ← 0 until target_tar_prdcts_mapping.length) {
              val buckets = target_tar_prdcts_mapping(i).getAs[Seq[Row]](0).toArray
              if (index_vec.contains(i)) {
                for (j ← 0 until buckets.length) {
                  if (!store_bucket_idx_t(i).getSeq[String](1).contains(j.toString)) {
                    buckets(j).getSeq[Array[Byte]](6).foreach { prod_grp ⇒
                      target_wo_alt_prdct_dtl = Array.concat(target_wo_alt_prdct_dtl,
                        _bv_indices(prod_grp).map(p ⇒
                          Row(
                            p,
                            "0",
                            "0",
                            "",
                            "0",
                            Array[Row](),
                            "N",
                            "0"
                          )
                        )
                      )
                    }
                  }
                }
              } else {
                for (j ← 0 until buckets.length) {
                  target_wo_alt_prdct_dtl = Array
                    .concat(target_wo_alt_prdct_dtl, _bv_indices(_bv_vector_or(buckets(j).getSeq[Array[Byte]](6).toArray))
                      .map(p ⇒
                        Row(
                          p,
                          "0",
                          "0",
                          "",
                          "0",
                          Array[Row](),
                          "N",
                          "0"
                        )
                      ))
                }
              }
            }
          }
        } else {
          target_wo_alt_prdct_dtl = Array.concat(target_wo_alt_prdct_dtl,
            _bv_indices(_bv_vector_or(target_prdcts)).map(p ⇒
              Row(
                p,
                "0",
                "0",
                "",
                "0",
                Array[Row](),
                "N",
                "0"
              )
            )
          )
        }
      } else {
        var alt_rank = 1
        target_tar_prdcts_mapping = apply_tar(in.getSeq[Array[Byte]](7).toArray,
          in.getAs[Seq[Row]]("alt_constituent_prdcts").toArray,
          tar_prdcts,
          0,
          target_tar_prdcts_mapping,
          "",
          0)
    
        alt_tar_prdcts_mapping = apply_tar(Array(),
          in.getAs[Seq[Row]]("alt_constituent_prdcts").toArray,
          tar_prdcts,
          1,
          target_tar_prdcts_mapping,
          in.getAs[String]("shared_qual"),
          1)
    
        if (in.getAs[String]("shared_qual") != "N/A") {
          for (a ← 0 to alt_tar_prdcts_mapping.length) {
            alt_tar_prdcts_mapping(a).getAs[Seq[Row]](0).foreach { bucket ⇒
              val alt_prdcts_all_prio_seq = tar_roa_df_alt(bucket.getAs[String](0).toInt).getSeq[Seq[Array[Byte]]](2)
              val alt_prdcts_all_prio =
                alt_prdcts_all_prio_seq.map(_.toArray).toArray
              var tar_roa_df_alt_cntnt = alt_prdcts_all_prio(bucket.getAs[String](1).toInt)
    
              bucket.getSeq[Array[Byte]](6).foreach { prds ⇒
                alt_prdcts_rank_vec =
                  Array.concat(alt_prdcts_rank_vec,
                    Array.fill(1)(Row(alt_rank, apply_udl_sort(prds, bucket.getAs[String]("11"), tar_roa_df_alt_cntnt), "", ""))
                  )
                alt_rank = alt_rank + _bv_count_one_bits(prds)
              }
              val target_prdcts = in.getSeq[Array[Byte]](7).toArray
              target_prdct_dtl = Array.concat(target_prdct_dtl,
                _bv_indices(target_prdcts(a)).map { prd ⇒
                  Row(prd, "0", (a + 1).toString, "", "0", alt_prdcts_rank_vec, "Y", "0")
                }
              )
              alt_prdcts_rank_vec = Array[Row]()
              alt_rank = 1
            }
          }
    
          var b = alt_tar_prdcts_mapping.length
          val target_prdcts = in.getSeq[Array[Byte]](7).toArray
          while (b < target_prdcts.length) {
            target_prdct_dtl = Array.concat(target_prdct_dtl,
              _bv_indices(target_prdcts(b)).map { prd ⇒
                Row(prd, "0", "0", "", "0", Array(), "N", "0")
              }
            )
            b = b + 1
          }
        } else {
          alt_tar_prdcts_mapping.foreach { alt_prd ⇒
            alt_prd.getAs[Seq[Row]](0).foreach { bucket ⇒
              val alt_prdcts_all_prio_seq = tar_roa_df_alt(bucket.getAs[String](0).toInt).getSeq[Seq[Array[Byte]]](2)
              val alt_prdcts_all_prio =
                alt_prdcts_all_prio_seq.map(_.toArray).toArray
              var tar_roa_df_alt_cntnt = alt_prdcts_all_prio(bucket.getAs[String](1).toInt)
              bucket.getSeq[Array[Byte]](6).foreach { prds ⇒
                alt_prdcts_rank_vec = Array.concat(alt_prdcts_rank_vec,
                  Array.fill(1)(
                    Row(alt_rank,
                      apply_udl_sort(prds, bucket.getAs[String]("11"), tar_roa_df_alt_cntnt),
                      bucket.getAs[String](9),
                      bucket.getAs[String](10)
                    )
                  )
                )
                alt_rank = alt_rank + _bv_count_one_bits(prds)
              }
            }
          }
          val target_prdcts = in.getSeq[Array[Byte]](7).toArray
          if (alt_prdcts_rank_vec.length > 0) {
            var udl_index = 0
            target_prdcts.foreach { target_prd ⇒
              udl_index = udl_index + 1
              target_prdct_dtl = Array.concat(target_prdct_dtl,
                _bv_indices(target_prd).map { prd ⇒
                  Row(prd, "0", udl_index.toString, "", "0", alt_prdcts_rank_vec, "Y", "0")
                }
              )
            }
          } else {
            target_prdcts.foreach { target_prd ⇒
              target_wo_alt_prdct_dtl = Array.concat(target_wo_alt_prdct_dtl,
                _bv_indices(target_prd).map { prd ⇒
                  Row(prd, "0", "0", "", "0", Array[Row](), "N", "0")
                }
              )
            }
          }
        }
      }
    
      Row(
        in.getAs[String]("tal_id"),
        in.getAs[String]("tal_name"),
        in.getAs[String]("shared_qual"),
        in.getAs[String]("tal_assoc_name"),
        in.getAs[String]("tar_udl_nm"),
        in.getAs[String]("priority"),
        in.getAs[String]("tal_assoc_type_cd"),
        target_prdct_dtl,
        target_wo_alt_prdct_dtl,
        in.getAs[Seq[String]]("constituent_grp_vec").toArray,
        in.getAs[String]("newline")
      )
    },
      StructType(List(
        StructField("tal_id", StringType, true),
        StructField("tal_name", StringType, true),
        StructField("shared_qual", StringType, true),
        StructField("tal_assoc_name", StringType, true),
        StructField("tar_udl_nm", StringType, true),
        StructField("priority", StringType, true),
        StructField("tal_assoc_type_cd", StringType, true),
        StructField(
          "ta_prdct_dtls",
          ArrayType(
            StructType(List(
              StructField("target_dl_bit", IntegerType, true),
              StructField("bucket_index", StringType, true),
              StructField("udl_index", StringType, true),
              StructField("tar_udl_nm", StringType, true),
              StructField("tac_index", StringType, true),
              StructField(
                "alt_prdcts_rank",
                ArrayType(
                  StructType(List(
                    StructField("alt_rank", IntegerType, true),
                    StructField("alt_prdcts", ArrayType(IntegerType, true), true),
                    StructField("constituent_group", StringType, true),
                    StructField("constituent_reqd", StringType, true),
                    StructField("udl_nm", StringType, true)
                  )),
                  true
                ),
                true
              ),
              StructField("has_alt", StringType, true),
              StructField("ST_flag", StringType, true)
            )),
            true
          ),
          true
        ),
        StructField(
          "ta_prdct_dtls_wo_alt",
          ArrayType(
            StructType(List(
              StructField("target_dl_bit", IntegerType, true),
              StructField("bucket_index", StringType, true),
              StructField("udl_index", StringType, true),
              StructField("tar_udl_nm", StringType, true),
              StructField("tac_index", StringType, true),
              StructField(
                "alt_prdcts_rank",
                ArrayType(
                  StructType(List(
                    StructField("alt_rank", IntegerType, true),
                    StructField("alt_prdcts", ArrayType(IntegerType, true), true),
                    StructField("constituent_group", StringType, true),
                    StructField("constituent_reqd", StringType, true),
                    StructField("udl_nm", StringType, true)
                  )),
                  true
                ),
                true
              ),
              StructField("has_alt", StringType, true),
              StructField("ST_flag", DecimalType(1, 0), true)
            )),
            true
          ),
          true
        ),
        StructField("constituent_grp_vec", ArrayType(StringType, true), true),
        StructField("newline", StringType, true)
      ))
    )
    
    val origColumns = in.columns.map(col)
    val out = in
      .withColumn(
        "lkp_tar_exp_override_tar_name",
        when(
          col("tal_assoc_type_cd") === lit(1) && !isnull(
            col("override_tar_name") && lookup_match("LKP_TAR_Exp", col("override_tar_name")) == lit(1)
          ),
          lookup("LKP_TAR_Exp", col("override_tar_name"))
        )
          .otherwise(lookup("LKP_TAR_Exp", col(Config.TAR_NM)))
      )
      .withColumn(
        "lkp_tac_exp_override_tar_name",
        when(!isnull(col("override_tac_name") && lookup_match("LKP_TAC_Exp", col("override_tac_name")) == lit(1)),
          lookup("LKP_TAC_Exp", col("override_tac_name"))
        )
          .otherwise(lookup("LKP_TAC_Exp", col(Config.TAC_NM)))
      ).select(struct((origColumns ++ Array(col("lkp_tar_exp_override_tar_name"), col("lkp_tac_exp_override_tar_name"))): _*).as("input"))
      .select(process_udf(col("input")).as("output")).select(col("output.*"))
    out
  }

}
