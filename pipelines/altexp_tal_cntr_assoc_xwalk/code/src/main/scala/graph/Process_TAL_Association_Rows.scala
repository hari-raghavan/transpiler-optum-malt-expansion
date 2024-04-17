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

object Process_TAL_Association_Rows {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import _root_.io.prophecy.abinitio.ScalaFunctions._
    
    val process_udf = udf(
      { (inputRows: Seq[Row]) ⇒
        var target_prdcts              = Array[Array[Byte]]()
        var alt_prdcts                 = Array[Byte]()
        var target_udl_name            = Array[String]()
        var alternate_udl_name         = Array[String]()
        var alt_constituent_prdcts_vec = Array[Row]()
        var constituent_grp_vec        = Array[String]()
    
        val inputRowsSorted = inputRows.toArray.sortBy(
          r => (r.getAs[String]("tal_assoc_name"), 
          r.getAs[String]("alt_rank"), 
          r.getAs[String]("priority"), 
          r.getAs[String]("constituent_group"), 
          r.getAs[String]("constituent_rank"))
        )
        inputRowsSorted.foreach { row ⇒
          var trgt_prdcts_lkp = _bv_all_zeros()
          var alt_prdcts_lkp  = _bv_all_zeros()
    
          if (!_isnull(row.getAs[String]("target_udl_name")) && !target_udl_name.contains(row.getAs[String]("target_udl_name"))) {
            trgt_prdcts_lkp = if (row.getAs[Array[Byte]](17) != null) {
              row.getAs[Array[Byte]](17).toArray
            } else {
              _bv_all_zeros()
            }
            if (target_prdcts.nonEmpty && _bv_count_one_bits(trgt_prdcts_lkp) > 0) {
              trgt_prdcts_lkp = _bv_difference(trgt_prdcts_lkp, _bv_vector_or(target_prdcts))
            }
    
            target_udl_name = Array.concat(target_udl_name, Array.fill(1)(row.getAs[String]("target_udl_name")))
            if (_bv_count_one_bits(trgt_prdcts_lkp) > 0) {
              target_prdcts = Array.concat(target_prdcts, Array.fill(1)(trgt_prdcts_lkp))
            }
          }
    
          if (!_isnull(row.getAs[String]("alt_udl_name")) && !alternate_udl_name.contains(row.getAs[String]("alt_udl_name"))) {
            alt_prdcts_lkp = if (row.getAs[Array[Byte]](18) != null) {
              row.getAs[Array[Byte]](18).toArray
            } else {
              _bv_all_zeros()
            }
            if (alt_constituent_prdcts_vec.nonEmpty && _bv_count_one_bits(alt_prdcts_lkp) > 0) {
              alt_prdcts_lkp = _bv_difference(alt_prdcts_lkp, alt_prdcts)
              alt_prdcts = _bv_or(alt_prdcts,                 alt_prdcts_lkp)
            }
    
            alternate_udl_name = Array.concat(alternate_udl_name, Array.fill(1)(row.getAs[String]("alt_udl_name")))
    
            if (!_isnull(row.getAs[String]("constituent_group"))) {
              if (row.getAs[String]("constituent_reqd") == "Y" && !constituent_grp_vec.contains(row.getAs[String]("constituent_group"))) {
                constituent_grp_vec = Array.concat(constituent_grp_vec, Array.fill(1)(row.getAs[String]("constituent_group")))
              }
              if (_bv_count_one_bits(alt_prdcts_lkp) > 0) {
                alt_constituent_prdcts_vec = Array.concat(alt_constituent_prdcts_vec,
                                                          Array.fill(1)(Row(
                                                            alt_prdcts_lkp,
                                                            row.getAs[String]("constituent_group"),
                                                            row.getAs[String]("constituent_reqd"),
                                                            row.getAs[String]("alt_udl_name")
                                                          ))
                )
              }
            } else {
              if (_bv_count_one_bits(alt_prdcts_lkp) > 0) {
                alt_constituent_prdcts_vec = Array.concat(alt_constituent_prdcts_vec,
                                                          Array.fill(1)(Row(
                                                            alt_prdcts_lkp,
                                                            null,
                                                            null,
                                                            row.getAs[String]("alt_udl_name")
                                                          ))
                )
              }
            }
          }
        }
    
        var lastRow = inputRows.toArray.last
    
        if (lastRow.getAs[String]("tal_assoc_type_cd").toInt == 3) {
          target_prdcts =
            if (target_prdcts.isEmpty && alt_constituent_prdcts_vec.nonEmpty) Array(_bv_all_zeros()) else target_prdcts
          alt_constituent_prdcts_vec =
            if (target_prdcts.nonEmpty && alt_constituent_prdcts_vec.isEmpty) Array(Row(_bv_all_zeros(), "", "", "", ""))
            else alt_constituent_prdcts_vec
        }
    
        Row(
          lastRow.getAs[java.math.BigDecimal](0),
          lastRow.getAs[String](1),
          lastRow.getAs[String](3),
          lastRow.getAs[String](6),
          lastRow.getAs[String](4),
          lastRow.getAs[String](15),
          lastRow.getAs[String](5),
          target_prdcts,
          alt_constituent_prdcts_vec,
          lastRow.getAs[String](12),
          lastRow.getAs[String](13),
          lastRow.getAs[String](14),
          constituent_grp_vec,
          lastRow.getAs[String](16)
        )
      },
      StructType(List(
        StructField("tal_id",            DecimalType(10, 0),          true),
        StructField("tal_name",          StringType,                  true),
        StructField("tal_assoc_name",    StringType,                  true),
        StructField("tar_udl_nm",        StringType,                  true),
        StructField("tal_desc",          StringType,                  true),
        StructField("priority",          StringType,                  true),
        StructField("tal_assoc_type_cd", StringType,                  true),
        StructField("target_prdcts",     ArrayType(BinaryType, true), true),
        StructField(
          "alt_constituent_prdcts",
          ArrayType(
            StructType(List(
              StructField("alt_prdcts",        BinaryType, true),
              StructField("constituent_group", StringType, true),
              StructField("constituent_reqd",  StringType, true),
              StructField("udl_nm",            StringType, true)
            )),
            true
          ),
          true
        ),
        StructField("shared_qual",         StringType,                  true),
        StructField("override_tac_name",   StringType,                  true),
        StructField("override_tar_name",   StringType,                  true),
        StructField("constituent_grp_vec", ArrayType(StringType, true), true),
        StructField("newline",             StringType,                  true)
      ))
    )
    
    val origColumns = in0.columns.map(col)
    val out0 = in0
      .groupBy("tal_assoc_id")
      .agg(
        collect_list(
          struct(
            (origColumns :+ lookup("Expanded_UDL", col("target_udl_name"))
              .getField("products") :+ lookup("Expanded_UDL", col("alt_udl_name")).getField("products")): _*
          )
        ).alias("inputRows")
      )
      .select((process_udf(col("inputRows"))).alias("output"))
      .select(col("output.*"))
    out0
  }

}
