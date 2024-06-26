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

object Scan_Remove_Exclusion_Products_from_Inclusion_and_Standard_Association {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import _root_.io.prophecy.abinitio.ScalaFunctions._
    import scala.util.control._
    
    val process_udf = udf({ (input: Seq[Row]) =>
        val outputRows = scala.collection.mutable.ArrayBuffer[Row]()
    
        var excl_target_prdcts = Array[Byte]()
        var excl_alt_prdcts = Array[Byte]()
        var target_prdcts = Array[Array[Byte]]()
        var shared_qual_prdcts = Array[Array[Byte]]()
        var alt_constituent_prdcts_vec = Array[Row]()
        var lv_constituent_grp_vec = Array[String]()
        input.foreach { in =>
            var lv_target_prdcts = _bv_all_zeros()
            var lv_alt_prdcts = _bv_all_zeros()
            target_prdcts = Array[Array[Byte]]()
            alt_constituent_prdcts_vec = Array[Row]()
            lv_constituent_grp_vec = Array[String]()
    
            if (in.getAs[String]("tal_assoc_type_cd").toInt == 3) {
                excl_target_prdcts = _bv_or(excl_target_prdcts, _bv_vector_or(in.getSeq[Array[Byte]](7).toArray))
                excl_alt_prdcts = _bv_or(excl_alt_prdcts, _bv_vector_or(
                    in.getAs[Seq[Row]]("alt_constituent_prdcts").toArray.map ( alt_consti_prdcts =>
                        alt_consti_prdcts.getAs[Array[Byte]]("alt_prdcts")
                    )
                ))
            } else {
                if (in.getAs[String]("shared_qual") == "N/A") {
                    var shared_qual_target_prdcts = _bv_all_zeros()
                    var shared_qual_alt_prdcts = _bv_all_zeros()
                    var lv_target_prdcts_vec = Array[Array[Byte]]()
                    shared_qual_target_prdcts = _bv_difference(in.getSeq[Array[Byte]](7).head, excl_target_prdcts)
                    shared_qual_alt_prdcts = if (in.getAs[Seq[Row]]("alt_constituent_prdcts").nonEmpty) {
                        _bv_difference(in.getAs[Seq[Row]]("alt_constituent_prdcts").head.getAs[Array[Byte]]("alt_prdcts"), excl_alt_prdcts)
                    } else {
                        _bv_all_zeros()
                    }
                    if ( _bv_count_one_bits(shared_qual_alt_prdcts) > 0 )  {
                        shared_qual_prdcts = in.getSeq[Array[Byte]](14).toArray
                        val loop = new Breaks;
                        loop.breakable {
                            shared_qual_prdcts.foreach { shared_prdcts =>
                                 if( _bv_count_one_bits(shared_qual_target_prdcts) > 0 && _bv_count_one_bits(shared_qual_alt_prdcts) > 0) {
                                    lv_target_prdcts = _bv_and(shared_qual_target_prdcts, shared_prdcts)
                                    if( _bv_count_one_bits(lv_target_prdcts) > 0 ) {
                                        shared_qual_target_prdcts = _bv_difference(shared_qual_target_prdcts, lv_target_prdcts)
                                        lv_alt_prdcts             = _bv_and(shared_qual_alt_prdcts, shared_prdcts)
                                        if ( _bv_count_one_bits(lv_alt_prdcts) > 0) {
                                            target_prdcts = Array.concat(target_prdcts, Array.fill(1)(lv_target_prdcts))
                                            alt_constituent_prdcts_vec = Array.concat(alt_constituent_prdcts_vec,
                                                Array.fill(1)(Row(lv_alt_prdcts, in.getAs[Seq[Row]]("alt_constituent_prdcts").head.getAs[String]("udl_nm"), "", ""))
                                            )
                                            shared_qual_alt_prdcts = _bv_difference(shared_qual_alt_prdcts, lv_alt_prdcts)
                                        } else {
                                            lv_target_prdcts_vec = Array.concat(lv_target_prdcts_vec, Array.fill(1)(lv_target_prdcts))
                                        }
                                    }
                                 } else {
                                    loop.break
                                 }
                            }
                        }
                    }
                    if ( _bv_count_one_bits(shared_qual_target_prdcts) > 0) {
                        lv_target_prdcts_vec = Array.concat(lv_target_prdcts_vec, Array.fill(1)(shared_qual_target_prdcts))
                    }
                    target_prdcts = Array.concat(target_prdcts, lv_target_prdcts_vec)
                } else {
                    in.getSeq[Array[Byte]](7).foreach { target_prds =>
                        lv_target_prdcts = _bv_difference(target_prds, excl_target_prdcts)
                        if ( _bv_count_one_bits(lv_target_prdcts) > 0 ) {
                            target_prdcts = Array.concat(target_prdcts, Array.fill(1)(lv_target_prdcts))
                        }
                    }
                    
                    in.getAs[Seq[Row]]("alt_constituent_prdcts").foreach { alt_constituent_prdct =>
                        lv_alt_prdcts = _bv_difference(alt_constituent_prdct.getAs[Array[Byte]]("alt_prdcts"), excl_alt_prdcts)
                        if ( _bv_count_one_bits(lv_alt_prdcts) > 0 ) {
                            if ( !_isnull(alt_constituent_prdct.getAs[String]("constituent_group") ) 
                                && !lv_constituent_grp_vec.contains((alt_constituent_prdct.getAs[String]("constituent_group")))
                                && alt_constituent_prdct.getAs[String]("constituent_reqd") == "Y") {
    
                                lv_constituent_grp_vec = Array.concat(lv_constituent_grp_vec, 
                                                                    Array.fill(1)(alt_constituent_prdct.getAs[String]("constituent_group")))  
                            }
                            alt_constituent_prdcts_vec = Array.concat(alt_constituent_prdcts_vec,
                             Array.fill(1)(Row(
                                lv_alt_prdcts,
                                alt_constituent_prdct.getAs[String]("constituent_group"),
                                alt_constituent_prdct.getAs[String]("constituent_reqd"),
                                alt_constituent_prdct.getAs[String]("udl_nm")
                             ))
                            )
                        }
                    }
    
                    if (in.getAs[Seq[String]]("constituent_grp_vec").toSet.diff(lv_constituent_grp_vec.toSet).nonEmpty) {
                        alt_constituent_prdcts_vec = alt_constituent_prdcts_vec.filter(xx => xx.getAs[String](1) == null)
                        lv_constituent_grp_vec = Array[String]()
                    }
                }
            }
            outputRows.append(
                Row(
                    in.getAs[java.math.BigDecimal](0),
                    in.getAs[String](1),
                    in.getAs[String](2),
                    in.getAs[String](3),
                    in.getAs[String](4),
                    in.getAs[String](5),
                    in.getAs[String](6),
                    target_prdcts,
                    alt_constituent_prdcts_vec,
                    in.getAs[String](9),
                    in.getAs[String](10),
                    in.getAs[String](11),
                    lv_constituent_grp_vec,
                    in.getAs[String](13),
                )
            )
        }
        outputRows.toArray
    },
    ArrayType(
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
      StructField("newline",             StringType,                  false)
    ))
    ))
    
    val origColumns = in0.columns.map(col)
    val out0 = in0
        .groupBy("run_eff_dt")
        .agg(
        collect_list(
            struct(
                (origColumns :+ lookup("LKP_Shared_Qualifier_Products", col("shared_qual")).getField("shared_qual_prdcts") ) : _*
            )
        ).alias("inputRows")
        )
        .select(explode(process_udf(col("inputRows"))).alias("output"))
        .select(col("output.*"))
    out0
  }

}
