package graph.TAL_Container_Assoc

import io.prophecy.libs._
import graph.TAL_Container_Assoc.config.Context
import udfs.UDFs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object TAL_Container_Assoc__Scan_Remove_Exclusion_Products_from_Inclusion_and_Standard_Association {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import _root_.io.prophecy.abinitio.ScalaFunctions._
    
    val process_udf = udf(
    { (input: Seq[Row]) ⇒
        val outputRows = scala.collection.mutable.ArrayBuffer[Row]()
    
        var excl_target_prdcts = Array[Byte]()
        var excl_alt_prdcts    = Array[Byte]()
        var target_prdcts      = Array[Row]()
        var alt_prdcts         = Array[Row]()
    
        input.foreach { in ⇒
        var lv_target_prdcts = Array[Byte]()
        var lv_alt_prdcts    = Array[Byte]()
        if (in.getAs[String]("tal_assoc_type_cd").toInt == 3) {
            excl_target_prdcts =
            _bv_or(excl_target_prdcts, _bv_vector_or(in.getSeq[Row](8).toArray.map(r ⇒ r.getAs[Array[Byte]](2))))
            excl_alt_prdcts =
            _bv_or(excl_alt_prdcts, _bv_vector_or(in.getSeq[Row](9).toArray.map(r ⇒ r.getAs[Array[Byte]](2))))
        } else {
            in.getSeq[Row](8).toArray.foreach { target_prds ⇒
                lv_target_prdcts = _bv_difference(target_prds.getAs[Array[Byte]](2), excl_target_prdcts)
                if (_bv_count_one_bits(lv_target_prdcts) > 0) {
                    target_prdcts = Array
                    .concat(
                        target_prdcts,
                        Array.fill(1)(
                        Row(target_prds.getAs[String](0), target_prds.getAs[String](1), lv_target_prdcts)
                        )
                    )
                }
            }
    
            in.getSeq[Row](9).toArray.foreach { alt_prds ⇒
                lv_alt_prdcts = _bv_difference(alt_prds.getAs[Array[Byte]](2), excl_alt_prdcts)
                if (_bv_count_one_bits(lv_target_prdcts) > 0) {
                    alt_prdcts = Array
                    .concat(
                        alt_prdcts,
                        Array.fill(1)(
                        Row(alt_prds.getAs[String](0),
                            alt_prds.getAs[String](1),
                            lv_alt_prdcts,
                            alt_prds.getAs[String](3),
                            alt_prds.getAs[String](4),
                            alt_prds.getAs[String](5)
                        )
                        )
                    )
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
            in.getAs[String](7),
            target_prdcts,
            alt_prdcts,
            in.getAs[String](10),
            in.getAs[String](11),
            in.getAs[String](12),
            in.getAs[String](13)
            )
        )
        }
        outputRows.toArray
    },
    ArrayType(
        StructType(
        List(
            StructField("tal_id",             StringType, true),
            StructField("tal_name",           StringType, true),
            StructField("tal_assoc_name",     StringType, true),
            StructField("clinical_indn_desc", StringType, true),
            StructField("tal_desc",           StringType, true),
            StructField("tal_assoc_desc",     StringType, true),
            StructField("priority",           StringType, true),
            StructField("tal_assoc_type_cd",  StringType, true),
            StructField(
            "target_prdcts",
            ArrayType(StructType(
                        List(StructField("udl_nm",   StringType, true),
                                StructField("udl_desc", StringType, true),
                                StructField("products", BinaryType, true)
                        )
                        ),
                        true
            ),
            true
            ),
            StructField(
            "alt_prdcts",
            ArrayType(
                StructType(
                List(
                    StructField("udl_nm",            StringType, true),
                    StructField("udl_desc",          StringType, true),
                    StructField("products",          BinaryType, true),
                    StructField("constituent_group", StringType, true),
                    StructField("constituent_reqd",  StringType, true),
                    StructField("constituent_rank",  StringType, true)
                )
                ),
                true
            ),
            true
            ),
            StructField("shared_qual",       StringType, true),
            StructField("override_tac_name", StringType, true),
            StructField("override_tar_name", StringType, true),
            StructField("newline",           StringType, true)
        )
        )
    )
    )
    
    val origColumns = in0.columns.map(col)
    val out0 = in0
    .groupBy("tal_id")
    .agg(
        collect_list(
        struct(
            origColumns: _*
        )
        ).alias("inputRows")
    )
    .select(explode(process_udf(col("inputRows"))).alias("output"))
    .select(col("output.*"))
    out0
  }

}
