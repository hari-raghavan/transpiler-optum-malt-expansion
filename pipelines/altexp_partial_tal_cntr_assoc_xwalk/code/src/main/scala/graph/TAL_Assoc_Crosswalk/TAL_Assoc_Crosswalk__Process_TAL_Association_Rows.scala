package graph.TAL_Assoc_Crosswalk

import io.prophecy.libs._
import graph.TAL_Assoc_Crosswalk.config.Context
import udfs.UDFs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object TAL_Assoc_Crosswalk__Process_TAL_Association_Rows {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import _root_.io.prophecy.abinitio.ScalaFunctions._
    val process_udf = udf(
        { (inputRows: Seq[Row], clinical_indn_desc: Array[String]) ⇒
        var target_prdcts_info  = Array[Row]()
        var all_target_prdcts   = Array[Byte]()
        var all_alt_prdcts      = Array[Byte]()
        var alt_prdcts_info     = Array[Row]()
        var target_udl_name     = Array[String]()
        var alternate_udl_name  = Array[String]()
        var clinc_indi_info     = Array[Row]()
        var _tal_assoc_name     = Array[String]()
        var _clinical_indn_desc = Array[String]()
    
        inputRows.foreach { row ⇒
            if (
            !_isnull(row.getAs[String]("target_udl_name")) && !target_udl_name
                .contains(row.getAs[String]("target_udl_name"))
            ) {
            var trgt_prdcts_lkp = row.getAs[Row]("trgt_prdcts_lkp")
            if (!_isnull(trgt_prdcts_lkp.getAs[String]("udl_nm"))) {
                if (_bv_count_one_bits(trgt_prdcts_lkp.getAs[Array[Byte]]("products")) > 0) {
                target_prdcts_info = Array.concat(target_prdcts_info, Array.fill(1)(trgt_prdcts_lkp))
                }
            }
            target_udl_name = Array.concat(target_udl_name, Array.fill(1)(row.getAs[String]("target_udl_name")))
            }
    
            if (
            !_isnull(row.getAs[String]("alt_udl_name")) && !alternate_udl_name
                .contains(row.getAs[String]("alt_udl_name"))
            ) {
            var alt_prdcts_lkp = row.getAs[Row]("alt_prdcts_lkp")
            if (!_isnull(alt_prdcts_lkp.getAs[String]("udl_nm"))) {
                if (_bv_count_one_bits(alt_prdcts_lkp.getAs[Array[Byte]]("products")) > 0) {
                alt_prdcts_info = Array.concat(alt_prdcts_info, Array.fill(1)(alt_prdcts_lkp))
                }
            }
            alternate_udl_name = Array.concat(alternate_udl_name, Array.fill(1)(row.getAs[String]("alt_udl_name")))
            }
        }
    
        var lastRow = inputRows.toArray.last
    
        if (!_tal_assoc_name.contains(lastRow.getAs[String]("tal_assoc_name"))) {
            _clinical_indn_desc = clinical_indn_desc
            clinc_indi_info =
            Array.concat(clinc_indi_info,
                            Array.fill(1)(Row(lastRow.getAs[String]("tal_assoc_name"), _clinical_indn_desc))
            )
        } else {
            var idx = _tal_assoc_name.indexOf(lastRow.getAs[String]("tal_assoc_name"))
            if (idx < _clinical_indn_desc.length)
            _clinical_indn_desc = clinc_indi_info(idx).getAs[Array[String]](1)
        }
        if (lastRow.getAs[String]("tal_assoc_type_cd").toInt == 3) {
            target_prdcts_info =
            if (target_prdcts_info.isEmpty && alt_prdcts_info.nonEmpty) Array(Row("", "", _bv_all_zeros()))
            else target_prdcts_info
            alt_prdcts_info =
            if (target_prdcts_info.nonEmpty && alt_prdcts_info.isEmpty) Array(Row("", "", _bv_all_zeros(), "", "", ""))
            else alt_prdcts_info
        }
    
        Row(
            lastRow.getAs[java.math.BigDecimal]("tal_assoc_dtl_id"),
            lastRow.getAs[java.math.BigDecimal]("tal_assoc_id"),
            lastRow.getAs[String]("tal_assoc_name"),
            _clinical_indn_desc.mkString("\\|"),
            lastRow.getAs[String]("tal_assoc_desc"),
            lastRow.getAs[String]("tal_assoc_type_cd"),
            target_prdcts_info,
            alt_prdcts_info,
            lastRow.getAs[String]("shared_qual"),
            lastRow.getAs[String]("override_tac_name"),
            lastRow.getAs[String]("override_tar_name"),
            lastRow.getAs[String]("newline")
        )
        },
        StructType(
        List(
            StructField("tal_assoc_dtl_id",   DecimalType(10, 0), true),
            StructField("tal_assoc_id",       DecimalType(10, 0), true),
            StructField("tal_assoc_name",     StringType,         true),
            StructField("clinical_indn_desc", StringType,         true),
            StructField("tal_assoc_desc",     StringType,         true),
            StructField("tal_assoc_type_cd",  StringType,         true),
            StructField(
            "target_udl_info",
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
            "alt_udl_info",
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
    
    val origColumns = in0.columns.map(col)
    val out0 = in0
        .groupBy("tal_assoc_id")
        .agg(
        collect_list(
            struct(
            (origColumns :+ lookup("Expanded_UDL", col("target_udl_name")).alias("trgt_prdcts_lkp")
                :+ lookup("Expanded_UDL", col("alt_udl_name")).alias("alt_prdcts_lkp"): _*) //15,16
            ).alias("inputRows")
        ),
        transform(lookup_row("LKP_CLIN_INDCN", last(col("tal_assoc_name"))),
                    xx ⇒ coalesce(xx.getField("clinical_indn_desc"), lit("N/A"))
        ).alias("clinical_indn_desc")
        )
        .select((process_udf(col("inputRows"), col("clinical_indn_desc"))).alias("output"))
        .select(col("output.*"))
    out0
  }

}
