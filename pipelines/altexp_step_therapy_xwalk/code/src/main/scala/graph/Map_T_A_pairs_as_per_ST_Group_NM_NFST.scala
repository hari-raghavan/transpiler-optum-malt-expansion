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

object Map_T_A_pairs_as_per_ST_Group_NM_NFST {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import _root_.io.prophecy.abinitio.ScalaFunctions._
    import scala.collection.mutable.ArrayBuffer
    
    val inputFilter_in_DF = in.filter(
      (element_at(col("target_rule_def"), lit(1)).getField("qualifier_cd") === lit("ST_STEP_NUM"))
        .and(col("tac_name") === lit(Config.TAC_NM))
    )
    
    def process_udf(row: Row) = {
      val target_prdcts           = row.getAs[Seq[Row]]("target_prdcts")
      val st_therapy_dtl_data_lkp = row.getAs[Seq[Row]]("st_therapy_dtl_data_lkp")
      val alternate_prdcts        = ArrayBuffer[Row]()
      val final_target_prdcts     = ArrayBuffer[Array[Byte]]()
      val final_alternate_prdcts  = ArrayBuffer[Row]()
    
      val results = ArrayBuffer[Row]()
      if (target_prdcts.length > 0) {
        st_therapy_dtl_data_lkp.foreach { lkp_data ⇒
          val compare_value = lkp_data.getAs[String](0)
    
          lkp_data.getAs[Seq[Row]](1).toArray.foreach { st_therapy_dtl_data ⇒
            if (target_prdcts.indexWhere(t ⇒ t.getAs[String](1) == st_therapy_dtl_data.getAs[String](0)) != -1) {
              val vec_index = alternate_prdcts.indexWhere(t ⇒ t.getAs[String](1) == st_therapy_dtl_data.getAs[String](0))
              if (vec_index == -1) {
                alternate_prdcts.append(
                  Row(
                    compare_value,
                    st_therapy_dtl_data.getAs[String](0),
                    st_therapy_dtl_data.getAs[Array[Byte]](2)
                  )
                )
              } else {
                alternate_prdcts(vec_index) = Row(
                  compare_value,
                  st_therapy_dtl_data.getAs[String](0),
                  _bv_or(alternate_prdcts(vec_index).getAs[Array[Byte]](2), st_therapy_dtl_data.getAs[Array[Byte]](2))
                )
              }
            }
          }
        }
      }
    
      target_prdcts.toArray.foreach { tar_prd ⇒
        final_target_prdcts.append(tar_prd.getAs[Array[Byte]](2))
        val vec_index = alternate_prdcts.indexWhere(t ⇒ t.getAs[String](1) == tar_prd.getAs[String](1))
    
        if (vec_index == -1) {
          final_target_prdcts.append(
            Row(
              _bv_all_zeros(),
              "",
              "",
              "N/A"
            )
          )
        } else {
          final_target_prdcts.append(
            Row(
              alternate_prdcts[vec_index].getAs[Array[Byte]](2),
              "",
              "",
              "N/A"
            )
          )
        }
      }
    
      final_target_prdcts.zipWithIndex.foreach {
        case (r, idx) ⇒
          results.append(
            java.math.BigDecimal(0),
            "N/A - follows ST TAC",
            "N/A - follows ST TAC",
            null,
            "N/A - follows ST TAC",
            "0",
            Array(final_target_prdcts(idx)),
            Array(final_alternate_prdcts(idx)),
            "N/A",
            null,
            null,
            Array[String](),
            row.getAs[String](13)
          )
      }
      result.toArray
    }
    
    val df = inputFilter_in_DF
      .withColumn(
        "target_prdcts",
        transform(
          lookup_row("Step_Therapy_DTL_File", element_at(col("target_rule_def"), 0).getField("compare_value")),
          st_therapy_dtl_data ⇒
            struct(
              (element_at(col("target_rule_def"), 0).getField("compare_value")).as("number"),
              (st_therapy_dtl_data.getField("step_therapy_group_name")).as("group"),
              (st_therapy_dtl_data.getField("products")).as("products")
            )
        )
      )
      .withColumn(
        "st_therapy_dtl_data_lkp",
        transform(
          col("alt_rule_def"),
          alt_rules ⇒
            struct(
              alt_rules
                .getField("compare_value")
                .as("compare_value")
                .lookup_row(("Step_Therapy_DTL_File", alt_rules.getField("compare_value")).as("st_therapy_dtl_data"))
            )
        )
      )
    
    val schema = StructType(
      StructField("tal_id",            DecimalType(10, 0), true),
      StructField("tal_name",          StringType, true),
      StructField("tal_assoc_name",    StringType, true),
      StructField("tar_udl_nm",        StringType, true),
      StructField("tal_desc",          StringType, true),
      StructField("priority",          StringType, true),
      StructField("tal_assoc_type_cd", StringType, true),
      StructField("target_prdcts",     Array(BinaryType), true),
      StructField(
        "alt_constituent_prdcts",
        ArrayType(
          StructType(
            StructField("alt_prdcts",        BinaryType, true),
            StructField("constituent_group", StringType, true),
            StructField("constituent_reqd",  StringType, true),
            StructField("udl_nm",            StringType, true)
          ),
          true
        ),
        true
      ),
      StructField("shared_qual",         StringType,                  true),
      StructField("override_tac_name",   StringType,                  true),
      StructField("override_tar_name",   StringType,                  true),
      StructField("constituent_grp_vec", ArrayType(StringType, true), true),
      StructField("newline",             StringType,                  true)
    )
    
    val out = df.rdd.flatMap(process_udf).toDF(schema)
    out
  }

}
