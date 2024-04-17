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

object Drop_Targets_From_Tal_Assoc {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import _root_.io.prophecy.abinitio.ScalaFunctions._
    import scala.collection.mutable.ArrayBuffer
    
    val process_udf = udf(
      { (input: Seq[Row]) ⇒
        var st_tar_prdcts = Array[Byte]()
        var results       = ArrayBuffer[Row]()
    
        input.foreach { row ⇒
          var target_prdcts = ArrayBuffer[Array[Byte]]()
          var result        = row
          if (row.getAs[String](1) == "N/A - follows ST TAC")
            st_tar_prdcts = _bv_or(st_tar_prdcts, _bv_vector_or( 
              row.getSeq[Array[Byte]](7).toArray
            ))
          else {
            row.getSeq[Array[Byte]](7).toArray.foreach { in_tar_prd ⇒
              target_prdcts.append(_bv_difference(in_tar_prd, st_tar_prdcts))
            }
          }
          result = updateIndexInRow(result,
                                    7,
                                    if (row.getAs[String](1) == "N/A - follows ST TAC") 
                                      row.getSeq[Array[Byte]](7).toArray
                                    else target_prdcts
          )
          results.append(result)
        }
        results.toArray
      },
      ArrayType(
        StructType(
          StructField("tal_id",            DecimalType(10, 0),        true),
          StructField("tal_name",          StringType,        true),
          StructField("tal_assoc_name",    StringType,        true),
          StructField("tar_udl_nm",        StringType,        true),
          StructField("tal_desc",          StringType,        true),
          StructField("priority",          StringType,        true),
          StructField("tal_assoc_type_cd", StringType,        true),
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
      )
    )
    
    val out0 = in
      .select(struct(in.columns.map(col): _*).as("tmp"))
      .groupBy(lit(1))
      .agg(collect_list(col("tmp")).as("input"))
      .select(explode(process_udf(col("input")).as("output")))
      .select(col("output.*"))
    out0
  }

}
