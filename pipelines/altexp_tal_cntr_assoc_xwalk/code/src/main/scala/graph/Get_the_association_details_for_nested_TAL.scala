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

object Get_the_association_details_for_nested_TAL {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import scala.collection.mutable.ArrayBuffer
    import java.math.BigDecimal
    
    def process_udf(row: Row) = {
      val results = ArrayBuffer[Row]()
      var _l1_factor = BigDecimal(5)
      var _l1_denominator = BigDecimal(100)
      var _l1_assocition_cnt = BigDecimal(2000)
      var _l2_factor = BigDecimal(1000)
      var tal_dtl_final = Array[Row]()
      var i = 0
      var j = 0
      var nested_tal_l1_dtl = row.getAs[Seq[Row]](11).toArray
      while (compareTo(i, nested_tal_l1_dtl.length) < 0) {
        if (!_isnull(nested_tal_l1_dtl(convertToInt(i)).getAs[String]("tal_assoc_name"))) {
              val nested_tal_l1_dtl_i = Row(
                BigDecimal(nested_tal_l1_dtl(convertToInt(i)).getAs[String]("tal_dtl_id")),
                BigDecimal(nested_tal_l1_dtl(convertToInt(i)).getAs[String]("tal_id")),
                nested_tal_l1_dtl(convertToInt(i)).getAs[String]("tal_name"),
                nested_tal_l1_dtl(convertToInt(i)).getAs[String]("tal_desc"),
                (nested_tal_l1_dtl(convertToInt(i)).getAs[String]("tal_dtl_type_cd")),
                nested_tal_l1_dtl(convertToInt(i)).getAs[String]("nested_tal_name"),
                nested_tal_l1_dtl(convertToInt(i)).getAs[String]("tal_assoc_name"),
                (BigDecimal(row.getAs[String]("priority")) + (BigDecimal(
                  nested_tal_l1_dtl(convertToInt(i)).getAs[String]("priority")
                ) / (_l1_denominator * (_l1_factor / _l1_assocition_cnt)))).toString,
                nested_tal_l1_dtl(convertToInt(i)).getAs[String]("eff_dt"),
                nested_tal_l1_dtl(convertToInt(i)).getAs[String]("term_dt"),
                nested_tal_l1_dtl(convertToInt(i)).getAs[String]("newline")
              )
              tal_dtl_final = Array.concat(tal_dtl_final, Array.fill(1)(nested_tal_l1_dtl_i))
        } else {
          j = 0
          val nested_tal_l1_dtl_arr = nested_tal_l1_dtl.getAs[Seq[Row]](i).toArray
          while (
            compareTo(j,
                  nested_tal_l1_dtl_arr.length    
            ) < 0
          ) {
            var nested_tal_l2_dtl = nested_tal_l1_dtl_arr(convertToInt(j))
            nested_tal_l2_dtl = updateIndexInRow(
              nested_tal_l2_dtl,
              7,
              ((BigDecimal(row.getAs[String]("priority")) + (BigDecimal(
                nested_tal_l1_dtl(convertToInt(i)).getAs[String]("priority")
              ) / (_l1_denominator * BigDecimal(_l1_factor / _l1_assocition_cnt)))) + (BigDecimal(
                nested_tal_l2_dtl.getAs[String]("priority")
              ) / (_l2_factor * BigDecimal(_l2_factor)))).toString
            )
            tal_dtl_final = Array.concat(tal_dtl_final, Array.fill(1)(nested_tal_l2_dtl))
            j = j + 1
          }
        }
        i = i + 1
      }
    
      tal_dtl_final.zipWithIndex.foreach {
        case (r, idx) ⇒
          results.append(
            row.getAs[String](0),
            row.getAs[String](1),
            row.getAs[String](2),
            row.getAs[String](3),
            row.getAs[String](4),
            tal_dtl_final(idx).getAs[String](2),
            if (row.getAs[String](6) == null)  tal_dtl_final(idx).getAs[String](6) else row.getAs[String](6),
            if (row.getAs[String](7) == null)  tal_dtl_final(idx).getAs[String](7) else row.getAs[String](7),
            row.getAs[String](8),
            row.getAs[String](9),
            row.getAs[String](10),
          )
      }
      result.toArray
    }
    
    def nested_tal_l1_dtl() = {
      lookup_row("TAL_Container_Dtls", col("nested_tal_name"))
    }
    
    def nested_tal_l2_dtl() = {
       transform(
          nested_tal_l1_dtl(),
          xx =>
            lookup_row("TAL_Container_Dtls", xx.getField("nested_tal_name"))
       )
    }
    
    val df = in
      .withColumn(
        "nested_tal_l1_dtl",
        nested_tal_l1_dtl(),
      ).withColumn(
        "nested_tal_l2_dtl",
        nested_tal_l2_dtl()
      )
    
    val schema = StructType(
                  List(
                    StructField("tal_dtl_id",      DecimalType(10, 0),  false),
                    StructField("tal_id",          DecimalType(10, 0),  false),
                    StructField("tal_name",        StringType,         false),
                    StructField("tal_desc",        StringType,         false),
                    StructField("tal_dtl_type_cd", StringType,         false),
                    StructField("nested_tal_name", StringType,         true),
                    StructField("tal_assoc_name",  StringType,         true),
                    StructField("priority",        StringType,         false),
                    StructField("eff_dt",          StringType,         false),
                    StructField("term_dt",         StringType,         false),
                    StructField("newline",         StringType,         true)
                  )
                )
    
    val out = in.rdd.flatMap(process_udf).toDF(schema)
    out
  }

}
