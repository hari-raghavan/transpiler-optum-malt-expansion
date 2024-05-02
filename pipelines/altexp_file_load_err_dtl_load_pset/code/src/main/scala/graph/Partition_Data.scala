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

object Partition_Data {
  def apply(context: Context, in: DataFrame): (DataFrame, DataFrame) = {
    val spark = context.spark
    val Config = context.config
    val tempDF_in      = in.zipWithIndex(0,         1, "tempId", spark)
    lazy val groupedDF = tempDF_in.select(col("*"), pmod(floor(col("tempId") / lit(1)), lit(2)).as("group_id"))
    lazy val out0_tmp  = groupedDF.filter(col("group_id") === lit(0))
    lazy val out0      = out0_tmp.drop("group_id")
    lazy val out1_tmp  = groupedDF.filter(col("group_id") === lit(1))
    lazy val out1      = out1_tmp.drop("group_id")
    (out1, out0)
  }

}
