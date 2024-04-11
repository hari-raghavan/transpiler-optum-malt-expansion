package graph.Association_Processing.Alternative_Rollup_Processing

import io.prophecy.libs._
import graph.Association_Processing.Alternative_Rollup_Processing.config.Context
import udfs.UDFs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object flatten {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    val out = in0.select("out.*")
    out0
  }

}
