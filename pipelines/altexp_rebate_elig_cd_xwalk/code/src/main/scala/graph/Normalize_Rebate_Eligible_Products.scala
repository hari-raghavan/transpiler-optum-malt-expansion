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

object Normalize_Rebate_Eligible_Products {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
      val normalize_out_DF = in.normalize(
        lengthExpression = Some(size(col("dl_bit"))),
        finishedExpression = None,
        finishedCondition = None,
        alias = "index",
        colsToSelect = List((element_at(col("dl_bit"), col("index") + lit(1)).cast(IntegerType)).as("dl_bit")),
        lengthRelatedGlobalExpressions = Map(),
        tempWindowExpr = Map()
      )
    
      val simpleSelect_in_DF =
        normalize_out_DF.select((col("dl_bit")).as("dl_bit"), (col("rebate_elig_cd")).as("rebate_elig_cd"))
    
      val out = simpleSelect_in_DF
    out
  }

}
