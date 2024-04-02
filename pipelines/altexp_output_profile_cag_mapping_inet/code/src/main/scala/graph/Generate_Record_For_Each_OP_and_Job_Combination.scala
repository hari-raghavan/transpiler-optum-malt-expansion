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

object Generate_Record_For_Each_OP_and_Job_Combination {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
      val normalize_out_DF = in.normalize(
        lengthExpression = Some(size(col("job_ids"))),
        finishedExpression = None,
        finishedCondition = None,
        alias = "index",
        colsToSelect = List((element_at(col("job_ids"), col("index") + lit(1))).as("job_id")),
        lengthRelatedGlobalExpressions = Map(),
        tempWindowExpr = Map()
      )
    
      val simpleSelect_in_DF = normalize_out_DF.select(
        (col("alt_run_id").cast(DecimalType(16, 0))).as("alt_run_id"),
        (col("output_profile_id")).as("output_profile_id"),
        (col("job_id")).as("job_id"),
        (col("err_msg")).as("err_msg")
      )
    
      val out = simpleSelect_in_DF
    out
  }

}
