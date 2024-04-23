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

object Generate_Records_For_Load_Ready_Files {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
      val normalize_out_DF = in.normalize(
        lengthExpression = Some(size(col("output_profile_id"))),
        finishedExpression = None,
        finishedCondition = None,
        alias = "index",
        colsToSelect = List(
          (when(col("index") < col("qual_len"), element_at(col("output_profile_id"), col("index") + lit(1)))
            .cast(StringType))
            .as("output_profile_id"),
          (when(col("index") >= col("qual_len"),
                element_at(col("err_msgs"), (col("index") - col("qual_len")).cast(IntegerType) + lit(1))
          ).otherwise(lit(""))).as("err_msg")
        ),
        lengthRelatedGlobalExpressions = Map("qual_len" -> size(col("output_profile_id"))),
        tempWindowExpr = Map()
      )
    
      val simpleSelect_in_DF =
        normalize_out_DF.select((col("output_profile_id")).as("output_profile_id"), (col("err_msg")).as("err_msg"))
    
      val out = simpleSelect_in_DF
    out
  }

}
