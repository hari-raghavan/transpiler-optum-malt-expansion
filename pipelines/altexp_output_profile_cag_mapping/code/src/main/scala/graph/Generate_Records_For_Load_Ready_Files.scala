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
        lengthExpression = Some((size(col("qual_output_profile_ids")) + size(col("non_qual_output_profile_ids")))),
        finishedExpression = None,
        finishedCondition = None,
        alias = "index",
        colsToSelect = List(
          (when(col("index") < col("qual_len"), element_at(col("qual_output_profile_ids"), col("index") + lit(1)))
            .otherwise(
              element_at(col("non_qual_output_profile_ids"), (col("index") - col("qual_len")).cast(IntegerType) + lit(1))
                .getField("non_qual_op_id")
            )
            .cast(StringType))
            .as("output_profile_id"),
          (when(col("index") < col("qual_len"), element_at(col("op_dtls"), col("index") + lit(1)).getField("job_ids"))
            .otherwise(
              element_at(col("non_qual_output_profile_ids"), (col("index") - col("qual_len")).cast(IntegerType) + lit(1))
                .getField("job_ids")
            ))
            .as("job_ids"),
          (when(col("index") >= col("qual_len"),
                element_at(col("err_msgs"), (col("index") - col("qual_len")).cast(IntegerType) + lit(1))
          ).otherwise(lit(""))).as("err_msg")
        ),
        lengthRelatedGlobalExpressions = Map("qual_len" -> size(col("qual_output_profile_ids"))),
        tempWindowExpr = Map()
      )
    
      val simpleSelect_in_DF = normalize_out_DF.select((col("output_profile_id")).as("output_profile_id"),
                                                       (col("job_ids")).as("job_ids"),
                                                       (col("err_msg")).as("err_msg"),
                                                       (col("as_of_dt")).as("as_of_dt")
      )
    
      val out = simpleSelect_in_DF
    out
  }

}
