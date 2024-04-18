package graph.Association_Processing

import io.prophecy.libs._
import graph.Association_Processing.config.Context
import udfs.UDFs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Normalize_Clinical_Indication {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
      val normalize_out_DF = in.normalize(
        lengthExpression = None,
        finishedExpression = None,
        finishedCondition = Some(lit(false)),
        alias = "index",
        colsToSelect = List(
          coalesce(
            (element_at(
              lookup_row("LKP_CLIN_INDCN", col("tal_assoc_name")),
              col("index") + lit(1)
            ).getField("rank").cast(StringType)),
            lit("")
          ).as("rank"),
          coalesce(
            (element_at(
              lookup_row("LKP_CLIN_INDCN", col("tal_assoc_name")),
              col("index") + lit(1)
            )
              .getField("clinical_indn_desc")),
            lit(0)
          ).as("clinical_indn_desc")
        ),
        lengthRelatedGlobalExpressions = Map(),
        tempWindowExpr = Map()
      )
    
    
      val simpleSelect_in_DF = normalize_out_DF.select(
        (col("formulary_name")).as("formulary_name"),
        (col("target_ndc")).as("target_ndc"),
        (col("tal_assoc_name")).as("tal_assoc_name"),
        (col("rank")).as("rank"),
        (lit(Config.RUN_TS)).as("rec_crt_ts"),
        (lit(Config.USER_ID)).as("rec_crt_user_id"),
        (col("clinical_indn_desc")).as("clinical_indn_desc")
      )
    
      val out = simpleSelect_in_DF
    out
  }

}
