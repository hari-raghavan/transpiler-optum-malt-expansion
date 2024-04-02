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

object Generate_Record_for_Qualifier_and_their_Values {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
      val normalizeDF = in.normalize(
        lengthExpression = Some(lit(6)),
        finishedExpression = None,
        finishedCondition = None,
        alias = "index",
        colsToSelect = List(
          (when(col("index") === lit(0),             lit("TIER"))
            .when(col("index") === lit(1),           lit("STATUS"))
            .when(col("index") === lit(2),           lit("PA"))
            .when(col("index") === lit(3),           lit("SPECIALTY"))
            .when(col("index") === lit(4),           lit("ST"))
            .otherwise(when(col("index") === lit(5), lit("ST_STEP_NUM"))))
            .as("qualifier_cd"),
          (when(col("index") === lit(0),             col("formulary_tier"))
            .when(col("index") === lit(1),           col("formulary_status"))
            .when(col("index") === lit(2),           col("pa_reqd_ind"))
            .when(col("index") === lit(3),           col("specialty_ind"))
            .when(col("index") === lit(4),           col("step_therapy_ind"))
            .otherwise(when(col("index") === lit(5), col("step_therapy_step_number").cast(StringType))))
            .as("compare_value")
        ),
        lengthRelatedGlobalExpressions = Map("dl_bit" -> lookup("LKP_Product_Lookup", col("ndc11")).getField("dl_bit")),
        tempWindowExpr = Map()
      )
    
      val simpleSelect_in_DF = normalizeDF.select((col("dl_bit").cast(IntegerType)).as("dl_bit"),
                                                  (col("qualifier_cd")).as("qualifier_cd"),
                                                  (col("compare_value")).as("compare_value")
      )
    
      val normalize_out_DF = simpleSelect_in_DF.filter(
        when(
          (col("qualifier_cd") === lit("ST_STEP_NUM")).and(
            (isnull(col("compare_value"))
              .or(col("compare_value").cast(StringType) <= lit(0)))
              .or(col("compare_value").cast(StringType) > lit(20))
          ),
          lit(0)
        ).otherwise(lit(1))
      )
    
      val out = normalize_out_DF
    out
  }

}
