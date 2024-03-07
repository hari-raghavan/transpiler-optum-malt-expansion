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

object Curr_Assign_Rank_and_Calculate_Ratio {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
      val normalize_out_DF = in.normalize(
        lengthExpression = Some(size(array_distinct(array_sort(col("gpi14"))))),
        finishedExpression = None,
        finishedCondition = None,
        alias = "index",
        colsToSelect = List(
          (element_at(col("gpi14"), col("index") + lit(1))).as("gpi14"),
          ((col("index") + lit(1)).cast(StringType)).as("rank"),
          (when(col("no_of_rec") === lit(1), lit(1))
            .otherwise(lit(1) + (col("index") * (lit(1) / (col("no_of_rec") - lit(1)))))
            .cast(DecimalType(5, 3)))
            .as("ratio")
        ),
        lengthRelatedGlobalExpressions = Map("no_of_rec" -> size(array_distinct(array_sort(col("gpi14")))),
                                             "gpi14" -> array_distinct(array_sort(col("gpi14")))
        ),
        tempWindowExpr = Map()
      )
    
      val simpleSelect_in_DF = normalize_out_DF.select(
        (col("gpi12")).as("gpi12"),
        (col("gpi14")).as("gpi14"),
        (col("rank")).as("rank"),
        (col("ratio")).as("ratio"),
        (lit(Config.RUN_TS)).as("rec_crt_ts"),
        (lit(Config.DB_ALTERNATE_USER)).as("rec_crt_user_id"),
        (lit(Config.RUN_TS)).as("rec_last_upd_ts"),
        (lit(Config.DB_ALTERNATE_USER)).as("rec_last_upd_user_id"),
        (col("run_eff_dt")).as("run_eff_dt")
      )
    
      val out = simpleSelect_in_DF
    out
  }

}
