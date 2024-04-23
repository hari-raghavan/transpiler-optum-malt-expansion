package graph.TAL_Container_Assoc.Expand_TAL_Data_At_UDL_level

import io.prophecy.libs._
import graph.TAL_Container_Assoc.Expand_TAL_Data_At_UDL_level.config.Context
import udfs.UDFs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Expand_Alternate_UDL_1 {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
      val normalize_out_DF = in.normalize(
        lengthExpression = Some(size(col("alt_prdcts"))),
        finishedExpression = None,
        finishedCondition = None,
        alias = "index",
        colsToSelect = List(
          (element_at(col("alt_prdcts"), col("index") + lit(1)).getField("udl_nm")).as("udl_id"),
          (element_at(col("alt_prdcts"), col("index") + lit(1)).getField("udl_desc")).as("udl_desc"),
          (element_at(col("alt_prdcts"), col("index") + lit(1)).getField("products")).as("products"),
          (col("index") + lit(1)).as("alt_rank"),
          (element_at(col("alt_prdcts"), col("index") + lit(1)).getField("constituent_group")).as("constituent_group"),
          (element_at(col("alt_prdcts"), col("index") + lit(1)).getField("constituent_reqd")).as("constituent_reqd"),
          (element_at(col("alt_prdcts"), col("index") + lit(1)).getField("constituent_rank")).as("constituent_rank")
        ),
        lengthRelatedGlobalExpressions = Map(),
        tempWindowExpr = Map()
      )
    
      val simpleSelect_in_DF = normalize_out_DF.select(
        (col("tal_name")).as("tal_id"),
        (col("tal_assoc_name")).as("tal_assoc_id"),
        (col("clinical_indn_desc")).as("clinical_indn_desc"),
        (col("tal_desc")).as("tal_desc"),
        (col("tal_assoc_desc")).as("tal_assoc_desc"),
        (col("tal_assoc_type_cd").cast(StringType)).as("tal_assoc_type_cd"),
        (col("priority").cast(StringType)).as("tal_assoc_rank"),
        (col("udl_id")).as("udl_id"),
        (col("udl_desc")).as("udl_desc"),
        (lit("ALTERNATIVE")).as("Target_Alternative"),
        (col("products")).as("products"),
        (col("alt_rank")).as("alt_rank"),
        (col("shared_qual")).as("shared_qual"),
        (col("override_tac_name")).as("override_tac_name"),
        (col("override_tar_name")).as("override_tar_name"),
        (col("constituent_group")).as("constituent_group"),
        (col("constituent_reqd")).as("constituent_reqd"),
        (col("constituent_rank")).as("constituent_rank"),
        (col("newline")).as("newline")
      )
    
      val out = simpleSelect_in_DF
    out
  }

}
