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

object Normalize_CAG_Data {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
      val normalize_out_DF = in.normalize(
        lengthExpression = Some(size(col("prdcts"))),
        finishedExpression = None,
        finishedCondition = None,
        alias = "index",
        colsToSelect = List(
          col("carrier"),
          col("account"),
          col("group"),
          col("cag_priority"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("ndc11")).as("ndc11"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("gpi14")).as("gpi14"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("status_cd")).as("status_cd"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("inactive_dt")).as("inactive_dt"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("eff_dt")).as("eff_dt"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("term_dt")).as("term_dt"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("msc")).as("msc"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("drug_name")).as("drug_name"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("rx_otc")).as("rx_otc"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("desi")).as("desi"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("roa_cd")).as("roa_cd"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("dosage_form_cd")).as("dosage_form_cd"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("prod_strength")).as("prod_strength"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("prod_short_desc")).as("prod_short_desc"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("roa_cd")).as("roa_cd"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("gpi14_desc")).as("gpi14_desc"),
          (element_at(col("prdcts"), col("index") + lit(1)).getField("gpi8_desc")).as("gpi8_desc"),
          col("run_eff_dt"),
          col("data_path")
        ),
        lengthRelatedGlobalExpressions = Map(),
        tempWindowExpr = Map()
      )
    
      val simpleSelect_in_DF = normalize_out_DF.select(
        col("carrier").as("carrier"),
        col("account").as("account"),
        col("group").as("group"),
        col("run_eff_dt").as("run_eff_dt"),
        col("cag_priority").as("cag_priority"),
        col("ndc11").as("ndc11"),
        col("gpi14").as("gpi14"),
        col("status_cd").as("status_cd"),
        col("inactive_dt").as("inactive_dt"),
        col("eff_dt").as("eff_dt"),
        col("term_dt").as("term_dt"),
        col("msc").as("msc"),
        col("drug_name").as("drug_name"),
        col("rx_otc").as("rx_otc"),
        col("desi").as("desi"),
        col("roa_cd").as("roa_cd"),
        col("dosage_form_cd").as("dosage_form_cd"),
        col("prod_strength").as("prod_strength"),
        col("prod_short_desc").as("prod_short_desc"),
        col("roa_cd").as("roa_cd"),
        col("gpi14_desc").as("gpi14_desc"),
        col("gpi8_desc").as("gpi8_desc"),
        lit("\n").as("newline"),
        col("run_eff_dt").as("run_eff_dt"),
        col("data_path").as("data_path")
      )
    
      val out = simpleSelect_in_DF
    out0
  }

}
