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

object Create_Exp_Tab {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
      val normalize_out_DF = in.normalize(
        lengthExpression = Some(size(bv_indices(col("products")))),
        finishedExpression = None,
        finishedCondition = None,
        alias = "index",
        colsToSelect = List(
          (
            concat(
              col("udl_nm"),
              lit("\t"),
              lookup("Products", element_at(bv_indices(col("products")), col("index") + lit(1)))
                .getField("ndc11")
                .cast(StringType),
              lit("\t"),
              lookup("Products", element_at(bv_indices(col("products")), col("index") + lit(1)))
                .getField("gpi14")
                .cast(StringType),
              lit("\t"),
              lookup("Products", element_at(bv_indices(col("products")), col("index") + lit(1))).getField("msc"),
              lit("\t"),
              lookup("Products", element_at(bv_indices(col("products")), col("index") + lit(1))).getField("drug_name"),
              lit("\t"),
              lookup("Products", element_at(bv_indices(col("products")), col("index") + lit(1)))
                .getField("prod_short_desc"),
              lit("\t"),
              lookup("Products", element_at(bv_indices(col("products")), col("index") + lit(1))).getField("gpi14_desc"),
              lit("\t"),
              when(
                array_contains(
                  array(lit(1.0e-5),
                        lit(2.0e-5),
                        lit(3.0e-5),
                        lit(4.0e-5),
                        lit(5.0e-5),
                        lit(6.0e-5),
                        lit(7.0e-5),
                        lit(8.0e-5),
                        lit(9.0e-5)
                  ),
                  lookup("Products", element_at(bv_indices(col("products")), col("index") + lit(1)))
                    .getField("prod_strength")
                ),
                concat(lit(""),
                       lookup("Products", element_at(bv_indices(col("products")), col("index") + lit(1)))
                         .getField("prod_strength")
                         .cast(DecimalType(37, 5)),
                       lit("")
                )
              ).otherwise(warning("%.15g not supported")),
              lit("\t"),
              lookup("Products", element_at(bv_indices(col("products")), col("index") + lit(1))).getField("roa_cd"),
              lit("\t"),
              lookup("Products", element_at(bv_indices(col("products")), col("index") + lit(1))).getField("dosage_form_cd"),
              lit("\t"),
              lookup("Products", element_at(bv_indices(col("products")), col("index") + lit(1))).getField("rx_otc"),
              lit("\t"),
              lookup("Products", element_at(bv_indices(col("products")), col("index") + lit(1))).getField("repack_cd"),
              lit("\t"),
              lookup("Products", element_at(bv_indices(col("products")), col("index") + lit(1))).getField("status_cd"),
              lit("\t"),
              date_format(to_date(lookup("Products", element_at(bv_indices(col("products")), col("index") + lit(1)))
                                    .getField("inactive_dt"),
                                  "yyyyMMdd"
                          ),
                          "MM/dd/yyyy"
              ).cast(StringType)
            )
          ).as("line")
        ),
        lengthRelatedGlobalExpressions = Map(),
        normalizeRelatedGlobalExpressions = Map(),
        tempWindowExpr = Map()
      )
    
      val simpleSelect_in_DF = normalize_out_DF.select((lit("UDL_EXPANSION")).as("sheet"), (col("line")).as("line"))
    
      val out = simpleSelect_in_DF
    out
  }

}
