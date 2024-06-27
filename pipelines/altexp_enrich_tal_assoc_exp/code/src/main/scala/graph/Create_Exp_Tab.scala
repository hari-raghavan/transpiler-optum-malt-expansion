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
              col("tal_assoc_name"),
              lit("\t"),
              when(col("tal_assoc_type_cd").cast(StringType) === lit(3),    lit("Exclusion_Association"))
                .when(col("tal_assoc_type_cd").cast(StringType) === lit(2), lit("Inclusion_Association"))
                .otherwise(lit("Standard_Association")),
              lit("\t"),
              coalesce(col("override_tac_name"), lit("")),
              lit("\t"),
              coalesce(col("override_tar_name"), lit("")),
              lit("\t"),
              col("shared_qual"),
              lit("\t"),
              col("udl_id"),
              lit("\t"),
              col("udl_desc"),
              lit("\t"),
              when(col("Target_Alternative") === lit("TARGET"), lit("N/A")).otherwise(col("alt_rank").cast(StringType)),
              lit("\t"),
              coalesce(col("constituent_group"), lit("N/A")),
              lit("\t"),
              coalesce(col("constituent_reqd"), lit("N/A")),
              lit("\t"),
              coalesce(col("constituent_rank").cast(StringType), lit("N/A")),
              lit("\t"),
              col("Target_Alternative"),
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
              ).cast(StringType),
              lit("\t"),
              col("clinical_indn_desc")
            )
          ).as("line")
        ),
        lengthRelatedGlobalExpressions = Map(),
        normalizeRelatedGlobalExpressions = Map(),
        tempWindowExpr = Map()
      )
    
      val simpleSelect_in_DF = normalize_out_DF.select((lit("TALASSOC_EXPANSION")).as("sheet"), (col("line")).as("line"))
    
      val out = simpleSelect_in_DF
    out
  }

}
