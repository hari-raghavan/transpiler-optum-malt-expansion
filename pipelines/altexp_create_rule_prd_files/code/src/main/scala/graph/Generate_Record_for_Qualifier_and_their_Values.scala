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
        lengthExpression = Some(lit(17)),
        finishedExpression = None,
        finishedCondition = None,
        alias = "index",
        colsToSelect = List(
          (when(col("index") === lit(0),              lit("NDC11"))
            .when(col("index") === lit(1),            lit("NDC9"))
            .when(col("index") === lit(2),            lit("GPI14"))
            .when(col("index") === lit(3),            lit("GPI12"))
            .when(col("index") === lit(4),            lit("GPI10"))
            .when(col("index") === lit(5),            lit("DOSAGE_FORM"))
            .when(col("index") === lit(6),            lit("ROA"))
            .when(col("index") === lit(7),            lit("DRUG_NAME"))
            .when(col("index") === lit(8),            lit("GPI8"))
            .when(col("index") === lit(9),            lit("GPI6"))
            .when(col("index") === lit(10),           lit("GPI4"))
            .when(col("index") === lit(11),           lit("MSC"))
            .when(col("index") === lit(12),           lit("RXOTC"))
            .when(col("index") === lit(13),           lit("DAYS_UNTIL_DRUG_STATUS_INACTIVE"))
            .when(col("index") === lit(14),           lit("STATUS_CD"))
            .when(col("index") === lit(15),           lit("REPACKAGER"))
            .otherwise(when(col("index") === lit(16), lit("DESI_CD"))))
            .as("qualifier_cd"),
          (rtrim(
            when(col("index") === lit(0),    col("ndc11"))
              .when(col("index") === lit(1), string_substring(col("ndc11"), lit(1), lit(9)))
              .when(col("index") === lit(2), col("gpi14"))
              .when(col("index") === lit(3), string_substring(col("gpi14"), lit(1), lit(12)))
              .when(col("index") === lit(4), string_substring(col("gpi14"), lit(1), lit(10)))
              .when(col("index") === lit(5), col("dosage_form_cd"))
              .when(col("index") === lit(6), col("roa_cd"))
              .when(col("index") === lit(7),
                    regexp_replace(col("drug_name"), lit("\\Q[[[^0-9]s[^0-9][^0-9]ce[^0-9]]]+\\E"), lit(""))
              )
              .when(col("index") === lit(8),  string_substring(col("gpi14"), lit(1), lit(8)))
              .when(col("index") === lit(9),  string_substring(col("gpi14"), lit(1), lit(6)))
              .when(col("index") === lit(10), string_substring(col("gpi14"), lit(1), lit(4)))
              .when(col("index") === lit(11), col("msc"))
              .when(col("index") === lit(12), col("rx_otc"))
              .when(
                col("index") === lit(13),
                when(col("days_until_inactive") > lit(180),    lit("180"))
                  .when(col("days_until_inactive") > lit(120), lit("120"))
                  .when(col("days_until_inactive") > lit(90),  lit("90"))
                  .when(col("days_until_inactive") > lit(60),  lit("60"))
                  .when(col("days_until_inactive") > lit(30),  lit("30"))
                  .otherwise(lit(""))
              )
              .when(col("index") === lit(14),           col("status_cd"))
              .when(col("index") === lit(15),           col("repack_cd"))
              .otherwise(when(col("index") === lit(16), col("desi")))
          )).as("compare_value")
        ),
        lengthRelatedGlobalExpressions =
          Map(
              "days_until_inactive" -> datediff(to_date(col("inactive_dt"),"yyyyMMdd"), to_date(lit(Config.BUSINESS_DATE),"yyyyMMdd"))
          ),
        tempWindowExpr = Map()
      )
    
      val simpleSelect_in_DF = normalizeDF.select((col("qualifier_cd")).as("qualifier_cd"),
                                                  (col("compare_value")).as("compare_value")
      )
    
      val simpleSelect_in_DF_1 = simpleSelect_in_DF.zipWithIndex(0, 1, "dl_bit", spark)
    
      val normalize_out_DF = simpleSelect_in_DF_1.filter(isNotNullAndNotBlank(col("compare_value")))
    
      val out = normalize_out_DF
    out
  }

}
