package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Separate_data_for_Different_Operators_RowDistributor {

  def apply(context: Context, in: DataFrame): (
    DataFrame,
    DataFrame,
    DataFrame,
    DataFrame,
    DataFrame,
    DataFrame,
    DataFrame
  ) =
    (in.filter(
       array_contains(
         array(lit("DOSAGE_FORM"), lit("ROA"), lit("MSC"), lit("DESI_CD")),
         col("qualifier_cd")
       )
     ),
     in.filter(
       array_contains(
         array(lit("DOSAGE_FORM"), lit("ROA"), lit("MSC"), lit("DESI_CD")),
         col("qualifier_cd")
       ).or(
         !array_contains(
           array(lit("DOSAGE_FORM"), lit("ROA"), lit("MSC"), lit("DESI_CD")),
           col("qualifier_cd")
         ).and(col("qualifier_cd") =!= lit("DAYS_UNTIL_DRUG_STATUS_INACTIVE"))
       )
     ),
     in.filter(
       (col("qualifier_cd") === lit("DAYS_UNTIL_DRUG_STATUS_INACTIVE"))
         .and(col("compare_value") === lit("180"))
         .or(
           (col("compare_value") === lit("120")).or(
             (col("compare_value") === lit("90")).or(
               (col("compare_value") === lit("60")).or(
                 (col("qualifier_cd") === lit(
                   "DAYS_UNTIL_DRUG_STATUS_INACTIVE"
                 )).and(col("compare_value") =!= lit("180"))
                   .and(col("compare_value") =!= lit("120"))
                   .and(col("compare_value") =!= lit("90"))
                   .and(col("compare_value") =!= lit("60"))
               )
             )
           )
         )
     ),
     in.filter(
       (col("qualifier_cd") === lit("DAYS_UNTIL_DRUG_STATUS_INACTIVE"))
         .and(col("compare_value") === lit("180"))
         .or(
           (col("compare_value") === lit("120")).or(
             (col("compare_value") === lit("90"))
               .or(col("compare_value") === lit("60"))
           )
         )
     ),
     in.filter(
       (col("qualifier_cd") === lit("DAYS_UNTIL_DRUG_STATUS_INACTIVE"))
         .and(col("compare_value") === lit("180"))
         .or(
           (col("compare_value") === lit("120"))
             .or(col("compare_value") === lit("90"))
         )
     ),
     in.filter(
       (col("qualifier_cd") === lit("DAYS_UNTIL_DRUG_STATUS_INACTIVE"))
         .and(col("compare_value") === lit("180"))
         .or(col("compare_value") === lit("120"))
     ),
     in.filter(
       (col("qualifier_cd") === lit("DAYS_UNTIL_DRUG_STATUS_INACTIVE"))
         .and(col("compare_value") === lit("180"))
     )
    )

}
