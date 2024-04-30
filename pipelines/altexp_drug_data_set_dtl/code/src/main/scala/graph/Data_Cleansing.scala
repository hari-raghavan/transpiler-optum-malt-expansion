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

object Data_Cleansing {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      coalesce(trim(col("rxclaim_env_name")), col("rxclaim_env_name"))
        .as("rxclaim_env_name"),
      coalesce(trim(col("carrier")), col("carrier")).as("carrier"),
      coalesce(trim(col("account")), col("account")).as("account"),
      coalesce(trim(col("group")),   col("group")).as("group"),
      col("run_eff_dt"),
      col("drug_data_set_dtl_id")
        .cast(DecimalType(10, 0))
        .as("drug_data_set_dtl_id"),
      col("drug_data_set_id").cast(DecimalType(10, 0)).as("drug_data_set_id"),
      coalesce(trim(col("ndc11")),                 col("ndc11")).as("ndc11"),
      coalesce(trim(col("gpi14")),                 col("gpi14")).as("gpi14"),
      coalesce(trim(col("status_cd")),             col("status_cd")).as("status_cd"),
      coalesce(trim(col("eff_dt")),
               first_defined(adjustCenturyDateInCyyFormat(lpad(col("eff_dt"),
                                                               7,
                                                               "0"
                                                          ).cast(StringType),
                                                          lit("1900"),
                                                          lit("CyyMMdd")
                             ),
                             lit("INVALID")
               )
      ).as("eff_dt"),
      coalesce(
        trim(col("term_dt")),
        first_defined(adjustCenturyDateInCyyFormat(
                        lpad(col("term_dt"), 7, "0").cast(StringType),
                        lit("1900"),
                        lit("CyyMMdd")
                      ),
                      lit("INVALID")
        )
      ).as("term_dt"),
      coalesce(
        trim(col("inactive_dt")),
        when(
          col("inactive_dt") === lit("0"),
          first_defined(adjustCenturyDateInCyyFormat(lpad(col("term_dt"),
                                                          7,
                                                          "0"
                                                     ).cast(StringType),
                                                     lit("1900"),
                                                     lit("CyyMMdd")
                        ),
                        lit("INVALID")
          )
        ).otherwise(
          first_defined(adjustCenturyDateInCyyFormat(
                          lpad(col("inactive_dt"), 7, "0").cast(StringType),
                          lit("1900"),
                          lit("CyyMMdd")
                        ),
                        lit("INVALID")
          )
        )
      ).as("inactive_dt"),
      coalesce(trim(col("msc")),             col("msc")).as("msc"),
      coalesce(trim(col("drug_name")),       col("drug_name")).as("drug_name"),
      coalesce(trim(col("prod_short_desc")), col("prod_short_desc"))
        .as("prod_short_desc"),
      coalesce(trim(col("rx_otc")),         col("rx_otc")).as("rx_otc"),
      coalesce(trim(col("rx_otc_cd")),      col("rx_otc_cd")).as("rx_otc_cd"),
      coalesce(trim(col("desi")),           col("desi")).as("desi"),
      coalesce(trim(col("roa_cd")),         col("roa_cd")).as("roa_cd"),
      coalesce(trim(col("dosage_form_cd")), col("dosage_form_cd"))
        .as("dosage_form_cd"),
      col("prod_strength").cast(DecimalType(14, 5)).as("prod_strength"),
      coalesce(trim(col("repack_cd")),          col("repack_cd")).as("repack_cd"),
      coalesce(trim(col("gpi14_desc")),         col("gpi14_desc")).as("gpi14_desc"),
      coalesce(trim(col("gpi8_desc")),          col("gpi8_desc")).as("gpi8_desc"),
      coalesce(col("newline"),                  lit("""
""")).as("newline")
    )

}
