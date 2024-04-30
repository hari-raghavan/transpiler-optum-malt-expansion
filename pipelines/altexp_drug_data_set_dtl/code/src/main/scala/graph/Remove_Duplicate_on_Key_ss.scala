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

object Remove_Duplicate_on_Key_ss {

  def apply(context: Context, in: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.Window
    in.withColumn(
        "row_number",
        row_number().over(
          Window
            .partitionBy(
              "rxclaim_env_name",
              "carrier",
              "account",
              "group",
              "run_eff_dt",
              "drug_data_set_dtl_id",
              "drug_data_set_id",
              "ndc11",
              "gpi14",
              "status_cd",
              "eff_dt",
              "term_dt",
              "inactive_dt",
              "msc",
              "drug_name",
              "prod_short_desc",
              "rx_otc",
              "rx_otc_cd",
              "desi",
              "roa_cd",
              "dosage_form_cd",
              "prod_strength",
              "repack_cd",
              "gpi14_desc",
              "gpi8_desc",
              "newline"
            )
            .orderBy(col("rxclaim_env_name").asc,
                     col("carrier").asc,
                     col("account").asc,
                     col("group").asc,
                     to_date(col("run_eff_dt"), "yyyyMMdd").asc,
                     col("ndc11").asc
            )
        )
      )
      .filter(col("row_number") === lit(1))
      .drop("row_number")
  }

}
