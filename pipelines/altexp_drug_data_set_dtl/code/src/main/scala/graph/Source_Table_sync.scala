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

object Source_Table_sync {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
      val out = in.syncDataFrameColumnsWithSchema(columnNames =
        List(
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
      )
    out
  }

}
