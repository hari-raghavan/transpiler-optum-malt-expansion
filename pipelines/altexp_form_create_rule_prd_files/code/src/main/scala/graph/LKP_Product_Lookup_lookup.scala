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

object LKP_Product_Lookup_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup(
      "LKP_Product_Lookup",
      in,
      context.spark,
      List("ndc11"),
      "dl_bit",
      "ndc11",
      "gpi14",
      "status_cd",
      "inactive_dt",
      "msc",
      "drug_name",
      "rx_otc",
      "desi",
      "roa_cd",
      "dosage_form_cd",
      "prod_strength",
      "repack_cd",
      "prod_short_desc",
      "gpi14_desc",
      "gpi8_desc",
      "newline"
    )

}
