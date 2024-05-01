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

object Create_Product_Lookup {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("ndc11"),
      col("gpi14"),
      col("status_cd"),
      col("inactive_dt"),
      col("msc"),
      col("drug_name"),
      col("rx_otc"),
      col("desi"),
      col("roa_cd"),
      col("dosage_form_cd"),
      col("prod_strength").cast(DecimalType(14, 5)).as("prod_strength"),
      col("repack_cd"),
      col("prod_short_desc"),
      col("gpi14_desc"),
      col("gpi8_desc"),
      coalesce(col("newline"), lit("""
""")).as("newline")
    )

}
