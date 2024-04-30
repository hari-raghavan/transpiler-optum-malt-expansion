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
      col("tal_dtl_id").cast(DecimalType(10, 0)).as("tal_dtl_id"),
      col("tal_id").cast(DecimalType(10,     0)).as("tal_id"),
      coalesce(trim(col("tal_name")),        col("tal_name")).as("tal_name"),
      coalesce(trim(col("tal_desc")),        col("tal_desc")).as("tal_desc"),
      col("tal_dtl_type_cd").cast(StringType).as("tal_dtl_type_cd"),
      coalesce(trim(col("nested_tal_name")), col("nested_tal_name"))
        .as("nested_tal_name"),
      coalesce(trim(col("tal_assoc_name")), col("tal_assoc_name"))
        .as("tal_assoc_name"),
      col("priority").cast(StringType).as("priority"),
      col("eff_dt"),
      col("term_dt"),
      coalesce(col("newline"), lit("""
""")).as("newline")
    )

}
