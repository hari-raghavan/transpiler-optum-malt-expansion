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
      col("alias_dtl_id").cast(DecimalType(10, 0)).as("alias_dtl_id"),
      col("alias_id").cast(DecimalType(10,     0)).as("alias_id"),
      col("alias_name"),
      coalesce(col("qual_priority").cast(StringType), lit(99))
        .as("qual_priority"),
      col("qual_id_type_cd"),
      col("qual_id_value"),
      col("search_txt"),
      col("replace_txt"),
      col("rank").cast(StringType).as("rank"),
      col("eff_dt"),
      col("term_dt"),
      coalesce(col("newline"), lit("""
""")).as("newline")
    )

}
