package graph.Association_Processing

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.Association_Processing.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Rank_AltsReformat_0 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("tal_id").cast(DecimalType(10, 0)).as("tal_id"),
      col("tal_name"),
      col("tal_assoc_name"),
      col("tar_udl_nm"),
      col("shared_qual"),
      col("ta_prdct_dtls_wo_alt"),
      coalesce(col("newline"), lit("""
""")).as("newline")
    )

}
