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

object Reformat_To_separate_NFST_and_STReformat_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("tac_id").cast(DecimalType(10, 0)).as("tac_id"),
      col("tac_name"),
      col("priority").cast(StringType).as("priority"),
      col("eff_dt"),
      col("term_dt"),
      col("target_rule_def"),
      col("alt_rule_def"),
      coalesce(col("newline"), lit("""
""")).as("newline")
    )

}
