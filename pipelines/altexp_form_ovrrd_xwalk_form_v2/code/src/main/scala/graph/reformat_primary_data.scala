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

object reformat_primary_data {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("primary_data.formulary_name").as("formulary_name"),
      col("primary_data.carrier").as("carrier"),
      col("primary_data.account").as("account"),
      col("primary_data.group").as("group"),
      col("primary_data.customer_name").as("customer_name"),
      col("primary_data.run_eff_dt").as("run_eff_dt"),
      col("primary_data.cag_priority").as("cag_priority")
    )

}
