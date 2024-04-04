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

object Accumulate_Products_for_each_CAG_and_Prioritize_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("formulary_name"),
      col("carrier"),
      col("account"),
      col("group"),
      col("customer_name"),
      col("run_eff_dt"),
      col("cag_priority"),
      col("prdcts"),
      col("data_path")
    )

}
