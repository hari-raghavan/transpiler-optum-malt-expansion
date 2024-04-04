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

object Reference_File_Creation {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("carrier"),
      col("account"),
      col("group"),
      when(!col("run_eff_dt").isNull, lit(1)).as("is_future_snap"),
      col("data_path"),
      coalesce(rpad(lit("""
"""), 1, " "), lit("""
""")).as("newline")
    )

}
