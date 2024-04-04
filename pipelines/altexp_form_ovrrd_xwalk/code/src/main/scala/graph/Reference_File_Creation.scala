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
      col("formulary_name"),
      col("carrier"),
      col("account"),
      col("group"),
      col("customer_name"),
      when(!col("run_eff_dt").isNull.and(
             col("run_eff_dt") === lit(context.config.FIRST_OF_NEXT_YEAR)
           ),
           lit(1)
      ).when(!col("run_eff_dt").isNull, lit(2)).as("is_future_snap"),
      col("data_path"),
      col("run_eff_dt"),
      coalesce(rpad(lit("""
"""), 1, " "), lit("""
""")).as("newline")
    )

}
