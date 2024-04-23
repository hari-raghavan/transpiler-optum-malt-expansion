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

object Map_Cag_and_Formulary_Override_URLs_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("carrier"),
      col("account"),
      col("group"),
      col("cag_override_data_path"),
      col("output_profile_id"),
      col("op_dtls"),
      col("err_msgs"),
      coalesce(col("newline"), lit("""
""")).as("newline")
    )

}
