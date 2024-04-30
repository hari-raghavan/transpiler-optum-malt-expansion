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

object Output_Surrogate_Key_File_Creation__Fetching_Required_Fields {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(col("alt_run_target_dtl_id")
                .cast(DecimalType(16, 0))
                .as("alt_run_target_dtl_id"),
              col("tal_assoc_name"),
              col("formulary_name"),
              col("target_ndc")
    )

}
