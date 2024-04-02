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

object Rebate_Elig_Cd_Crosswalk_input_select_filter {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(
      col("output_profile_id")
        .cast(DecimalType(10, 0)) === lit(context.config.OUTPUT_PROF_ID)
    )

}
