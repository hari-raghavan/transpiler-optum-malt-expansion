package graph.TAL_Assoc_Crosswalk

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.TAL_Assoc_Crosswalk.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Process_TAL_Association_Rows_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("tal_assoc_dtl_id").cast(DecimalType(10, 0)).as("tal_assoc_dtl_id"),
      col("tal_assoc_id").cast(DecimalType(10,     0)).as("tal_assoc_id"),
      col("tal_assoc_name"),
      col("clinical_indn_desc"),
      col("tal_assoc_desc"),
      col("tal_assoc_type_cd").cast(StringType).as("tal_assoc_type_cd"),
      col("target_udl_info"),
      col("alt_udl_info"),
      col("shared_qual"),
      col("override_tac_name"),
      col("override_tar_name"),
      coalesce(col("newline"), lit("""
""")).as("newline")
    )

}
