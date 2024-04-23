package graph.TAL_Container_Assoc

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.TAL_Container_Assoc.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Combine_Association_details_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("tal_id").cast(DecimalType(10, 0)).as("tal_id"),
      col("tal_name"),
      col("tal_assoc_name"),
      lookup("TAL_ASSOC_XWALK", col("tal_assoc_name"))
        .getField("clinical_indn_desc")
        .as("clinical_indn_desc"),
      col("tal_desc"),
      lookup("TAL_ASSOC_XWALK", col("tal_assoc_name"))
        .getField("tal_assoc_desc")
        .as("tal_assoc_desc"),
      col("priority").cast(StringType).as("priority"),
      lookup("TAL_ASSOC_XWALK", col("tal_assoc_name"))
        .getField("tal_assoc_type_cd")
        .as("tal_assoc_type_cd"),
      lookup("TAL_ASSOC_XWALK", col("tal_assoc_name"))
        .getField("target_udl_info")
        .as("target_prdcts"),
      lookup("TAL_ASSOC_XWALK", col("tal_assoc_name"))
        .getField("alt_udl_info")
        .as("alt_prdcts"),
      lookup("TAL_ASSOC_XWALK", col("tal_assoc_name"))
        .getField("shared_qual")
        .as("shared_qual"),
      lookup("TAL_ASSOC_XWALK", col("tal_assoc_name"))
        .getField("override_tac_name")
        .as("override_tac_name"),
      lookup("TAL_ASSOC_XWALK", col("tal_assoc_name"))
        .getField("override_tar_name")
        .as("override_tar_name"),
      coalesce(col("newline"), lit("""
""")).as("newline")
    )

}
