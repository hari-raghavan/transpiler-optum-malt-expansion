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

object Process_TAL_Association_Rows_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("tal_id").cast(DecimalType(10, 0)).as("tal_id"),
      col("tal_name"),
      col("tal_assoc_name"),
      col("tar_udl_nm"),
      col("tal_desc"),
      col("priority").cast(StringType).as("priority"),
      col("tal_assoc_type_cd").cast(StringType).as("tal_assoc_type_cd"),
      col("target_prdcts"),
      col("alt_constituent_prdcts"),
      col("shared_qual"),
      col("override_tac_name"),
      col("override_tar_name"),
      col("constituent_grp_vec"),
      coalesce(col("newline"), lit("""
""")).as("newline")
    )

}
