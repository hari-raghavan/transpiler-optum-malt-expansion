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

object Populate_ALT_SELECTION_CD_and_Create_TAD_Xwalk_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(col("target_gpi14"),
              col("alt_selection_cd"),
              col("alt_selection_ids"),
              col("tad_alt_dtls"),
              coalesce(col("newline"), lit("""
""")).as("newline")
    )

}
