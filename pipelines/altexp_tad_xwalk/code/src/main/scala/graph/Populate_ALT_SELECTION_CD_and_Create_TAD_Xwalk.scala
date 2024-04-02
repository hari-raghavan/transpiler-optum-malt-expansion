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

object Populate_ALT_SELECTION_CD_and_Create_TAD_Xwalk {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("target_gpi14"))
      .agg(
        alt_selection_cd_udf(collect_list(col("alt_selection_id")))
          .as("alt_selection_cd"),
        collect_list(col("alt_selection_id")).as("alt_selection_ids"),
        collect_list(struct(col("rank"), col("qty_adjust_factor")))
          .as("tad_alt_dtls"),
        last(coalesce(col("newline"), lit("""
"""))).as("newline")
      )

}
