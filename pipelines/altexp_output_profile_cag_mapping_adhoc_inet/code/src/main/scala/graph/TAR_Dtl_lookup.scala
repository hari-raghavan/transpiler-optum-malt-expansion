package graph

import io.prophecy.libs._
import config.Context
import udfs.UDFs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object TAR_Dtl_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup(
      "TAR_Dtl",
      in,
      context.spark,
      List("tar_name"),
      "tar_id",
      "tar_dtl_id",
      "tar_name",
      "sort_ind",
      "filter_ind",
      "priority",
      "tar_dtl_type_cd",
      "tar_roa_df_set_id",
      "target_rule",
      "alt_rule",
      "rebate_elig_cd",
      "eff_dt",
      "term_dt",
      "newline"
    )

}
