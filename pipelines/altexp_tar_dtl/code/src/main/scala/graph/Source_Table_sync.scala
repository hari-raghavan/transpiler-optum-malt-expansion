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

object Source_Table_sync {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
      val out = in.syncDataFrameColumnsWithSchema(columnNames =
        List(
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
      )
    out
  }

}
