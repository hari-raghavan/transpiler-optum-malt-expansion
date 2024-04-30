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
          "alias_dtl_id",
          "alias_id",
          "alias_name",
          "qual_priority",
          "qual_id_type_cd",
          "qual_id_value",
          "search_txt",
          "replace_txt",
          "rank",
          "eff_dt",
          "term_dt",
          "newline"
        )
      )
    out
  }

}
