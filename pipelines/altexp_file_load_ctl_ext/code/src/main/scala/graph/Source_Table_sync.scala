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
          "file_load_cntl_id",
          "component_ids",
          "as_of_date",
          "rxclaim_env_name",
          "carrier",
          "account",
          "group",
          "component_type_cd",
          "file_name_w",
          "published_ind",
          "alt_run_id",
          "report_file_name",
          "newline"
        )
      )
    out
  }

}
