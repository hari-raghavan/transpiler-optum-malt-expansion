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

object Get_FILE_LOAD_CNTL_ID_jdbcdb_predicate_pushdown {
  def apply(context: Context, right: DataFrame, left: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    lazy val out = right.collectDataFrameColumnsToApplyFilter(List("job_id"), left)
    out
  }

}
