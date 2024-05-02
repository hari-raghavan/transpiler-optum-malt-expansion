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

object list_load_ready_files {
  def apply(context: Context): DataFrame = {
    val spark = context.spark
    val Config = context.config
    lazy val arrayDF = generateDataFrameWithSequenceColumn(1, Config.LIST_OF_ALT_FILES.length, "index", spark)
    lazy val out     = arrayDF.select(element_at(typedLit(Config.LIST_OF_ALT_FILES), col("index")).as("line"))
    out
  }

}
