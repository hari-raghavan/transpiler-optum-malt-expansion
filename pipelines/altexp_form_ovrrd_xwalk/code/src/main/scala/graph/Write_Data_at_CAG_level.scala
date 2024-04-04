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

object Write_Data_at_CAG_level {
  def apply(context: Context, in: DataFrame): Unit = {
    val spark = context.spark
    val Config = context.config
    val withFileDF = in.withColumn("fileName", col("data_path"))
    withFileDF.breakAndWriteDataFrameForOutputFile(List(""), "fileName", "csv", Some("\\x01"))
  }

}
