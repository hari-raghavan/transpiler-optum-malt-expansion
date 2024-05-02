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

object Append_Mode_DateTimeNormalize {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.withColumn("as_of_date", to_date(col("as_of_date"), "yyyyMMdd"))

}
