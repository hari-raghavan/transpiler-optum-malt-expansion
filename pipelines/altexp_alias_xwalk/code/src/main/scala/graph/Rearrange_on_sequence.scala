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

object Rearrange_on_sequence {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.orderBy(col("alias_name").asc,
               col("qual_priority").asc,
               col("rank").asc,
               col("qual_id_value").asc,
               col("search_txt").asc
    )

}
