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

object SORT {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.orderBy(col("formulary_name").asc,
               col("tal_assoc_name").asc,
               col("target_ndc").asc
    )

}
