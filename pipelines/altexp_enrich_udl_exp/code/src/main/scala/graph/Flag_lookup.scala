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

object Flag_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup("Flag", in, context.spark, List("flag"), "flag")

}
