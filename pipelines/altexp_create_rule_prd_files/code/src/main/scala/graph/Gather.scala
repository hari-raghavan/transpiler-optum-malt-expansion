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

object Gather {

  def apply(
    context: Context,
    in6:     DataFrame,
    in5:     DataFrame,
    in4:     DataFrame
  ): DataFrame = List(in6, in5, in4).flatMap(Option(_)).reduce(_.unionAll(_))

}
