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

object Agg_same_group_products_UnionAll {

  def apply(
    context: Context,
    in6:     DataFrame,
    in5:     DataFrame,
    in4:     DataFrame,
    in3:     DataFrame,
    in2:     DataFrame
  ): DataFrame =
    List(in6, in5, in4, in3, in2).flatMap(Option(_)).reduce(_.unionAll(_))

}
