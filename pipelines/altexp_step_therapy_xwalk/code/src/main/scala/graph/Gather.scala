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

  def apply(context: Context, in4: DataFrame, in3: DataFrame): DataFrame =
    List(in4, in3).flatMap(Option(_)).reduce(_.unionAll(_))

}
