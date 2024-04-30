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

object Handle_Serial_Logs {

  def apply(context: Context, in2: DataFrame, in4: DataFrame): DataFrame =
    List(in2, in4).flatMap(Option(_)).reduce(_.unionAll(_))

}
