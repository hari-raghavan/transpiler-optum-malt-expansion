package graph.Join_With_DB

import io.prophecy.libs._
import graph.Join_With_DB.config.Context
import udfs.UDFs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Join_with_DB_Log {
  def apply(context: Context, in3: DataFrame, in: DataFrame, in1: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
      val out = in.generateLogOutput(componentName = "Join_With_DB__Join_with_DB",
                                     inputRowCount = in1.count() + in3.count(),
                                     sparkSession = spark
      )
    out
  }

}
