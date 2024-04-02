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

object Select_Records_With_Valid_Step_Number_Value {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(!col("step_therapy_step_number").isNull)

}
