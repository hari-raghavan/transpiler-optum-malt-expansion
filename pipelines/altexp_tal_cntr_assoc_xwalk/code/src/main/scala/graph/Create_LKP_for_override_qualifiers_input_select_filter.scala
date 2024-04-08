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

object Create_LKP_for_override_qualifiers_input_select_filter {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(col("shared_qual") =!= lit("N/A"))

}
