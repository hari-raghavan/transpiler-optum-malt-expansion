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

object Populate_TAR_Target_Alternatives_Crosswalk_output_select_filter {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(size(col("contents")) > lit(0))

}
