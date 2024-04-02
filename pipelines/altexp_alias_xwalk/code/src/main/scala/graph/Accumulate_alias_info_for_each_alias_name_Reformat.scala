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

object Accumulate_alias_info_for_each_alias_name_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(col("alias_id").cast(DecimalType(10, 0)).as("alias_id"),
              col("alias_name"),
              col("alias_info")
    )

}
