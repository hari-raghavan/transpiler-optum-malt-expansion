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

object Sort_ST_Group_ST_Numbers {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.orderBy(col("step_therapy_group_name").asc,
               col("step_therapy_step_number").asc
    )

}
