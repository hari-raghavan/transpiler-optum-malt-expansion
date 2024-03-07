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

object Curr_Map_GPI14_under_GPI12 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("gpi12"))
      .agg(collect_list(col("gpi14")).as("gpi14"),
           max(col("run_eff_dt")).as("run_eff_dt")
      )

}
