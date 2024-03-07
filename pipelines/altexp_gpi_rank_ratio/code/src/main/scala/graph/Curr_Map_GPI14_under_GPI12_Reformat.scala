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

object Curr_Map_GPI14_under_GPI12_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(col("gpi12"), col("gpi14"), col("run_eff_dt"))

}
