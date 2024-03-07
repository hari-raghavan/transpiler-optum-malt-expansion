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

object Sort_GPI12s {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.orderBy(col("gpi12").asc, to_date(col("run_eff_dt"), "yyyyMMdd").asc)

}
