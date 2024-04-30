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

object Data_Cleansing {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      coalesce(trim(col("gpi14")), col("gpi14")).as("gpi14"),
      col("rank").cast(StringType).as("rank"),
      col("ratio").cast(DecimalType(5, 3)).as("ratio"),
      col("run_eff_dt")
    )

}
