package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Gpi_rank_ratio {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("csv")
      .option("header", false)
      .option("sep",    "\u0001")
      .schema(
        StructType(
          Array(
            StructField("gpi14",      StringType, true),
            StructField("rank",       StringType, true),
            StructField("ratio",      StringType, true),
            StructField("run_eff_dt", StringType, true)
          )
        )
      )
      .load("/~null")

}
