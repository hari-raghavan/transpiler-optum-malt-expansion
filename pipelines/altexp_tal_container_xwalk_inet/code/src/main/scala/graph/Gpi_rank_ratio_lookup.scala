package graph

import io.prophecy.libs._
import config.Context
import udfs.UDFs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Gpi_rank_ratio_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup("Gpi_rank_ratio",
                 in,
                 context.spark,
                 List("gpi14"),
                 "gpi14",
                 "rank",
                 "ratio",
                 "run_eff_dt"
    )

}
