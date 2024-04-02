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

object TSD_Dtl_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup(
      "TSD_Dtl",
      in,
      context.spark,
      List("tsd_name"),
      "tsd_dtl_id",
      "tsd_id",
      "tsd_name",
      "tsd_cd",
      "formulary_tier",
      "formulary_status",
      "priority",
      "eff_dt",
      "term_dt",
      "newline"
    )

}
