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

object TAL_Dtl_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup(
      "TAL_Dtl",
      in,
      context.spark,
      List("tal_name"),
      "tal_dtl_id",
      "tal_id",
      "tal_name",
      "tal_desc",
      "tal_dtl_type_cd",
      "nested_tal_name",
      "tal_assoc_name",
      "priority",
      "eff_dt",
      "term_dt",
      "newline"
    )

}
