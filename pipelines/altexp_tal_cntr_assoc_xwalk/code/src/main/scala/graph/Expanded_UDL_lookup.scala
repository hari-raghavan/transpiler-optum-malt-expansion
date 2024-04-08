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

object Expanded_UDL_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup("Expanded_UDL",
                 in,
                 context.spark,
                 List("udl_nm"),
                 "udl_id",
                 "udl_nm",
                 "udl_desc",
                 "products",
                 "eff_dt",
                 "term_dt",
                 "contents",
                 "newline"
    )

}
