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

object Form_ref_file_lkp_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup(
      "form_ref_file_lkp",
      in,
      context.spark,
      List("formulary_name",
           "carrier",
           "account",
           "group",
           "customer_name",
           "is_future_snap"
      ),
      "formulary_name",
      "carrier",
      "account",
      "group",
      "customer_name",
      "is_future_snap",
      "data_path",
      "run_eff_dt",
      "newline"
    )

}
