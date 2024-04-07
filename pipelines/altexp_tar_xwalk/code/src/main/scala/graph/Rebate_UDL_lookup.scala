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

object Rebate_UDL_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup(
      "Rebate_UDL",
      in,
      context.spark,
      List("output_profile_id", "rebate_elig_cd"),
      "output_profile_rebate_dtl_id",
      "udl_name",
      "output_profile_id",
      "rebate_elig_cd",
      "newline"
    )

}
