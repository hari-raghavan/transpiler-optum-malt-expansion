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

object TAR_Dtl_lookup {
  def apply(context: Context, in0: DataFrame): Unit = {
    val spark = context.spark
    val Config = context.config
    createExtendedLookup(
          "TAR_Dtl",
          in0,
          spark,
          List(LookupCondition("tar_name", "==", "in1"), LookupCondition("eff_dt", "<=", "in2"), LookupCondition("term_dt", ">", "in2")),
          List("in1", "in2"),
          "tar_id",
          "tar_dtl_id",
          "tar_name",
          "sort_ind",
          "filter_ind",
          "priority",
          "tar_dtl_type_cd",
          "tar_roa_df_set_id",
          "target_rule",
          "alt_rule",
          "rebate_elig_cd",
          "eff_dt",
          "term_dt",
          "newline"
    )
  }

}
