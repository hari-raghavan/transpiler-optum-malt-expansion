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

object TAC_Dtl_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup("TAC_Dtl",
                 in,
                 context.spark,
                 List("tac_name"),
                 "tac_dtl_id",
                 "tac_id",
                 "tac_name",
                 "priority",
                 "target_rule",
                 "alt_rule",
                 "eff_dt",
                 "term_dt",
                 "newline"
    )

}
