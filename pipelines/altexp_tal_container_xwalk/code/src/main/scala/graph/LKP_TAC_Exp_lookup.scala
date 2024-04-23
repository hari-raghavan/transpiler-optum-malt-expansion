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

object LKP_TAC_Exp_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup("LKP_TAC_Exp",
                 in,
                 context.spark,
                 List("tac_name"),
                 "tac_id",
                 "tac_name",
                 "tac_contents",
                 "eff_dt",
                 "term_dt",
                 "newline"
    )

}
