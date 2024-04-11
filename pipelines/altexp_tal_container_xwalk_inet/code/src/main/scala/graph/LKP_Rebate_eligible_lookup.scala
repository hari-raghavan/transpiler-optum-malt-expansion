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

object LKP_Rebate_eligible_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup("LKP_Rebate_eligible",
                 in,
                 context.spark,
                 List("dl_bit"),
                 "dl_bit",
                 "rebate_elig_cd"
    )

}
