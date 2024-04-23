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

object LKP_Step_Grp_NM_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup("LKP_Step_Grp_NM",
                 in,
                 context.spark,
                 List("ndc11", "step_therapy_step_number"),
                 "ndc11",
                 "step_therapy_step_number",
                 "step_therapy_group_names"
    )

}
