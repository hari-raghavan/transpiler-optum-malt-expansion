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

object Step_Therapy_DTL_File_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup("Step_Therapy_DTL_File",
                 in,
                 context.spark,
                 List("step_therapy_step_number"),
                 "step_therapy_group_name",
                 "step_therapy_step_number",
                 "products"
    )

}
