package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Collect_Step_Group_Names_and_Create_Crosswalk_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(col("ndc11"),
              col("step_therapy_step_number")
                .cast(StringType)
                .as("step_therapy_step_number"),
              col("step_therapy_group_names")
    )

}
