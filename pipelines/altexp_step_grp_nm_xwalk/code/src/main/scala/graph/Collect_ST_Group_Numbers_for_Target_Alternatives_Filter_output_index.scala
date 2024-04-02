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

object Collect_ST_Group_Numbers_for_Target_Alternatives_Filter_output_index {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(
      expr(
        "array_position(transform(target_rule_def, x -> struct(x['qualifier_cd'] AS qualifier_cd)), struct('ST_STEP_NUM' AS qualifier_cd))"
      ) - lit(1) > lit(-1)
    )

}
