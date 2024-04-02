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

object Collect_ST_Group_Numbers_for_Target_Alternatives_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("tac_id").cast(DecimalType(10, 0)).as("tac_id"),
      col("tac_name"),
      col("priority").cast(StringType).as("priority"),
      coalesce(element_at(col("target_rule_def"), 1)
                 .getField("compare_value")
                 .cast(StringType),
               lit("0")
      ).as("_target_st_grp_num"),
      coalesce(
        filter(transform(col("alt_rule_def"),
                         _alt_st_num => _alt_st_num.getField("compare_value")
               ),
               xx => !xx.isNull
        ),
        array()
      ).as("_alt_st_grp_nums")
    )

}
