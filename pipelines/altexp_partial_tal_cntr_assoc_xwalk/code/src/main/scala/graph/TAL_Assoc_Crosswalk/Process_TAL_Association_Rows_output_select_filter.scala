package graph.TAL_Assoc_Crosswalk

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.TAL_Assoc_Crosswalk.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Process_TAL_Association_Rows_output_select_filter {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(
      when(
        array_contains(array(lit(1), lit(2)),
                       col("tal_assoc_type_cd").cast(IntegerType)
        ).and(!size(col("target_udl_info")).cast(BooleanType))
          .or(
            !size(col("target_udl_info"))
              .cast(BooleanType)
              .and(!size(col("alt_udl_info")).cast(BooleanType))
          ),
        lit(0).cast(BooleanType)
      ).otherwise(lit(1).cast(BooleanType))
    )

}
