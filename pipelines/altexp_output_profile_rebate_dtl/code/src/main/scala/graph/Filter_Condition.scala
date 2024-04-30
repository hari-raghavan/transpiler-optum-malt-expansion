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

object Filter_Condition {

  def apply(context: Context, in: DataFrame): (DataFrame, DataFrame) =
    (in.filter(
       lookup_match("LKP_Lookup",
                    col("output_profile_id").cast(DecimalType(10, 0))
       ).cast(BooleanType)
     ),
     in.filter(
       !lookup_match("LKP_Lookup",
                     col("output_profile_id").cast(DecimalType(10, 0))
       ).cast(BooleanType)
     )
    )

}
