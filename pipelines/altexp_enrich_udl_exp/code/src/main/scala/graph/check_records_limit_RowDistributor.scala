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

object check_records_limit_RowDistributor {

  def apply(context: Context, in: DataFrame): (DataFrame, DataFrame) =
    (in.filter(lookup_match("Flag",     lit(0)).cast(BooleanType)),
     in.filter(not(lookup_match("Flag", lit(0)).cast(BooleanType)))
    )

}
