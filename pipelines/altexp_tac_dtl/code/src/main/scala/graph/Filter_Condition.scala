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
       is_not_null(col("target_rule"))
         .and(is_not_null(col("alt_rule")))
         .and(
           lookup_match("LKP_Lookup1", col("tac_name"))
             .cast(BooleanType)
             .or(lookup_match("LKP_Lookup", col("tac_name")).cast(BooleanType))
         )
     ),
     in.filter(
       !is_not_null(col("target_rule"))
         .and(is_not_null(col("alt_rule")))
         .and(
           lookup_match("LKP_Lookup1", col("tac_name"))
             .cast(BooleanType)
             .or(lookup_match("LKP_Lookup", col("tac_name")).cast(BooleanType))
         )
     )
    )

}
