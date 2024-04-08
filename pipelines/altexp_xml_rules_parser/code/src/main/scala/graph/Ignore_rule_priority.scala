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

object Ignore_rule_priority {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.orderBy(col("udl_id").asc,
               col("inclusion_cd").asc,
               col("rule_expression_id").asc,
               col("conjunction_cd").asc
    )

}
