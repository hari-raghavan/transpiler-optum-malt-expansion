package graph.Association_Processing.Alternative_Rollup_Processing

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.Association_Processing.Alternative_Rollup_Processing.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object FBE_Target_Product_with_without_Constituent_Data {

  def apply(context: Context, in: DataFrame): (DataFrame, DataFrame) =
    (in.filter(!size(col("constituent_grp_vec")).cast(BooleanType)),
     in.filter(size(col("constituent_grp_vec")).cast(BooleanType))
    )

}
