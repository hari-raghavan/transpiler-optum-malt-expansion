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

object Select_TALs_w_o_Nested_TAL_RowDistributor {

  def apply(context: Context, in: DataFrame): (DataFrame, DataFrame) =
    (in.filter(!col("tal_assoc_name").isNull),
     in.filter(col("tal_assoc_name").isNull)
    )

}
