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

object Rebate_Elig_Cd_Crosswalk {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("rebate_elig_cd"))
      .agg(
        bv_indices(expr("bit_or(coalesce(Expanded_UDL(udl_name).products, 0))"))
          .as("dl_bit")
      )

}
