package graph.Association_Processing

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.Association_Processing.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Rank_Alts_RowDistributor {

  def apply(context: Context, in: DataFrame): (DataFrame, DataFrame) =
    (in.filter(size(col("ta_prdct_dtls_wo_alt")).cast(BooleanType)),
     in.filter(
       size(col("ta_prdct_dtls_wo_alt"))
         .cast(BooleanType)
         .or(!size(col("ta_prdct_dtls_wo_alt")).cast(BooleanType))
     )
    )

}
