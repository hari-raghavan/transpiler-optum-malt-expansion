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

object Reformat_To_separate_NFST_and_ST_RowDistributor {

  def apply(context: Context, in: DataFrame): (DataFrame, DataFrame) = {
    val Config = context.config
    (in.filter(lit(Config.TAC_CONDITION).cast(IntegerType) > lit(1)),
     in.filter(!(lit(Config.TAC_CONDITION).cast(IntegerType) > lit(1)))
    )
  }

}
