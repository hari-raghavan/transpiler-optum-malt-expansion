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

object Populate_TAC_Target_Alternatives_Crosswalk_input_select_filter {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val Config = context.config
    in.filter(
      (col("eff_dt") <= lit(Config.BUSINESS_DATE))
        .and(col("term_dt") >= lit(Config.BUSINESS_DATE))
    )
  }

}
