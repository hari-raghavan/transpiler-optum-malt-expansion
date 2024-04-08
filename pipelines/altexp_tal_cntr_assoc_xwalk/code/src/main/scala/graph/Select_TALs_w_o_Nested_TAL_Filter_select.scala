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

object Select_TALs_w_o_Nested_TAL_Filter_select {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val Config = context.config
    in.filter(
      (col("tal_name") === lit(Config.TAL_NAME)).and(
        (to_date(col("eff_dt"), "yyyyMMdd") <= to_date(
          lit(Config.BUSINESS_DATE),
          "yyyyMMdd"
        )).and(
          to_date(col("term_dt"), "yyyyMMdd") >= to_date(
            lit(Config.BUSINESS_DATE),
            "yyyyMMdd"
          )
        )
      )
    )
  }

}
