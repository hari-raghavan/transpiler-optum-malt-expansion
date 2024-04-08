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

object Populate_TAR_Target_Alternatives_Crosswalk_input_select_filter {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val Config = context.config
    in.filter(
      (to_date(col("eff_dt"), "yyyyMMdd") <= to_date(lit(Config.BUSINESS_DATE),
                                                     "yyyyMMdd"
      )).and(
        to_date(col("term_dt"), "yyyyMMdd") >= to_date(
          lit(Config.BUSINESS_DATE),
          "yyyyMMdd"
        )
      )
    )
  }

}
