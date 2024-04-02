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

object Select_valid_drugs {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(
      to_date(col("term_dt"), "yyyyMMdd") >= to_date(
        lit(context.config.BUSINESS_DATE),
        "yyyyMMdd"
      )
    )

}
