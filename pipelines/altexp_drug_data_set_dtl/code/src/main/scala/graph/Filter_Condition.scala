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

object Filter_Condition {

  def apply(context: Context, in: DataFrame): (DataFrame, DataFrame) = {
    val Config = context.config
    (in.filter(
       is_not_null(to_timestamp(col("eff_dt"), "yyyyMMdd"))
         .and(
           is_not_null(to_timestamp(col("term_dt"), "yyyyMMdd")).and(
             col("run_eff_dt").isNull
               .and(col("term_dt") >= lit(Config.BUSINESS_DATE))
               .or(
                 is_not_null(col("run_eff_dt")).and(
                   col("term_dt") >= concat(
                     string_substring(
                       date_format(
                         to_date(
                           date_add_months(
                             date_format(
                               to_date(lit(Config.BUSINESS_DATE), "yyyyMMdd"),
                               "yyyy-MM-dd"
                             ),
                             1
                           ),
                           "yyyy-MM-dd"
                         ),
                         "yyyyMMdd"
                       ),
                       lit(1),
                       lit(6)
                     ),
                     lit("01")
                   )
                 )
               )
           )
         )
         .and(
           col("inactive_dt").isNull
             .or(is_not_null(to_timestamp(col("inactive_dt"), "yyyyMMdd")))
         )
     ),
     in.filter(
       !is_not_null(to_timestamp(col("eff_dt"), "yyyyMMdd"))
         .and(
           is_not_null(to_timestamp(col("term_dt"), "yyyyMMdd")).and(
             col("run_eff_dt").isNull
               .and(col("term_dt") >= lit(Config.BUSINESS_DATE))
               .or(
                 is_not_null(col("run_eff_dt")).and(
                   col("term_dt") >= concat(
                     string_substring(
                       date_format(
                         to_date(
                           date_add_months(
                             date_format(
                               to_date(lit(Config.BUSINESS_DATE), "yyyyMMdd"),
                               "yyyy-MM-dd"
                             ),
                             1
                           ),
                           "yyyy-MM-dd"
                         ),
                         "yyyyMMdd"
                       ),
                       lit(1),
                       lit(6)
                     ),
                     lit("01")
                   )
                 )
               )
           )
         )
         .and(
           col("inactive_dt").isNull
             .or(is_not_null(to_timestamp(col("inactive_dt"), "yyyyMMdd")))
         )
     )
    )
  }

}
