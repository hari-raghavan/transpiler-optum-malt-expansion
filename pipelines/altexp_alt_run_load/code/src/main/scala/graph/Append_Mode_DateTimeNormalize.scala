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

object Append_Mode_DateTimeNormalize {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.withColumn("run_start_ts",
                  to_timestamp(col("run_start_ts"), "yyyy-MM-dd HH:mm:ss")
      )
      .withColumn("run_complete_ts",
                  to_timestamp(col("run_complete_ts"), "yyyy-MM-dd HH:mm:ss")
      )
      .withColumn("rec_crt_ts",
                  to_timestamp(col("rec_crt_ts"), "yyyy-MM-dd HH:mm:ss")
      )
      .withColumn("rec_last_upd_ts",
                  to_timestamp(col("rec_last_upd_ts"), "yyyy-MM-dd HH:mm:ss")
      )
      .withColumn("run_eff_dt", to_date(col("run_eff_dt"), "yyyyMMdd"))
      .withColumn("as_of_dt",   to_date(col("as_of_dt"), "yyyyMMdd"))

}
