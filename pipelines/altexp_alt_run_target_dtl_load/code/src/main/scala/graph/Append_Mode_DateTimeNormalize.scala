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
    in.withColumn("rec_crt_ts",
                  to_timestamp(col("rec_crt_ts"), "yyyy-MM-dd HH:mm:ss")
      )
      .withColumn("last_exp_dt", to_date(col("last_exp_dt"), "yyyyMMdd"))

}
