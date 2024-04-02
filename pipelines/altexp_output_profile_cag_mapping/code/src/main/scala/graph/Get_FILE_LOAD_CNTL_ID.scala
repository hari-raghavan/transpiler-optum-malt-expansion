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

object Get_FILE_LOAD_CNTL_ID {

  def apply(context: Context, left: DataFrame, right: DataFrame): DataFrame = {
    val Config = context.config
    var res    = left.as("left")
    res = res.join(right.as("right"),
                   col("left.job_id") === col("right.job_id"),
                   "left_outer"
    )
    res.select(
      col("left.alt_run_id").as("alt_run_id"),
      col("right.file_load_cntl_id").cast(DecimalType(16, 0)).as("job_run_id"),
      date_format(
        to_timestamp(
          substring(
            datetime_add(
              datetime_from_unixtime(
                file_information(lit(Config.OUTPUT_PROFILE_FILE))
                  .getField("created")
              ).cast(StringType),
              lit(0).cast(LongType),
              lit(-6).cast(LongType),
              lit(0).cast(LongType),
              lit(0).cast(LongType)
            ),
            1,
            14
          ),
          "yyyyMMddHHmmss"
        ),
        "yyyy-MM-dd HH:mm:ss"
      ).as("rec_crt_ts"),
      lit(Config.DB_ALTERNATE_USER).as("rec_crt_user_id"),
      lit("""
""").cast(StringType).as("newline")
    )
  }

}
