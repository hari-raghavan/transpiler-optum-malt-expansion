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

object Create_ALT_RUN_Load_Ready_FileReformat_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("alt_run_id").cast(DecimalType(16, 0)).as("alt_run_id"),
      lit(4000).cast(DecimalType(16,         0)).as("job_run_id"),
      date_format(
        to_timestamp(
          substring(
            datetime_add(
              datetime_from_unixtime(
                file_information(lit(context.config.OUTPUT_PROFILE_FILE))
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
      lit("aialtexp").as("rec_crt_user_id"),
      coalesce(rpad(lit("""
"""), 1, " "), lit("""
""")).as("newline")
    )

}
