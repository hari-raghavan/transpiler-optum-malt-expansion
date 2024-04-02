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

object Create_ALT_RUN_ALT_RUN_JOB_DETAILS_Load_Ready_FileReformat_0 {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val Config = context.config
    in.select(
      coalesce(col("alt_run_id").cast(DecimalType(16, 0)), lit(-1))
        .as("alt_run_id"),
      col("output_profile_id").cast(DecimalType(10, 0)).as("output_profile_id"),
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
      ).as("run_start_ts"),
      lit(Config.RUN_TS).as("run_complete_ts"),
      when(is_blank(col("err_msg")), lit(1))
        .otherwise(lit(4))
        .cast(StringType)
        .as("alt_run_status_cd"),
      lit("Y").as("published_ind"),
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
      lit(Config.RUN_TS).as("rec_last_upd_ts"),
      lit(Config.DB_ALTERNATE_USER).as("rec_last_upd_user_id"),
      lit(Config.BUSINESS_DATE).as("run_eff_dt"),
      col("as_of_dt"),
      coalesce(rpad(lit("""
"""), 1, " "), lit("""
""")).as("newline")
    )
  }

}
