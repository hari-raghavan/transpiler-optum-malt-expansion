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

object Join_With_Input_Surrogate_Key_File__Join {

  def apply(context: Context, left: DataFrame, right: DataFrame): DataFrame =
    left
      .as("left")
      .join(right.as("right"), lit(true), "inner")
      .select(
        col("right.file_load_cntl_id")
          .cast(DecimalType(16, 0))
          .as("file_load_cntl_id"),
        col("right.err_desc").as("err_desc"),
        col("right.rec_crt_ts").as("rec_crt_ts"),
        col("right.rec_crt_user_id").as("rec_crt_user_id"),
        col("right.rec_last_upd_ts").as("rec_last_upd_ts"),
        col("right.rec_last_upd_user_id").as("rec_last_upd_user_id")
      )

}
