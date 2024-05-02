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
      .join(right.as("right"),
            col("left.newline") === col("right.newline"),
            "inner"
      )
      .select(
        col("right.file_load_cntl_id")
          .cast(DecimalType(16, 0))
          .as("file_load_cntl_id"),
        col("right.component_ids").as("component_ids"),
        col("right.as_of_date").as("as_of_date"),
        col("right.rxclaim_env_name").as("rxclaim_env_name"),
        col("right.carrier").as("carrier"),
        col("right.account").as("account"),
        col("right.group").as("group"),
        col("right.component_type_cd").cast(StringType).as("component_type_cd"),
        col("right.file_name_w").as("file_name_w"),
        col("right.published_ind").as("published_ind"),
        col("right.alt_run_id").cast(DecimalType(16, 0)).as("alt_run_id"),
        col("right.report_file_name").as("report_file_name"),
        coalesce(col("left.newline").cast(StringType), lit("""
""")).as("newline")
      )

}
