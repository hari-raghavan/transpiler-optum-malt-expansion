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

object Populate_Audit_Fields {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val Config = context.config
    in.select(
      lit(Config.DB_ALTERNATE_USER).as("user_id"),
      lit("S").as("file_load_type_cd"),
      lit(9).as("component_type_cd"),
      lit(1).as("file_load_status_cd"),
      lit(Config.RUN_TS).as("file_load_start_ts"),
      lit(Config.RUN_TS).as("rec_crt_ts"),
      lit(Config.DB_ALTERNATE_USER).as("rec_crt_user_id"),
      lit("Y").as("published_ind"),
      col("job_id").cast(DecimalType(10, 0)).as("alt_run_id"),
      col("newline")
    )
  }

}
