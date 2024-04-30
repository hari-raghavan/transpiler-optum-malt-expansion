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

object Remove_Duplicate_on_Key_ss {

  def apply(context: Context, in: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.Window
    in.withColumn(
        "row_number",
        row_number().over(
          Window
            .partitionBy(
              "file_load_cntl_id",
              "component_ids",
              "as_of_date",
              "rxclaim_env_name",
              "carrier",
              "account",
              "group",
              "component_type_cd",
              "file_name_w",
              "published_ind",
              "alt_run_id",
              "report_file_name",
              "newline"
            )
            .orderBy(lit(1))
        )
      )
      .filter(col("row_number") === lit(1))
      .drop("row_number")
  }

}
