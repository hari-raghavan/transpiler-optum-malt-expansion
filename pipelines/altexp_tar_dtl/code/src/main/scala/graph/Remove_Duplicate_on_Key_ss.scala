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
              "tar_id",
              "tar_dtl_id",
              "tar_name",
              "sort_ind",
              "filter_ind",
              "priority",
              "tar_dtl_type_cd",
              "tar_roa_df_set_id",
              "target_rule",
              "alt_rule",
              "rebate_elig_cd",
              "eff_dt",
              "term_dt",
              "newline"
            )
            .orderBy(col("tar_id").asc,
                     to_date(col("eff_dt"), "yyyyMMdd").asc,
                     col("priority").asc
            )
        )
      )
      .filter(col("row_number") === lit(1))
      .drop("row_number")
  }

}
