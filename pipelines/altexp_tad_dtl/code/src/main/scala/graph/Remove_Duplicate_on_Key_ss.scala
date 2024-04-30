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
            .partitionBy("tad_id",
                         "target_gpi14",
                         "alt_grouping_gpi12",
                         "alt_selection_id",
                         "rank",
                         "qty_adjust_factor",
                         "newline"
            )
            .orderBy(col("target_gpi14").asc, col("rank").asc)
        )
      )
      .filter(col("row_number") === lit(1))
      .drop("row_number")
  }

}
