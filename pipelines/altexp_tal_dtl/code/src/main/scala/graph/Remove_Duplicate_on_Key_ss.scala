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
            .partitionBy("tal_dtl_id",
                         "tal_id",
                         "tal_name",
                         "tal_desc",
                         "tal_dtl_type_cd",
                         "nested_tal_name",
                         "tal_assoc_name",
                         "priority",
                         "eff_dt",
                         "term_dt",
                         "newline"
            )
            .orderBy(col("tal_id").asc, col("priority").asc)
        )
      )
      .filter(col("row_number") === lit(1))
      .drop("row_number")
  }

}
