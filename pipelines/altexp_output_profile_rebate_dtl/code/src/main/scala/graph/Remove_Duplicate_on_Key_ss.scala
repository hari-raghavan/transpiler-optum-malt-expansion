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
            .partitionBy("output_profile_rebate_dtl_id",
                         "udl_name",
                         "output_profile_id",
                         "rebate_elig_cd",
                         "newline"
            )
            .orderBy(col("output_profile_id").asc, col("rebate_elig_cd").asc)
        )
      )
      .filter(col("row_number") === lit(1))
      .drop("row_number")
  }

}
