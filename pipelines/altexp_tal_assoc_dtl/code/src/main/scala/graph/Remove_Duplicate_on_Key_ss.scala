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
              "tal_assoc_dtl_id",
              "tal_assoc_id",
              "tal_assoc_name",
              "tal_assoc_desc",
              "tal_assoc_type_cd",
              "target_udl_name",
              "alt_udl_name",
              "alt_rank",
              "constituent_rank",
              "constituent_group",
              "constituent_reqd",
              "shared_qual",
              "override_tac_name",
              "override_tar_name",
              "newline"
            )
            .orderBy(col("tal_assoc_id").asc, col("tal_assoc_dtl_id").asc)
        )
      )
      .filter(col("row_number") === lit(1))
      .drop("row_number")
  }

}
