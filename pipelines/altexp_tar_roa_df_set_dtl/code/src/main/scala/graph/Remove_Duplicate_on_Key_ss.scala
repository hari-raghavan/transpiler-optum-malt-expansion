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
            .partitionBy("tar_roa_df_set_dtl_id",
                         "tar_roa_df_set_id",
                         "target_roa_cd",
                         "target_dosage_form_cd",
                         "alt_roa_cd",
                         "alt_dosage_form_cd",
                         "priority",
                         "newline"
            )
            .orderBy(col("tar_roa_df_set_id").asc,
                     col("tar_roa_df_set_dtl_id").asc,
                     col("target_roa_cd").desc,
                     col("target_dosage_form_cd").desc,
                     col("priority").asc
            )
        )
      )
      .filter(col("row_number") === lit(1))
      .drop("row_number")
  }

}
