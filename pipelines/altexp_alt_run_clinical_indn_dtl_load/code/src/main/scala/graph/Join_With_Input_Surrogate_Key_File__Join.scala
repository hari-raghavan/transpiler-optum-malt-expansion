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
      .join(
        right.as("right"),
        (col("left.formulary_name") === col("right.formulary_name"))
          .and(col("left.tal_assoc_name") === col("right.tal_assoc_name"))
          .and(col("left.target_ndc") === col("right.target_ndc")),
        "inner"
      )
      .select(
        lit(28001).cast(DecimalType(16, 0)).as("alt_run_id"),
        coalesce(col("left.alt_run_target_dtl_id").cast(DecimalType(16, 0)),
                 lit(-1)
        ).as("alt_run_target_dtl_id"),
        col("right.formulary_name").as("formulary_name"),
        col("right.target_ndc").as("target_ndc"),
        col("right.tal_assoc_name").as("tal_assoc_name"),
        col("right.rank").cast(StringType).as("rank"),
        col("right.rec_crt_ts").as("rec_crt_ts"),
        col("right.rec_crt_user_id").as("rec_crt_user_id"),
        col("right.clinical_indn_desc").as("clinical_indn_desc"),
        coalesce(col("right.newline").cast(StringType), lit("""
""")).as("newline")
      )

}
