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

object Select_Valid_Associations {

  def apply(context: Context, left: DataFrame, right: DataFrame): DataFrame =
    left
      .as("left")
      .join(right.as("right"),
            col("left.tal_assoc_name") === col("right.tal_assoc_name"),
            "inner"
      )
      .select(
        col("left.tal_id").cast(DecimalType(10, 0)).as("tal_id"),
        col("left.tal_name").as("tal_name"),
        col("right.tal_assoc_id").cast(DecimalType(10, 0)).as("tal_assoc_id"),
        col("right.tal_assoc_name").as("tal_assoc_name"),
        col("left.tal_desc").as("tal_desc"),
        col("right.tal_assoc_type_cd").cast(StringType).as("tal_assoc_type_cd"),
        col("right.target_udl_name").as("target_udl_name"),
        col("right.alt_udl_name").as("alt_udl_name"),
        col("right.alt_rank").cast(StringType).as("alt_rank"),
        col("right.constituent_rank").cast(StringType).as("constituent_rank"),
        col("right.constituent_group").as("constituent_group"),
        col("right.constituent_reqd").as("constituent_reqd"),
        col("right.shared_qual").as("shared_qual"),
        col("right.override_tac_name").as("override_tac_name"),
        col("right.override_tar_name").as("override_tar_name"),
        col("left.priority").cast(StringType).as("priority"),
        coalesce(col("right.newline").cast(StringType), lit("""
""")).as("newline")
      )

}
