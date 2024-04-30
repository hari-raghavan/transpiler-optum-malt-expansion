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

object Data_Cleansing {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("tal_assoc_id").cast(DecimalType(10, 0)).as("tal_assoc_id"),
      coalesce(trim(col("tal_assoc_name")),    col("tal_assoc_name"))
        .as("tal_assoc_name"),
      col("clinical_indn_id").cast(DecimalType(10, 0)).as("clinical_indn_id"),
      coalesce(trim(col("clinical_indn_name")),    col("clinical_indn_name"))
        .as("clinical_indn_name"),
      coalesce(trim(col("clinical_indn_desc")), col("clinical_indn_desc"))
        .as("clinical_indn_desc"),
      col("rank").cast(StringType).as("rank")
    )

}
