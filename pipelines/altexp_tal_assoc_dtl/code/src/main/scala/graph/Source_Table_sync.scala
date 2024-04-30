package graph

import io.prophecy.libs._
import config.Context
import udfs.UDFs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Source_Table_sync {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
      val out = in.syncDataFrameColumnsWithSchema(columnNames =
        List(
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
      )
    out
  }

}
