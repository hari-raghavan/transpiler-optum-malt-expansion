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
        List("tar_roa_df_set_dtl_id",
             "tar_roa_df_set_id",
             "target_roa_cd",
             "target_dosage_form_cd",
             "alt_roa_cd",
             "alt_dosage_form_cd",
             "priority",
             "newline"
        )
      )
    out
  }

}
