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

object Drug_Data_Set_Drug_Data_Set_DTL_sync {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
      val out = in.syncDataFrameColumnsWithSchema(columnNames =
        List("gpi12", "gpi14", "eff_dt", "term_dt", "inactive_dt", "run_eff_dt")
      )
    out
  }

}
