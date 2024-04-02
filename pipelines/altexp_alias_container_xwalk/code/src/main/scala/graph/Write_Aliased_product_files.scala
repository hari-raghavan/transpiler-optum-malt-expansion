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

object Write_Aliased_product_files {
  def apply(context: Context, in: DataFrame): Unit = {
    val spark = context.spark
    val Config = context.config
    val withFileDF = in.withColumn("fileName", concat(lit(Config.ALIAS_FILE_NAMES), col("alias_set_name"), lit(".dat")))
    withFileDF.breakAndWriteDataFrameForOutputFile(List("alias_set_name", "ndc11", "alias_label_nm", "newline"),
                                                   "fileName",
                                                   "csv",
                                                   Some("\\\\x01")
    )
  }

}
