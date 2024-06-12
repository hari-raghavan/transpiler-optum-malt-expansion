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

object Create_Product_Crosswalk_For_Different_Levels_2 {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    val out0 = in0.withColumn("data_path", 
                                op_fl_nn_condition(lit(Config.AI_SERIAL_HOME), 
                                                    lit(Config.OUTPUT_FILE_PREFIX),
                                                    lit(Config.ENV_NM),
                                                    lit(Config.BUSINESS_DATE)))
    out0
  }

}
