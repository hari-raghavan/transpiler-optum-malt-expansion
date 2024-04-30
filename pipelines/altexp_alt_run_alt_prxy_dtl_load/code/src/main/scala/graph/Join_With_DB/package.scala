package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import graph.Join_With_DB.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Join_With_DB {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val df_Join_with_DB = Join_with_DB(context, in)
    Load_MFS_log_file_7(context, df_Join_with_DB)
    Unused_Data_7(context,       df_Join_with_DB)
    df_Join_with_DB
  }

}
