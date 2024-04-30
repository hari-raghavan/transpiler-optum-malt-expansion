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
    val df_Join_with_DB_jdbc0 = Join_with_DB_jdbc0(context)
    val df_Join_with_DB_jdbcdb_predicate_pushdown =
      Join_with_DB_jdbcdb_predicate_pushdown(context, df_Join_with_DB_jdbc0, in)
    val df_Join_with_DB_jdbc_drop_duplicate = Join_with_DB_jdbc_drop_duplicate(
      context,
      df_Join_with_DB_jdbcdb_predicate_pushdown
    )
    val df_Join_with_DB =
      Join_with_DB(context, in, df_Join_with_DB_jdbc_drop_duplicate)
    val df_Join_with_DB_Log = Join_with_DB_Log(
      context,
      df_Join_with_DB_jdbc_drop_duplicate,
      df_Join_with_DB,
      in
    )
    Load_MFS_log_file0(context, df_Join_with_DB_Log)
    df_Join_with_DB
  }

}
