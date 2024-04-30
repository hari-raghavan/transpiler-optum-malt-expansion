package graph.Join_With_DB

import io.prophecy.libs._
import graph.Join_With_DB.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Load_MFS_log_file_9 {

  def apply(context: Context, in: DataFrame): Unit =
    in.write
      .format("csv")
      .option("header", false)
      .option("sep",    "|")
      .mode("error")
      .save(context.config.LOG_FILE_NM_JWD)

}
