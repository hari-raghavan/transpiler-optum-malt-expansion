package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Load_Serial_log_file_5 {

  def apply(context: Context, in: DataFrame): Unit =
    in.write
      .format("csv")
      .option("header", false)
      .option("sep",    "|")
      .mode("error")
      .save(context.config.LOG_FILE_NM)

}
