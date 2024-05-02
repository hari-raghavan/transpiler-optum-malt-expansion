package graph.Join_With_DB

import io.prophecy.libs._
import graph.Join_With_DB.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Unused_Data_6 {

  def apply(context: Context, in: DataFrame): Unit =
    in.write
      .format("csv")
      .option("header", false)
      .option("sep",    ",")
      .mode("error")
      .save(context.config.UNUSED_DATA_FILE_NM)

}
