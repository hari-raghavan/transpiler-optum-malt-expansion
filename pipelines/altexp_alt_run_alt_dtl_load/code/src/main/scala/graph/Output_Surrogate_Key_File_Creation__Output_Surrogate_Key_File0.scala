package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Output_Surrogate_Key_File_Creation__Output_Surrogate_Key_File0 {

  def apply(context: Context, in: DataFrame): Unit =
    in.write
      .format("csv")
      .option("header", false)
      .option("sep",    "\u0001")
      .mode("error")
      .save(context.config.SURR_KEY_OUTPUT_FILE_NM)

}
