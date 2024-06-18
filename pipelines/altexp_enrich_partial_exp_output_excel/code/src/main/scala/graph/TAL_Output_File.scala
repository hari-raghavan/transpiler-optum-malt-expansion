package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object TAL_Output_File {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("csv")
      .option("header", false)
      .option("sep",    "NA")
      .schema(
        StructType(
          Array(StructField("sheet", StringType, true),
                StructField("line",  StringType, true)
          )
        )
      )
      .load(context.config.OUTPUT_FILE)

}
