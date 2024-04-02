package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object UDL_ref_File {

  def apply(context: Context, in: DataFrame): Unit = {
    var writer = in.write.format("text").mode("overwrite")
    writer = writer
    writer = writer
    writer.save(context.config.UDL_REF_FILE)
  }

}
