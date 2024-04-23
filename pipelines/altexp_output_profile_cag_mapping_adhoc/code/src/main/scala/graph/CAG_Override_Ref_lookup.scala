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

object CAG_Override_Ref_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup(
      "CAG_Override_Ref",
      in,
      context.spark,
      List("carrier", "account", "group", "is_future_snap"),
      "carrier",
      "account",
      "group",
      "is_future_snap",
      "data_path",
      "newline"
    )

}
