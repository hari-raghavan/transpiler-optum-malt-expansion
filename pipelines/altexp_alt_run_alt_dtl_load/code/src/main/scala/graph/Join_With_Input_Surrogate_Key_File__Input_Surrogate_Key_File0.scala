package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Join_With_Input_Surrogate_Key_File__Input_Surrogate_Key_File0 {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("csv")
      .option("header", false)
      .option("sep",    "\u0001")
      .schema(
        StructType(
          Array(
            StructField("alt_run_target_dtl_id", StringType, true),
            StructField("tal_assoc_name",        StringType, true),
            StructField("formulary_name",        StringType, true),
            StructField("target_ndc",            StringType, true)
          )
        )
      )
      .load(context.config.SURR_KEY_INPUT_FILE_NM)

}
