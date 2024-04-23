package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object LKP_CLIN_INDCN {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("csv")
      .option("header", false)
      .option("sep",    "\u0001")
      .schema(
        StructType(
          Array(
            StructField("tal_assoc_id",       StringType, true),
            StructField("tal_assoc_name",     StringType, true),
            StructField("clinical_indn_id",   StringType, true),
            StructField("clinical_indn_name", StringType, true),
            StructField("clinical_indn_desc", StringType, true),
            StructField("rank",               StringType, true)
          )
        )
      )
      .load("/~null")

}
