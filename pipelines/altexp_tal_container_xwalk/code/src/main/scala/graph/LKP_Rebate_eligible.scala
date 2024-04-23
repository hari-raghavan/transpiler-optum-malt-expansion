package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object LKP_Rebate_eligible {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("csv")
      .option("header", false)
      .option("sep",    "\u0001")
      .schema(
        StructType(
          Array(StructField("dl_bit",         StringType, true),
                StructField("rebate_elig_cd", StringType, true)
          )
        )
      )
      .load("/~null")

}
