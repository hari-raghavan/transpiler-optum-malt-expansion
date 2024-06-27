package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Write_Excel_Enrich_Product_Data_2 {

  def apply(context: Context, in: DataFrame): Unit =
    in.write
      .format("csv")
      .option("header", false)
      .option("sep",    "NA")
      .mode("error")
      .save("/altexp_partial_tal_exp.xlsx")

}
