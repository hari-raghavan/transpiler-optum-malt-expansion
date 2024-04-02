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

object Form_Rule_Products_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup("Form_Rule_Products",
                 in,
                 context.spark,
                 List("qualifier_cd", "compare_value"),
                 "qualifier_cd",
                 "operator",
                 "compare_value",
                 "products"
    )

}
