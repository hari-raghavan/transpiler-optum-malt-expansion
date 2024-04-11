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

object LKP_Alias_Prod_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup("LKP_Alias_Prod",
                 in,
                 context.spark,
                 List("ndc11"),
                 "alias_set_name",
                 "ndc11",
                 "alias_label_nm",
                 "newline"
    )

}
