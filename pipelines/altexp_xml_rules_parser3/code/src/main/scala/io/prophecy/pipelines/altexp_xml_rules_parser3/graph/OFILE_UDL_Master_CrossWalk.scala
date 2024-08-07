package io.prophecy.pipelines.altexp_xml_rules_parser3.graph

import io.prophecy.libs._
import io.prophecy.pipelines.altexp_xml_rules_parser3.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object OFILE_UDL_Master_CrossWalk {

  def apply(context: Context, in: DataFrame): Unit =
    in.write
      .format("csv")
      .option("header", false)
      .option("sep",    "\u0000")
      .mode("error")
      .save(context.config.UDL_MSTR_XWALK)

}
