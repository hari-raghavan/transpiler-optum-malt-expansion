package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Create_Criteria_Tab {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("udl_id"))
      .agg(
        last(lit("UDL_EXPANSION_CRITERIA")).as("sheet"),
        last(
          concat(
            col("udl_nm"),
            lit("\t"),
            col("udl_desc"),
            lit("\t"),
            lit("PUB"),
            lit("\t"),
            date_format(to_date(lit("20240627"), "yyyyMMdd"), "MM/dd/yyyy")
              .cast(StringType),
            lit("\t"),
            lit("RXCL3-CTR"),
            lit("\t"),
            lit("NULL"),
            lit("\t"),
            lit("NULL"),
            lit("\t"),
            lit("NULL")
          )
        ).as("line")
      )

}
