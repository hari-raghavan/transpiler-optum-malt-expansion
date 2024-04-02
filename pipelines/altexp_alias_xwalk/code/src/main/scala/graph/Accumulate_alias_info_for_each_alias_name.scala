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

object Accumulate_alias_info_for_each_alias_name {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("alias_name"))
      .agg(
        max(col("alias_id")).cast(DecimalType(10, 0)).as("alias_id"),
        collect_list(
          struct(
            col("qual_priority").cast(StringType).as("qual_priority"),
            coalesce(col("qual_id_value"), lit("")).as("qual_id_value"),
            col("search_txt"),
            col("replace_txt"),
            col("eff_dt"),
            col("term_dt")
          )
        ).as("alias_info")
      )

}
