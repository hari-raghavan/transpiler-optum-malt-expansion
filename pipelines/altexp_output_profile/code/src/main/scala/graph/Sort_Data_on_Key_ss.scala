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

object Sort_Data_on_Key_ss {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.orderBy(
      col("carrier").asc,
      col("account").asc,
      col("group").asc,
      col("future_flg").asc,
      to_date(col("as_of_dt"), "yyyyMMdd").asc,
      col("output_profile_id").asc,
      col("alias_priority").asc
    )

}
