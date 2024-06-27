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

object Count_of_records {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(lit(1).as("1"))
      .agg(
        when(count(struct(col("sheet"), col("line"))) >= lit(1048575), lit(1))
          .otherwise(lit(0))
          .cast(DecimalType(1, 0))
          .as("flag")
      )

}
