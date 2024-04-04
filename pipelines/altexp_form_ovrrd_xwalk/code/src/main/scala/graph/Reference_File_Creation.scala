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

object Reference_File_Creation {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      lit(null).cast(StringType).as("formulary_name"),
      lit(null).cast(StringType).as("carrier"),
      lit(null).cast(StringType).as("account"),
      lit(null).cast(StringType).as("group"),
      lit(null).cast(StringType).as("customer_name"),
      coalesce(
        concat(
          lit("if(!is_null($\"run_eff_dt\") and $\"run_eff_dt\" === "),
          lit(context.config.FIRST_OF_NEXT_YEAR),
          lit(" ) 1 else if ( !is_null($\"run_eff_dt\") ) 2")
        ).cast(DecimalType(1, 0)),
        lit(0)
      ).as("is_future_snap"),
      col("data_path"),
      lit(null).cast(StringType).as("run_eff_dt"),
      coalesce(rpad(lit("""
"""), 1, " "), lit("""
""")).as("newline")
    )

}
