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
object Curr_Map_GPI14_under_GPI12_input_select_filter { def apply(context: Context, in: DataFrame): DataFrame = in.filter(!to_timestamp(first_defined(adjustCenturyDateInCyyFormat(lpad(col("eff_dt"), 7, "0").cast(StringType), lit("CyyMMdd")), lit("INVALID")), "yyyyMMdd").isNull and (!to_timestamp(first_defined(adjustCenturyDateInCyyFormat(lpad(col("term_dt"), 7, "0").cast(StringType), lit("CyyMMdd")), lit("INVALID")), "yyyyMMdd").isNull and first_defined(adjustCenturyDateInCyyFormat(lpad(col("term_dt"), 7, "0").cast(StringType), lit("CyyMMdd")), lit("INVALID")) >= lit(context.config.BUSINESS_DATE)) and (when(col("inactive_dt") === lit("0"), first_defined(adjustCenturyDateInCyyFormat(lpad(col("term_dt"), 7, "0").cast(StringType), lit("CyyMMdd")), lit("INVALID"))).otherwise(first_defined(adjustCenturyDateInCyyFormat(lpad(col("inactive_dt"), 7, "0").cast(StringType), lit("CyyMMdd")), lit("INVALID"))).cast(BooleanType).isNull or !to_timestamp(when(col("inactive_dt") === lit("0"), first_defined(adjustCenturyDateInCyyFormat(lpad(col("term_dt"), 7, "0").cast(StringType), lit("CyyMMdd")), lit("INVALID"))).otherwise(first_defined(adjustCenturyDateInCyyFormat(lpad(col("inactive_dt"), 7, "0").cast(StringType), lit("CyyMMdd")), lit("INVALID"))).cast(BooleanType), "yyyyMMdd").isNull)) }