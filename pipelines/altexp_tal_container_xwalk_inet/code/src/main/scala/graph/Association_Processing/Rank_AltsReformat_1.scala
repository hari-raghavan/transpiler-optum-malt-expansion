package graph.Association_Processing

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.Association_Processing.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Rank_AltsReformat_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("tal_id").cast(DecimalType(10, 0)).as("tal_id"),
      col("tal_name"),
      col("tal_assoc_name"),
      col("tar_udl_nm"),
      transform(
        col("ta_prdct_dtls"),
        y =>
          struct(
            y.getField("target_dl_bit").as("target_dl_bit"),
            coalesce(y.getField("ST_flag"), lit(0)).as("ST_flag"),
            array(
              struct(
                lit(0).as("alt_prd"),
                lit(0).as("alt_rank"),
                lit("").as("rebate_elig_cd"),
                lit("").as("tad_eli_code"),
                lit("").as("qty_adjust_factor"),
                lit("").as("tal_assoc_name"),
                lit("").as("tala"),
                lit("").as("udl_nm"),
                lit("").as("priority"),
                lit("").as("constituent_group"),
                lit("").as("constituent_reqd")
              )
            ).as("alt_prdcts")
          )
      ).as("ta_prdct_dtls"),
      col("constituent_grp_vec"),
      coalesce(col("newline"), lit("""
""")).as("newline")
    )

}
