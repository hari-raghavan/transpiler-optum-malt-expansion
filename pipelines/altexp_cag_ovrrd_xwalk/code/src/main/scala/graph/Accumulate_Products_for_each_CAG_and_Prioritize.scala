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

object Accumulate_Products_for_each_CAG_and_Prioritize {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("carrier"), col("account"), col("group"))
      .agg(
        max(lit("")).as("data_path"),
        when(max(col("carrier").isNull),             max(lit(1)))
          .when(max(col("carrier") === lit("*ALL")), max(lit(2)))
          .when(max(
                  !col("carrier").isNull
                    .and(col("account") === lit("*ALL"))
                    .and(col("group") === lit("*ALL"))
                ),
                max(lit(3))
          )
          .when(max(
                  !col("carrier").isNull
                    .and(!col("account").isNull)
                    .and(col("group") === lit("*ALL"))
                ),
                max(lit(4))
          )
          .otherwise(max(lit(5)))
          .as("cag_priority"),
        last(col("run_eff_dt")).as("run_eff_dt"),
        collect_list(
          struct(
            col("ndc11"),
            col("gpi14"),
            col("status_cd"),
            col("inactive_dt"),
            col("eff_dt"),
            col("term_dt"),
            col("msc"),
            col("drug_name"),
            col("rx_otc"),
            col("desi"),
            col("roa_cd"),
            col("dosage_form_cd"),
            col("prod_strength").cast(DecimalType(14, 5)).as("prod_strength"),
            col("repack_cd"),
            col("prod_short_desc"),
            col("gpi14_desc"),
            col("gpi8_desc")
          )
        ).as("prdcts")
      )

}
