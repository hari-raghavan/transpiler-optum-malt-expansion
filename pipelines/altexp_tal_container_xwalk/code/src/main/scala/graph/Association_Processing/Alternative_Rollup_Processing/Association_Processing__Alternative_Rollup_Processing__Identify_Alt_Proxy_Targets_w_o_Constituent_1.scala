package graph.Association_Processing.Alternative_Rollup_Processing

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.Association_Processing.Alternative_Rollup_Processing.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Association_Processing__Alternative_Rollup_Processing__Identify_Alt_Proxy_Targets_w_o_Constituent_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(
        col("alt_run_alt_dtl_load.tal_assoc_name").as("tal_assoc_name"),
        col("alt_run_alt_dtl_load.target_ndc").as("target_ndc"),
        col("alt_run_alt_dtl_load.alt_prod_short_desc_grp")
          .as("alt_prod_short_desc_grp")
      )
      .agg(
        element_at(
          reverse(
            filter(
              collect_list(col("alt_run_alt_dtl_load")),
              xx =>
                xx.getField("rank") === min(col("alt_run_alt_dtl_load.rank"))
            )
          ),
          1
        ).as("out"),
        min(col("alt_run_alt_dtl_load.rank").cast(StringType))
          .cast(StringType)
          .as("rank")
      )

}
