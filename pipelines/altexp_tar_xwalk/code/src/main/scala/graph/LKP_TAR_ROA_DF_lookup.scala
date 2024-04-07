package graph

import io.prophecy.libs._
import config.Context
import udfs.UDFs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object LKP_TAR_ROA_DF_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup(
      "LKP_TAR_ROA_DF",
      in,
      context.spark,
      List("tar_roa_df_set_id"),
      "tar_roa_df_set_dtl_id",
      "tar_roa_df_set_id",
      "target_roa_cd",
      "target_dosage_form_cd",
      "alt_roa_cd",
      "alt_dosage_form_cd",
      "priority",
      "newline"
    )

}
