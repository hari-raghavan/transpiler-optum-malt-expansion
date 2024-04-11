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

object LKP_TAR_Exp_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup(
      "LKP_TAR_Exp",
      in,
      context.spark,
      List("tar_name"),
      "tar_id",
      "tar_dtl_id",
      "tar_name",
      "tar_dtl_type_cd",
      "contents",
      "common_alt_prdcts",
      "common_target_prdcts",
      "keep_all_targets",
      "newline"
    )

}
