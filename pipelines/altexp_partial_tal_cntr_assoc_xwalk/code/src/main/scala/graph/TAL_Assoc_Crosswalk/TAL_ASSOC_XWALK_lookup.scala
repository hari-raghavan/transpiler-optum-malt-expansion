package graph.TAL_Assoc_Crosswalk

import io.prophecy.libs._
import graph.TAL_Assoc_Crosswalk.config.Context
import udfs.UDFs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object TAL_ASSOC_XWALK_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup(
      "TAL_ASSOC_XWALK",
      in,
      context.spark,
      List("tal_assoc_name"),
      "tal_assoc_dtl_id",
      "tal_assoc_id",
      "tal_assoc_name",
      "clinical_indn_desc",
      "tal_assoc_desc",
      "tal_assoc_type_cd",
      "target_udl_info",
      "alt_udl_info",
      "shared_qual",
      "override_tac_name",
      "override_tar_name",
      "newline"
    )

}
