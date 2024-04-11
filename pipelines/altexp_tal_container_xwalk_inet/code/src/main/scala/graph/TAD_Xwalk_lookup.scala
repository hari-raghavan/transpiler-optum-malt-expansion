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

object TAD_Xwalk_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup("TAD_Xwalk",
                 in,
                 context.spark,
                 List("target_gpi14"),
                 "target_gpi14",
                 "alt_selection_cd",
                 "alt_selection_ids",
                 "tad_alt_dtls",
                 "newline"
    )

}
