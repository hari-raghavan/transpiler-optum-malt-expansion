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

object RLP_List_qualifiers_per_UDL_and_set_override_flag_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(col("udl_id"),
              col("udl_nm"),
              col("user_defined_list_desc"),
              col("qual_list"),
              col("override_flg"),
              col("eff_dt"),
              col("term_dt"),
              col("newline")
    )

}
