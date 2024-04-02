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

object LKP_UDL_Master_CrossWalk_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup(
      "LKP_UDL_Master_CrossWalk",
      in,
      context.spark,
      List("udl_id"),
      "udl_id",
      "udl_nm",
      "user_defined_list_desc",
      "qual_list",
      "override_flg",
      "eff_dt",
      "term_dt",
      "newline"
    )

}
