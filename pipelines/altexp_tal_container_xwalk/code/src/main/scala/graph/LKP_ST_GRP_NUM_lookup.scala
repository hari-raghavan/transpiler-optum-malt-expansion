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

object LKP_ST_GRP_NUM_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup("LKP_ST_GRP_NUM",
                 in,
                 context.spark,
                 List("__id"),
                 "tac_id",
                 "tac_name",
                 "priority",
                 "_target_st_grp_num",
                 "_alt_st_grp_nums"
    )

}
