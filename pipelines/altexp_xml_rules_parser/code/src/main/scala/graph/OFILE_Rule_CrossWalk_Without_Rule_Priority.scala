package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object OFILE_Rule_CrossWalk_Without_Rule_Priority {

  def apply(context: Context, in: DataFrame): Unit =
    in.write
      .format("csv")
      .option("header", false)
      .option("sep",    "\u0000")
      .mode("error")
      .save(context.config.RULE_XWALK_WT_RL_PRIORITY)

}
