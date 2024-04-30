package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Source_Table12 {

  def apply(context: Context): DataFrame = {
    val Config = context.config
    import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
    var reader = context.spark.read.format("jdbc")
    reader = reader
      .option("url",               s"${Config.DB_Url}")
      .option("user",              s"${Config.DB_User}")
      .option("password",          s"${Config.DB_Password}")
      .option("pushDownPredicate", true)
      .option("driver",            Config.DB_Driver)
    reader = reader.option(
      "query",
      """SELECT UDL.USER_DEFINED_LIST_ID
,       UDL.USER_DEFINED_LIST_NAME
,       UDL.USER_DEFINED_LIST_DESC
,       UDLR.USER_DEFINED_LIST_RULE_ID
,       UDLR.RULE
,       UDLR.INCL_CD
,	UDLR.EFF_DT
,	UDLR.TERM_DT
FROM FA_OWNER.USER_DEFINED_LIST UDL
INNER JOIN FA_OWNER.USER_DEFINED_LIST_RULE UDLR ON UDL.USER_DEFINED_LIST_ID = UDLR.USER_DEFINED_LIST_ID
WHERE UDL.REC_ACTIVE_IND = 'Y' AND UDLR.REC_ACTIVE_IND = 'Y' AND UDL.PUBLISHED_IND='Y'"""
    )
    var df = reader.load()
    df
  }

}
