package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Source_Table4 {

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
      """SELECT TC_DTL.TAC_DTL_ID
      ,TC.TAC_ID
      ,TC.TAC_NAME
      ,TC_DTL.PRIORITY
      ,TC_DTL.TARGET_RULE
      ,TC_DTL.ALT_RULE
      ,TC_DTL.EFF_DT
      ,TC_DTL.TERM_DT
FROM FA_OWNER.TAC TC, FA_OWNER.TAC_DTL TC_DTL
WHERE TC.TAC_ID = TC_DTL.TAC_ID AND TC.REC_ACTIVE_IND = 'Y' AND TC_DTL.REC_ACTIVE_IND = 'Y' AND TC.PUBLISHED_IND = 'Y'"""
    )
    var df = reader.load()
    df
  }

}
