package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Source_Table7 {

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
      "SELECT TA_ASSOC_DTL.TAL_ASSOC_DTL_ID      ,TA_ASSOC.TAL_ASSOC_ID      ,TA_ASSOC.TAL_ASSOC_NAME      ,TA_ASSOC.TAL_ASSOC_DESC      ,TA_ASSOC.TAL_ASSOC_TYPE_CD      ,TA_ASSOC_DTL.TARGET_UDL_NAME      ,TA_ASSOC_DTL.ALT_UDL_NAME      ,TA_ASSOC_DTL.ALT_RANK      ,REPLACE(TA_CD.TA_JOIN_CD_DESC, '-') AS SHARED_QUAL      ,TA_ASSOC.OVERRIDE_TAC_NAME      ,TA_ASSOC.OVERRIDE_TAR_NAME      ,TA_ASSOC_DTL.CONSTITUENT_GROUP      ,TA_ASSOC_DTL.CONSTITUENT_REQD      ,TA_ASSOC_DTL.CONSTITUENT_RANKFROM FA_OWNER.TAL_ASSOC TA_ASSOC, FA_OWNER.TAL_ASSOC_DTL TA_ASSOC_DTL, FA_OWNER.TA_JOIN_CD TA_CD     WHERE TA_ASSOC.TAL_ASSOC_ID = TA_ASSOC_DTL.TAL_ASSOC_ID     AND TA_ASSOC.REC_ACTIVE_IND = 'Y' AND TA_ASSOC_DTL.REC_ACTIVE_IND = 'Y'     AND TA_ASSOC.TA_JOIN_CD = TA_CD.TA_JOIN_CD"
    )
    var df = reader.load()
    df
  }

}
