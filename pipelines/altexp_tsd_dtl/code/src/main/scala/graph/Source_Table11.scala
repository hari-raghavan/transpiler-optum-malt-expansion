package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Source_Table11 {

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
      """SELECT
				TSD_DTL.TSD_DTL_ID
			,	TSD_DTL.TSD_ID
			,	TSD.TSD_NAME
			,	TSD_DTL.TSD_CD
			,	TSD_DTL.FORMULARY_TIER
			,	TSD_DTL.FORMULARY_STATUS
			,	TSD_DTL.PRIORITY
                        ,       TSD_DTL.EFF_DT
                        ,       TSD_DTL.TERM_DT
FROM	FA_OWNER.TSD TSD
INNER JOIN FA_OWNER.TSD_DTL TSD_DTL ON TSD.TSD_ID = TSD_DTL.TSD_ID
INNER JOIN FA_OWNER.TSD_CD TSD_CD ON TSD_DTL.TSD_CD = TSD_CD.TSD_CD
WHERE 	TSD.REC_ACTIVE_IND = 'Y' 
AND TSD_DTL.REC_ACTIVE_IND='Y' AND TSD_CD.REC_ACTIVE_IND='Y'"""
    )
    var df = reader.load()
    df
  }

}
