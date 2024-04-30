package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Source_Table10 {

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
				TAR_ROA_DF_SET_DTL_ID
			,	TAR_ROA_DF_SET_ID
			,	TAR_ROA.TARGET_ROA_CD
			,	TAR_ROA.TARGET_DOSAGE_FORM_CD
			, 	TAR_ROA.ALT_ROA_CD
			, 	TAR_ROA.ALT_DOSAGE_FORM_CD
			, 	TAR_ROA.PRIORITY
FROM FA_OWNER.TAR_ROA_DF_SET_DTL TAR_ROA
WHERE TAR_ROA.REC_ACTIVE_IND='Y'"""
    )
    var df = reader.load()
    df
  }

}
