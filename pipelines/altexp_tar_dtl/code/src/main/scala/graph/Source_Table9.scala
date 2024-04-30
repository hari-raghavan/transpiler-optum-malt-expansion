package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Source_Table9 {

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
				TAR_DTL.TAR_ID
			,	TAR_DTL.TAR_DTL_ID
			,	TAR.TAR_NAME
			, 	TAR_DTL.SORT_IND
			,	TAR_DTL.FILTER_IND
			, 	TAR_DTL.PRIORITY
			, 	TAR_DTL.TAR_DTL_TYPE_CD
			, 	TAR_DTL.TAR_ROA_DF_SET_ID
			, 	TAR_DTL.TARGET_RULE
			, 	TAR_DTL.ALT_RULE 
                        ,       TAR_DTL.REBATE_ELIG_CD
                        ,	TAR.EFF_DT
                        ,	TAR.TERM_DT			
FROM FA_OWNER.TAR_DTL TAR_DTL
INNER JOIN FA_OWNER.TAR TAR ON TAR.TAR_ID = TAR_DTL.TAR_ID
WHERE TAR_DTL.REC_ACTIVE_IND='Y' AND TAR.REC_ACTIVE_IND='Y' AND TAR.PUBLISHED_IND ='Y'"""
    )
    var df = reader.load()
    df
  }

}
