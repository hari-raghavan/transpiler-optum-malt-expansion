package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Source_Table6 {

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
      """SELECT ASSOC.TAL_ASSOC_ID, ASSOC.TAL_ASSOC_NAME, CLI_INDN_DTL.CLINICAL_INDN_ID, CLI_INDN_DTL.CLINICAL_INDN_NAME, CLI_INDN_DTL.CLINICAL_INDN_DESC, CLI_INDN_DTL.'RANK'
FROM FA_OWNER.TAL_ASSOC  ASSOC   
LEFT OUTER JOIN 	(
						SELECT ASSOC_CLI_INDN.TAL_ASSOC_ID, ASSOC_CLI_INDN.CLINICAL_INDN_ID, CLI_INDN.CLINICAL_INDN_NAME, CLI_INDN.CLINICAL_INDN_DESC, ASSOC_CLI_INDN.'RANK'
						FROM FA_OWNER.TAL_ASSOC_CLINICAL_INDN ASSOC_CLI_INDN, FA_OWNER.CLINICAL_INDN CLI_INDN
						WHERE CLI_INDN.REC_ACTIVE_IND = 'Y'
						AND ASSOC_CLI_INDN.REC_ACTIVE_IND = 'Y'
						AND ASSOC_CLI_INDN.CLINICAL_INDN_ID = CLI_INDN.CLINICAL_INDN_ID
					 ) CLI_INDN_DTL
ON ASSOC.TAL_ASSOC_ID=CLI_INDN_DTL.TAL_ASSOC_ID
WHERE ASSOC.REC_ACTIVE_IND = 'Y'"""
    )
    var df = reader.load()
    df
  }

}
