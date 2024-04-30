package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Source_Table_14 {

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
        RD.FILE_LOAD_CNTL_ID,
        LISTAGG(RD.COMPONENT_IDS, ',') WITHIN GROUP (ORDER BY COMPONENT_IDS) AS COMPONENT_IDS,
        RD.AS_OF_DATE,
        RD.RXCLAIM_ENV_NAME,
        RD.CAG_CARRIER AS 'CARRIER',
        RD.CAG_ACCOUNT AS 'ACCOUNT',
        RD.CAG_GROUP AS 'GROUP',
        FLC.COMPONENT_TYPE_CD,
        FLC.FILE_NAME_W,
        FLC.PUBLISHED_IND,
        FLC.ALT_RUN_ID,
        FLC.REPORT_FILE_NAME
FROM
FA_OWNER.FILE_LOAD_CNTL FLC INNER JOIN FA_OWNER.REPORT_DETAILS RD ON RD.FILE_LOAD_CNTL_ID = FLC.FILE_LOAD_CNTL_ID
WHERE
  (   ( FILE_LOAD_TYPE_CD='A' 
        AND RD.COMPONENT_IDS IN ( SELECT OP .OUTPUT_PROFILE_NAME FROM FA_OWNER.OUTPUT_PROFILE OP 
                                WHERE OP .REC_ACTIVE_IND = 'Y' AND  OP .PUBLISHED_IND = 'Y'  AND FORMULARY_CUSTOMER_ID IS NOT NULL ) )
       OR FILE_LOAD_TYPE_CD='P' 
  )
  AND FILE_LOAD_STATUS_CD=0
GROUP BY RD.FILE_LOAD_CNTL_ID,AS_OF_DATE,RXCLAIM_ENV_NAME,CAG_CARRIER,CAG_ACCOUNT,CAG_GROUP,FLC.COMPONENT_TYPE_CD,FLC.FILE_NAME_W,FLC.PUBLISHED_IND,FLC.ALT_RUN_ID,FLC.REPORT_FILE_NAME"""
    )
    var df = reader.load()
    df
  }

}
