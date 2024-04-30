package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Source_Table2 {

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
      s"SELECT 				      OPFD .OUTPUT_PROFILE_ID	    , OP .RXCLAIM_ENV_NAME            , OPFD .FORMULARY_NAME	    , OPFD .OUTPUT_PROFILE_FORM_DTL_ID            , OP .OUTPUT_PROFILE_NAME            , OPAD.ALIAS_NAME            , OPAD.PRIORITY                         AS ALIAS_PRIORITY            , OP .CAG_CARRIER                      AS CARRIER            , OP .CAG_ACCOUNT                      AS 'ACCOUNT'	    , OP .CAG_GROUP                        AS 'GROUP'	    , OP .TAL_NAME	    , OP .TAC_NAME	    , OP .TAR_NAME	    , OP .TSD_NAME             , JB .JOB_ID            , JB .JOB_NAME     	    , OP .FORMULARY_CUSTOMER_ID            AS CUSTOMER_NAME                   , JB .RUN_DAY            , OP .LOB_NAME	            , JB .RUN_JAN1_END_MMDD            , JB .RUN_JAN1_START_MMDD            , CASE                 WHEN JB.RUN_NEXT_MONTH_DAY IS NOT NULL                  THEN 2                 WHEN JB .RUN_JAN1_START_MMDD IS NOT NULL AND SUBSTR('${Config.BUSINESS_DATE}', 5, 4) >= JB .RUN_JAN1_START_MMDD AND SUBSTR('${Config.BUSINESS_DATE}', 5, 4) <= JB .RUN_JAN1_END_MMDD                 THEN 1                 ELSE 0              END AS FUTURE_FLG            , OPFD .FORMULARY_PSEUDONYM            , OP.NOTES_ID                , OP.OUTPUT_PROFILE_DESC            , OP.FORMULARY_OPTION_CD            , OPLD.LAYOUT_NAME            , CASE                  WHEN JB.RUN_NEXT_MONTH_DAY IS NOT NULL                 THEN ADD_MONTHS(trunc(to_date('${Config.BUSINESS_DATE}', 'YYYYMMDD'), 'MM'),1)+(JB.RUN_NEXT_MONTH_DAY-1)                  WHEN JB .RUN_JAN1_START_MMDD IS NOT NULL AND SUBSTR('${Config.BUSINESS_DATE}', 5, 4) >= JB .RUN_JAN1_START_MMDD AND SUBSTR('${Config.BUSINESS_DATE}', 5, 4) <= JB .RUN_JAN1_END_MMDD                 THEN TO_DATE('${Config.FIRST_OF_NEXT_YEAR}','YYYYMMDD')                 ELSE TO_DATE('${Config.BUSINESS_DATE}','YYYYMMDD')              END AS AS_OF_DT                        , OP.ST_TAC_IND    FROM                     FA_OWNER.OUTPUT_PROFILE OP INNER JOIN               FA_OWNER.OUTPUT_PROFILE_FORM_DTL OPFD	 ON OPFD .OUTPUT_PROFILE_ID = OP .OUTPUT_PROFILE_ID INNER JOIN               FA_OWNER.OUTPUT_PROFILE_LAYOUT_DTL OPLD ON OPLD .OUTPUT_PROFILE_ID = OP .OUTPUT_PROFILE_IDINNER JOIN               FA_OWNER.OUTPUT_PROFILE_JOB_DTL OPJD ON OPJD.OUTPUT_PROFILE_ID = OP .OUTPUT_PROFILE_IDINNER JOIN               FA_OWNER.'JOB' JB  ON JB.JOB_NAME = OPJD.JOB_NAMELEFT OUTER JOIN          FA_OWNER.OUTPUT_PROFILE_ALIAS_DTL OPAD ON OPAD.OUTPUT_PROFILE_ID = OP .OUTPUT_PROFILE_ID AND OPAD .REC_ACTIVE_IND = 'Y'WHERE        OP .FORMULARY_CUSTOMER_ID IS NOT NULL        AND OP .REC_ACTIVE_IND = 'Y' AND  OP .PUBLISHED_IND = 'Y' AND OPFD .REC_ACTIVE_IND = 'Y'        AND (TO_DATE('${Config.BUSINESS_DATE}','YYYYMMDD') BETWEEN JB .EFF_DT AND JB .TERM_DT)	AND JB .REC_ACTIVE_IND = 'Y' AND OPJD .REC_ACTIVE_IND = 'Y'        AND (	        JB .RUN_DAY IS NULL		OR (    JB .JOB_RUN_FREQ_CD = 2        AND     TO_CHAR(TO_DATE('${Config.BUSINESS_DATE}','YYYYMMDD'),'D') = JB .RUN_DAY )		OR (    JB .JOB_RUN_FREQ_CD = 3        AND     TO_CHAR(TO_DATE('${Config.BUSINESS_DATE}','YYYYMMDD'),'DD') = JB .RUN_DAY )            )        AND OP .RXCLAIM_ENV_NAME = '${Config.ENV_NM}'"
    )
    var df = reader.load()
    df
  }

}
