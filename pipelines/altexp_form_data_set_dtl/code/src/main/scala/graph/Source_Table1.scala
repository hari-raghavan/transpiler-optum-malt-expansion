package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Source_Table1 {

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
      s"""SELECT 
              FDS.FORMULARY_DATA_SET_ID,
              FDS.FORMULARY_NAME,
              FDS.CAG_CARRIER                           AS  CARRIER,
              FDS.CAG_ACCOUNT                           AS  'ACCOUNT',
              FDS.CAG_GROUP                             AS  'GROUP',
              FDS.RXCLAIM_ENV_NAME ,
              FDS.FORMULARY_CUSTOMER_ID                 AS CUSTOMER_NAME ,
              FDS.LAST_EXP_DT,
              FDS.RUN_EFF_DT,
              FDSD.FORMULARY_DATA_SET_DTL_ID,
              FDSD.NDC                                  AS NDC11,
              FDSD.FORMULARY_TIER,
              FDSD.FORMULARY_STATUS, 
              FDSD.PA_REQD_IND,
              FDSD.SPECIALTY_IND,
              FDSD.STEP_THERAPY_IND,
              FDSD.FORMULARY_TIER_DESC,
              FDSD.FORMULARY_STATUS_DESC,
              FDSD.PA_TYPE_CD,
              FDSD.STEP_THERAPY_TYPE_CD,
              FDSD.STEP_THERAPY_GROUP_NAME,
              FDSD.STEP_THERAPY_STEP_NUMBER
FROM   
        FA_OWNER.FORMULARY_DATA_SET_DTL FDSD, FA_OWNER.FORMULARY_DATA_SET FDS,
(       SELECT OP.CAG_CARRIER, OP.CAG_ACCOUNT, OP.CAG_GROUP, OP.FORMULARY_CUSTOMER_ID, OPFD.FORMULARY_NAME
        FROM FA_OWNER.OUTPUT_PROFILE OP, FA_OWNER.OUTPUT_PROFILE_FORM_DTL OPFD
        WHERE OP.OUTPUT_PROFILE_ID = OPFD.OUTPUT_PROFILE_ID
        AND OP.RXCLAIM_ENV_NAME = '${Config.ENV_NM}' AND OP.REC_ACTIVE_IND = 'Y' AND OP.PUBLISHED_IND = 'Y' AND OPFD.REC_ACTIVE_IND = 'Y'
        AND OP.FORMULARY_CUSTOMER_ID IS NOT NULL
        AND OPFD.FORMULARY_NAME IN 
        (
                SELECT DISTINCT OPFD.FORMULARY_NAME FROM 
                  FA_OWNER.OUTPUT_PROFILE OP, FA_OWNER.OUTPUT_PROFILE_FORM_DTL OPFD,
                  FA_OWNER.OUTPUT_PROFILE_JOB_DTL OPJD, FA_OWNER.JOB JB
                WHERE     OP.OUTPUT_PROFILE_ID = OPFD.OUTPUT_PROFILE_ID
                      AND OP.OUTPUT_PROFILE_ID = OPJD.OUTPUT_PROFILE_ID
                      AND OPJD.JOB_NAME        = JB.JOB_NAME
                      AND OP.RXCLAIM_ENV_NAME = '${Config.ENV_NM}' AND OP.REC_ACTIVE_IND = 'Y' AND OP.PUBLISHED_IND = 'Y' 
                      AND OPFD.REC_ACTIVE_IND = 'Y' 
                      AND OPJD.REC_ACTIVE_IND = 'Y'
                      AND OP.FORMULARY_CUSTOMER_ID IS NOT NULL
                      AND JB.REC_ACTIVE_IND = 'Y' 
                      AND (TO_DATE('${Config.BUSINESS_DATE}','YYYYMMDD') BETWEEN JB .EFF_DT AND JB .TERM_DT)
                      AND  (          JB .RUN_DAY IS NULL
                              OR (    JB .JOB_RUN_FREQ_CD = 2        AND     TO_CHAR(TO_DATE('${Config.BUSINESS_DATE}','YYYYMMDD'),'D') = JB .RUN_DAY )
                              OR (    JB .JOB_RUN_FREQ_CD = 3        AND     TO_CHAR(TO_DATE('${Config.BUSINESS_DATE}','YYYYMMDD'),'DD') = JB .RUN_DAY )
                            )
        )
        GROUP BY OP.CAG_CARRIER, OP.CAG_ACCOUNT, OP.CAG_GROUP, OP.FORMULARY_CUSTOMER_ID, FORMULARY_NAME )FORM
        WHERE FORM.FORMULARY_NAME  = FDS.FORMULARY_NAME
        AND   FORM.FORMULARY_CUSTOMER_ID = FDS.FORMULARY_CUSTOMER_ID
        AND   FDS.RXCLAIM_ENV_NAME = '${Config.ENV_NM}' AND (FDS.RUN_EFF_DT IS NULL OR (TO_CHAR(FDS.RUN_EFF_DT, 'DDD') = '001' ))
        AND   FDS.FORMULARY_DATA_SET_ID = FDSD.FORMULARY_DATA_SET_ID
        AND ( FDS.CAG_CARRIER IS NULL OR 
              FDS.CAG_CARRIER = '*ALL' OR 
              ( ( FDS.CAG_CARRIER = FORM.CAG_CARRIER AND FDS.CAG_ACCOUNT = FORM.CAG_ACCOUNT AND FDS.CAG_GROUP = FORM.CAG_GROUP )
                OR
                ( FDS.CAG_CARRIER = FORM.CAG_CARRIER AND FDS.CAG_ACCOUNT = FORM.CAG_ACCOUNT AND FDS.CAG_GROUP = '*ALL' )
                OR
                ( FDS.CAG_CARRIER = FORM.CAG_CARRIER AND FDS.CAG_ACCOUNT = '*ALL' AND FDS.CAG_GROUP = '*ALL' )
              )
            )"""
    )
    var df = reader.load()
    df
  }

}
