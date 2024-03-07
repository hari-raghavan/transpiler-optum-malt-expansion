package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Unload_Job_Details {

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
      s"""SELECT DISTINCT JB.JOB_ID, JB.JOB_NAME 
		FROM FA_OWNER.OUTPUT_PROFILE OP, FA_OWNER.OUTPUT_PROFILE_JOB_DTL OPJD, FA_OWNER.JOB JB
                WHERE OP.OUTPUT_PROFILE_ID = OPJD.OUTPUT_PROFILE_ID
                      AND OPJD.JOB_NAME        = JB.JOB_NAME
                      AND OP.REC_ACTIVE_IND = 'Y' AND PUBLISHED_IND = 'Y'  
                      AND OPJD.REC_ACTIVE_IND = 'Y'
                      AND JB.REC_ACTIVE_IND = 'Y' 
                      AND (to_date('${Config.DEFAULT_BUSINESS_DATE}','YYYYMMDD') BETWEEN JB .EFF_DT AND JB .TERM_DT)
                      AND  (          JB .RUN_DAY IS NULL
                              OR (    JB .JOB_RUN_FREQ_CD = 2        AND     TO_CHAR(TO_DATE('${Config.DEFAULT_BUSINESS_DATE}','YYYYMMDD'),'D') = JB .RUN_DAY )
                              OR (    JB .JOB_RUN_FREQ_CD = 3        AND     TO_CHAR(TO_DATE('${Config.DEFAULT_BUSINESS_DATE}','YYYYMMDD'),'DD') = JB .RUN_DAY )
                            )"""
    )
    var df = reader.load()
    df
  }

}
