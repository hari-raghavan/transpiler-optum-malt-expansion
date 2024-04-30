package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Source_Table13 {

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
      """SELECT 	        ADL.ALIAS_DTL_ID
        ,	AL.ALIAS_ID
        ,	AL.ALIAS_NAME
        ,       CASE 
                        WHEN UPPER ( TRIM ( QUAL_ID_TYPE_CD ) ) = 'PRODUCT ID'  AND LENGTH( QUAL_ID_VALUE ) = 11        THEN 1
                        WHEN UPPER ( TRIM ( QUAL_ID_TYPE_CD ) ) = 'PRODUCT ID'  AND LENGTH( QUAL_ID_VALUE ) =  9        THEN 2
                        WHEN QUAL_ID_TYPE_CD                    = 'GPI'         AND LENGTH( QUAL_ID_VALUE ) = 14        THEN 3
                        WHEN QUAL_ID_TYPE_CD                    = 'GPI'         AND LENGTH( QUAL_ID_VALUE ) = 12        THEN 4
                        WHEN QUAL_ID_TYPE_CD                    = 'GPI'         AND LENGTH( QUAL_ID_VALUE ) = 10        THEN 5
                        WHEN QUAL_ID_TYPE_CD                    = 'GPI'         AND LENGTH( QUAL_ID_VALUE ) =  8        THEN 6
                        WHEN QUAL_ID_TYPE_CD                    = 'GPI'         AND LENGTH( QUAL_ID_VALUE ) =  6        THEN 7
                        WHEN QUAL_ID_TYPE_CD                    = 'GPI'         AND LENGTH( QUAL_ID_VALUE ) =  4        THEN 8
                        ELSE 99
                END AS QUAL_PRIORITY
        ,	ADL.QUAL_ID_TYPE_CD
        ,	ADL.QUAL_ID_VALUE
        ,	ADL.SEARCH_TXT
        ,	ADL.REPLACE_TXT
        ,       ADL.RANK
        ,       ADL.EFF_DT 
        ,       ADL.TERM_DT
FROM FA_OWNER.ALIAS AL, FA_OWNER.ALIAS_DTL ADL
WHERE AL.ALIAS_ID = ADL.ALIAS_ID AND ADL.DRUG_ATTR_TYPE_CD = 2 AND ADL.REC_ACTIVE_IND = 'Y' AND AL.REC_ACTIVE_IND = 'Y'"""
    )
    var df = reader.load()
    df
  }

}
