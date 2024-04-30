package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Source_Table0 {

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
				DDS  .RXCLAIM_ENV_NAME                         AS RXCLAIM_ENV_NAME
			,	DDS  .CAG_CARRIER                              AS CARRIER
			,	DDS  .CAG_ACCOUNT                              AS ACCOUNT
			,	DDS  .CAG_GROUP                                AS 'GROUP'
			,	DDS  .RUN_EFF_DT                               AS 'RUN_EFF_DT'
			,	DDSD .DRUG_DATA_SET_DTL_ID
			,	DDSD .DRUG_DATA_SET_ID
			,	DDSD .NDC		    		        AS NDC11
			,	DDSD .GPI14                                    
			,	DDSD .STATUS_CD
			,	DDSD .EFF_DT
			,	DDSD .TERM_DT
			,	DDSD .INACTIVE_DT
			,	DDSD .MULTI_SRC_CD				AS MSC
			,	DDSD .PROD_NAME_EXT 			        AS DRUG_NAME
			,       DDSD .PROD_SHORT_DESC
			,	DDSD .RX_OTC
			,	DDSD .RX_OTC_CD
			,	DDSD .DESI
			,	DDSD .ROA_CD
			,	DDSD .DOSAGE_FORM_CD
                        ,       DDSD .PROD_STRENGTH
			,	DDSD .REPACK_CD
			,       DDSD .GPI14_DESC
			,       DDSD .GPI8_DESC
FROM 	FA_OWNER.DRUG_DATA_SET DDS
			INNER JOIN FA_OWNER.DRUG_DATA_SET_DTL DDSD   ON DDS .DRUG_DATA_SET_ID   = DDSD .DRUG_DATA_SET_ID
                        AND DDS  .RXCLAIM_ENV_NAME LIKE '${Config.ENV_NM}%'
                        AND DDSD .TERM_DT > DDSD .INACTIVE_DT
                        AND ( DDS  .CAG_CARRIER IS NULL OR 
                              DDS  .CAG_CARRIER = '*ALL' OR 
                              DDS  .CAG_CARRIER IN ( SELECT DISTINCT CAG_CARRIER FROM FA_OWNER.OUTPUT_PROFILE 
                                                      WHERE RXCLAIM_ENV_NAME LIKE '${Config.ENV_NM}%' AND REC_ACTIVE_IND = 'Y' AND PUBLISHED_IND = 'Y'
                                                      AND CAG_CARRIER IS NOT NULL AND CAG_CARRIER <> '*ALL'
                                                      AND FORMULARY_CUSTOMER_ID IS NOT NULL ) 
                            )
		       AND (DDS.RUN_EFF_DT IS NULL OR TO_CHAR(DDS.RUN_EFF_DT, 'DDD') = '001' )"""
    )
    var df = reader.load()
    df
  }

}
