package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Drug_Data_Set_Drug_Data_Set_DTL {

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
	           SUBSTR(DDSD.GPI14,1,12) AS GPI12
			,       DDSD .GPI14
                        ,	DDSD .EFF_DT
			,	DDSD .TERM_DT
			,	DDSD .INACTIVE_DT
			,       DDS  .RUN_EFF_DT             
FROM 	FA_OWNER.DRUG_DATA_SET DDS
INNER JOIN FA_OWNER.DRUG_DATA_SET_DTL DDSD   ON DDS .DRUG_DATA_SET_ID   = DDSD .DRUG_DATA_SET_ID
                        AND DDS  .RXCLAIM_ENV_NAME IN ('RXBK1-PRD',
                                                        'RXCL1-PRD',
                                                        'RXCL2-CIG',
                                                        'RXCL2-PRD',
                                                        'RXCL3-CTR',
                                                        'RXCL3-KSR',
                                                        'RXCL4-AZM',
                                                        'RXCL4-INM',
                                                        'RXCL4-SDM')
                        AND DDS  .CAG_CARRIER IS NULL
                        AND DDS  .RUN_EFF_DT  IS NULL
                        AND DDSD .TERM_DT > DDSD .INACTIVE_DT"""
    )
    var df = reader.load()
    df
  }

}
