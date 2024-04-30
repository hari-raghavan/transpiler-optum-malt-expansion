package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Source_Table3 {

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
      "SELECT OUTPUT_PROFILE_REBATE_DTL_ID,UDL_NAME,OUTPUT_PROFILE_ID,REBATE_ELIG_CD FROM FA_OWNER.OUTPUT_PROFILE_REBATE_DTL WHERE REC_ACTIVE_IND='Y'"
    )
    var df = reader.load()
    df
  }

}
