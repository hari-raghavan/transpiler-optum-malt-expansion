package graph

import io.prophecy.libs._
import config.Context
import udfs.UDFs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Update_File_Load_Cntl_table_for_Success {
  def apply(context: Context): Unit = {
    val spark = context.spark
    val Config = context.config
    
    import java.sql._
    val queryList = List(s"""update FA_OWNER.FILE_LOAD_CNTL set FILE_LOAD_STATUS_CD = 4,FILE_LOAD_END_TS = CURRENT_TIMESTAMP,REC_LAST_UPD_TS = CURRENT_TIMESTAMP,REC_LAST_UPD_USER_ID = ${Config.DB_ALTERNATE_USER} WHERE FILE_LOAD_TYPE_CD = 'S' AND FILE_LOAD_STATUS_CD = 1""")
        var dbc: Connection = null
        try {
          Class.forName(Config.DB_Driver)
          dbc = DriverManager.getConnection(
                   Config.hostname,
                   Config.DB_User,
                   Config.DB_Password
          )
          executeNonSelectSQLQueries(queryList, dbc)
          dbc.close()
        } catch {
          case e: Throwable ⇒ e.printStackTrace()
        }
  }

}
