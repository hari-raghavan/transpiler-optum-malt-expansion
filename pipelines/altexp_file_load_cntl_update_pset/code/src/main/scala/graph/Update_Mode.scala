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

object Update_Mode {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import java.util.Properties
     import java.sql._
     import java.text.SimpleDateFormat
     import scala.collection.mutable.ListBuffer
    
     val connectionProperties = new Properties()
        connectionProperties.put("password", Config.DB_Password)
        connectionProperties.put("user", Config.DB_User)
        connectionProperties.put("jdbcUrl", Config.DB_Url)
        connectionProperties.put("dbDriver", Config.DB_Driver)
     val brConnect = spark.sparkContext.broadcast(connectionProperties)
     val query0 = s"""update FA_OWNER.FILE_LOAD_CNTL
    set
    FILE_LOAD_STATUS_CD = 4,
    FILE_NAME_W = ${Config.FILE_NAME_W},
    FILE_LOAD_END_TS = CURRENT_TIMESTAMP,
    REC_LAST_UPD_TS = CURRENT_TIMESTAMP,
    REC_LAST_UPD_USER_ID = ${Config.DB_ALTERNATE_USER}
    WHERE
    FILE_LOAD_CNTL_ID = ?"""
     in.rdd.foreachPartition(partition ⇒ {
          var dbc: Connection = null
          try {
            val connectionProperties = brConnect.value
            Class.forName(connectionProperties.getProperty("dbDriver"))
            dbc = DriverManager.getConnection(
              connectionProperties.getProperty("jdbcUrl"),
              connectionProperties.getProperty("user"),
              connectionProperties.getProperty("password")
            )
            val db_batchSize = 10000
            val preparedStmt = dbc.prepareStatement(query0)
            partition.grouped(db_batchSize).foreach { batch ⇒
              batch.foreach { row ⇒
                preparedStmt.setString(1, row.getString(row.fieldIndex("file_load_cntl_id")))
                preparedStmt.addBatch()
              }
              preparedStmt.executeBatch()
            }
            preparedStmt.close()
          }
          finally {
            if(dbc != null)
              dbc.close()
          }
        })
    
    val out = in
    out
  }

}
