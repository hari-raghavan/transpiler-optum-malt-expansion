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
      connectionProperties.put("user",     Config.DB_User)
      connectionProperties.put("jdbcUrl",  Config.DB_Url)
      connectionProperties.put("dbDriver", Config.DB_Driver)
      val brConnect = spark.sparkContext.broadcast(connectionProperties)
      val query0    = s"""INSERT INTO FA_OWNER.ALT_RUN_TARGET_DTL
    ( ALT_RUN_TARGET_DTL_ID , ALT_RUN_ID, FORMULARY_NAME, TARGET_NDC, TARGET_PROD_NAME_EXT, TARGET_PROD_SHORT_DESC, TARGET_GPI14_DESC, TARGET_FORMULARY_TIER, TARGET_MULTI_SRC_CD, TARGET_ROA_CD,
      TARGET_DOSAGE_FORM_CD, REC_CRT_TS, REC_CRT_USER_ID, TARGET_PA_TYPE_CD, TARGET_FORMULARY_TIER_DESC, TARGET_PA_REQD_IND, TARGET_STEP_THERAPY_TYPE_CD,
      TARGET_SPECIALTY_IND, TARGET_STEP_THERAPY_IND, TARGET_GPI14, TARGET_FORMULARY_STATUS, TARGET_FORMULARY_STATUS_DESC,TARGET_STEP_THERAPY_GROUP_NAME,TARGET_STEP_THERAPY_STEP_NUM,TARGET_GPI8_DESC, LAST_EXP_DT , FORMULARY_CD, TAL_ASSOC_NAME, TARGET_TALA_ST, TARGET_UDL)
    VALUES
    ( ?, ?, ?, ?, ?, ?, ?, ?,
      ?, ?, ?, ?, ?, ?, ?_desc,
      ?, ?, ?, ?, ?, ?, ?_desc,
      ?,?, ?, ?, ?, ?, ?, ?)"""
      in.rdd.foreachPartition { partition ⇒
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
              preparedStmt.setString(1,  row.getString(row.fieldIndex("alt_run_target_dtl_id")))
              preparedStmt.setString(2,  row.getString(row.fieldIndex("alt_run_id")))
              preparedStmt.setString(3,  row.getString(row.fieldIndex("formulary_name")))
              preparedStmt.setString(4,  row.getString(row.fieldIndex("target_ndc")))
              preparedStmt.setString(5,  row.getString(row.fieldIndex("target_prod_name_ext")))
              preparedStmt.setString(6,  row.getString(row.fieldIndex("target_prod_short_desc")))
              preparedStmt.setString(7,  row.getString(row.fieldIndex("target_gpi14_desc")))
              preparedStmt.setString(8,  row.getString(row.fieldIndex("target_formulary_tier")))
              preparedStmt.setString(9,  row.getString(row.fieldIndex("target_multi_src_cd")))
              preparedStmt.setString(10, row.getString(row.fieldIndex("target_roa_cd")))
              preparedStmt.setString(11, row.getString(row.fieldIndex("target_dosage_form_cd")))
              preparedStmt.setTimestamp(12,
                                        new java.sql.Timestamp(
                                          new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                                            .parse(
                                              row.getString(row.fieldIndex("rec_crt_ts"))
                                            )
                                            .getTime
                                        )
              )
              preparedStmt.setString(13, row.getString(row.fieldIndex("rec_crt_user_id")))
              preparedStmt.setString(14, row.getString(row.fieldIndex("target_pa_type_cd")))
              preparedStmt.setString(15, row.getString(row.fieldIndex("target_formulary_tier_desc")))
              preparedStmt.setString(16, row.getString(row.fieldIndex("target_pa_reqd_ind")))
              preparedStmt.setString(17, row.getString(row.fieldIndex("target_step_therapy_type_cd")))
              preparedStmt.setString(18, row.getString(row.fieldIndex("target_specialty_ind")))
              preparedStmt.setString(19, row.getString(row.fieldIndex("target_step_therapy_ind")))
              preparedStmt.setString(20, row.getString(row.fieldIndex("target_gpi14")))
              preparedStmt.setString(21, row.getString(row.fieldIndex("target_formulary_status")))
              preparedStmt.setString(22, row.getString(row.fieldIndex("target_formulary_status_desc")))
              preparedStmt.setString(23, row.getString(row.fieldIndex("target_step_therapy_group_name")))
              preparedStmt.setString(24, row.getString(row.fieldIndex("target_step_therapy_step_num")))
              preparedStmt.setString(25, row.getString(row.fieldIndex("target_gpi8_desc")))
              preparedStmt.setDate(26,
                                   new java.sql.Date(
                                     new SimpleDateFormat("yyyyMMdd")
                                       .parse(
                                         row.getString(row.fieldIndex("last_exp_dt"))
                                       )
                                       .getTime
                                   )
              )
              preparedStmt.setString(27, row.getString(row.fieldIndex("formulary_cd")))
              preparedStmt.setString(28, row.getString(row.fieldIndex("tal_assoc_name")))
              preparedStmt.setString(29, row.getString(row.fieldIndex("tala")))
              preparedStmt.setString(30, row.getString(row.fieldIndex("tar_udl")))
              preparedStmt.addBatch()
            }
            preparedStmt.executeBatch()
          }
          preparedStmt.close()
        } finally if (dbc != null)
          dbc.close()
      }
    val out = in
    out
  }

}
