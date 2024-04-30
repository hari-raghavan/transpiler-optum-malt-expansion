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
      val query0    = s"""INSERT INTO FA_OWNER.ALT_RUN_ALT_DTL
    (  ALT_RUN_ALT_DTL_ID, ALT_RUN_ID, ALT_RUN_TARGET_DTL_ID, ALT_NDC, ALT_PROD_NAME_EXT, ALT_PROD_SHORT_DESC, ALT_GPI8_DESC, ALT_FORMULARY_TIER, ALT_MULTI_SRC_CD, ALT_ROA_CD, ALT_DOSAGE_FORM_CD,
       'RANK', ALT_STEP_THERAPY_IND, ALT_PA_REQD_IND, REC_CRT_TS, REC_CRT_USER_ID, ALT_GPI14_DESC, ALT_SPECIALTY_IND, ALT_PA_TYPE_CD, ALT_FORMULARY_STATUS_DESC,
       ALT_GPI14, ALT_FORMULARY_TIER_DESC, ALT_STEP_THERAPY_GROUP_NAME,ALT_STEP_THERAPY_STEP_NUMBER, ALT_FORMULARY_STATUS, ALT_STEP_THERAPY_TYPE_CD, REBATE_ELIG_CD, TAD_ELIGIBLE_CD, ALT_QTY_ADJ,
       TAL_ASSOC_NAME, ALT_TALA_ST, CONSTITUENT_GROUP, CONSTITUENT_REQD, TAL_ASSOC_RANK, ALT_UDL
    )
    VALUES
    (  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
       ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
       ?, ?_desc ,?,?, ?, ?, ? , ?, ?,
       ?, ?, ?, ?, ?, ?
    )"""
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
              preparedStmt.setString(1,  row.getString(row.fieldIndex("alt_run_alt_dtl_id")))
              preparedStmt.setString(2,  row.getString(row.fieldIndex("alt_run_id")))
              preparedStmt.setString(3,  row.getString(row.fieldIndex("alt_run_target_dtl_id")))
              preparedStmt.setString(4,  row.getString(row.fieldIndex("alt_ndc")))
              preparedStmt.setString(5,  row.getString(row.fieldIndex("alt_prod_name_ext")))
              preparedStmt.setString(6,  row.getString(row.fieldIndex("alt_prod_short_desc")))
              preparedStmt.setString(7,  row.getString(row.fieldIndex("alt_gpi8_desc")))
              preparedStmt.setString(8,  row.getString(row.fieldIndex("alt_formulary_tier")))
              preparedStmt.setString(9,  row.getString(row.fieldIndex("alt_multi_src_cd")))
              preparedStmt.setString(10, row.getString(row.fieldIndex("alt_roa_cd")))
              preparedStmt.setString(11, row.getString(row.fieldIndex("alt_dosage_form_cd")))
              preparedStmt.setString(12, row.getString(row.fieldIndex("rank")))
              preparedStmt.setString(13, row.getString(row.fieldIndex("alt_step_therapy_ind")))
              preparedStmt.setString(14, row.getString(row.fieldIndex("alt_pa_reqd_ind")))
              preparedStmt.setTimestamp(15,
                                        new java.sql.Timestamp(
                                          new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                                            .parse(
                                              row.getString(row.fieldIndex("rec_crt_ts"))
                                            )
                                            .getTime
                                        )
              )
              preparedStmt.setString(16, row.getString(row.fieldIndex("rec_crt_user_id")))
              preparedStmt.setString(17, row.getString(row.fieldIndex("alt_gpi14_desc")))
              preparedStmt.setString(18, row.getString(row.fieldIndex("alt_specialty_ind")))
              preparedStmt.setString(19, row.getString(row.fieldIndex("alt_pa_type_cd")))
              preparedStmt.setString(20, row.getString(row.fieldIndex("alt_formulary_status_desc")))
              preparedStmt.setString(21, row.getString(row.fieldIndex("alt_gpi14")))
              preparedStmt.setString(22, row.getString(row.fieldIndex("alt_formulary_tier_desc")))
              preparedStmt.setString(23, row.getString(row.fieldIndex("alt_step_therapy_group_name")))
              preparedStmt.setString(24, row.getString(row.fieldIndex("alt_step_therapy_step_number")))
              preparedStmt.setString(25, row.getString(row.fieldIndex("alt_formulary_status")))
              preparedStmt.setString(26, row.getString(row.fieldIndex("alt_step_therapy_type_cd")))
              preparedStmt.setString(27, row.getString(row.fieldIndex("rebate_elig_cd")))
              preparedStmt.setString(28, row.getString(row.fieldIndex("tad_eligible_cd")))
              preparedStmt.setString(29, row.getString(row.fieldIndex("alt_qty_adj")))
              preparedStmt.setString(30, row.getString(row.fieldIndex("tal_assoc_name")))
              preparedStmt.setString(31, row.getString(row.fieldIndex("tala")))
              preparedStmt.setString(32, row.getString(row.fieldIndex("constituent_group")))
              preparedStmt.setString(33, row.getString(row.fieldIndex("constituent_reqd")))
              preparedStmt.setString(34, row.getString(row.fieldIndex("tal_assoc_rank")))
              preparedStmt.setString(35, row.getString(row.fieldIndex("alt_udl")))
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
