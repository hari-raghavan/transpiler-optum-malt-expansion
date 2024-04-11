package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Target_Load_Ready {

  def apply(context: Context, in: DataFrame): Unit = {
    val Config = context.config
    import _root_.io.prophecy.abinitio.dml.DMLSchema.parse
    import _root_.io.prophecy.libs.FFSchemaRecord
    import _root_.io.prophecy.libs.FixedFormatSchemaImplicits._
    import play.api.libs.json.Json
    try {
      val schema = Some(
        """
record
decimal("\\\\x01", 0, maximum_length=16) alt_run_target_dtl_id = -1 ;
decimal("\\\\x01", 0, maximum_length=16) alt_run_id = -1 ;
string("\\\\x01", 40) tal_assoc_name = NULL ;
string("\\\\x01", 40) tala = NULL ;
string("\\\\x01", 20) tar_udl = NULL ;
string("\\\\x01", 20) formulary_name ;
string("\\\\x01", 11) target_ndc ;
string("\\\\x01", 2) target_formulary_tier ;
string("\\\\x01", 2) target_formulary_status ;
string("\\\\x01", 1) target_pa_reqd_ind ;
string("\\\\x01", 1) target_step_therapy_ind ;
string("\\\\x01", 1) target_specialty_ind ;
string("\\\\x01", 1) target_multi_src_cd ;
string("\\\\x01", 2) target_roa_cd ;
string("\\\\x01", 4) target_dosage_form_cd ;
string("\\\\x01", 14) target_gpi14 ;
string("\\\\x01", 70) target_prod_name_ext ;
string("\\\\x01", 30) target_prod_short_desc ;
string("\\\\x01", 60) target_gpi14_desc ;
string("\\\\x01", 60) target_gpi8_desc ;
string("\\\\x01", 50) target_formulary_tier_desc ;
string("\\\\x01", 50) target_formulary_status_desc ;
string("\\\\x01", 1) target_pa_type_cd ;
string("\\\\x01", 1) target_step_therapy_type_cd ;
string("\\\\x01", 1000) target_step_therapy_group_name = NULL ;
decimal("\\\\x01", 0, maximum_length=39) target_step_therapy_step_num = NULL ;
string("\\\\x01", 10) formulary_cd = NULL ;
datetime("YYYY-MM-DD HH1224:MI:SS")('\\\\x01') rec_crt_ts ;
string("\\\\x01", 30) rec_crt_user_id ;
date("YYYYMMDD")('\\\\x01') last_exp_dt ;
string(1) newline = "\n" ;
end"""
      ).map(s => parse(s).asInstanceOf[FFSchemaRecord])
      var writer = in.write.format("io.prophecy.libs.FixedFileFormat")
      writer = writer.mode("overwrite")
      schema
        .map(s => Json.stringify(Json.toJson(s)))
        .foreach(schema => writer = writer.option("schema", schema))
      writer.save(Config.TARGET_LOAD_READY_FILE)
    } catch {
      case e: Error =>
        println(s"Error occurred while writing dataframe: $e")
        throw new Exception(e.getMessage)
      case e: Throwable =>
        println(s"Throwable occurred while writing dataframe: $e")
        throw new Exception(e.getMessage)
    }
  }

}
