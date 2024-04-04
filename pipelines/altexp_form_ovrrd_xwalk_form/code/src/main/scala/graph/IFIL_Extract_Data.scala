package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object IFIL_Extract_Data {

  def apply(context: Context): DataFrame = {
    val spark  = context.spark
    val Config = context.config
    import _root_.io.prophecy.abinitio.dml.DMLSchema.parse
    import _root_.io.prophecy.libs.FFSchemaRecord
    import _root_.io.prophecy.libs.FixedFormatSchemaImplicits._
    import play.api.libs.json.Json
    var df: DataFrame = spark.emptyDataFrame
    try {
      var reader = spark.read
        .option(
          "schema",
          Some("""
record
decimal("\\\\x01", 0, maximum_length=10) formulary_data_set_id ;
string("\\\\x01", 20) formulary_name ;
string("\\\\x01", 20) formulary_id = NULL ;
string("\\\\x01", 10) formulary_cd = "" ;
string("\\\\x01", 20) carrier = NULL ;
string("\\\\x01", 20) account = NULL ;
string("\\\\x01", 20) group = NULL ;
string("\\\\x01", 20) rxclaim_env_name ;
string("\\\\x01", 20) customer_name = NULL ;
date("YYYYMMDD")('\\\\x01') last_exp_dt ;
date("YYYYMMDD")('\\\\x01') run_eff_dt = NULL ;
decimal("\\\\x01", 0, maximum_length=10) formulary_data_set_dtl_id ;
string("\\\\x01", 11) ndc11 ;
string("\\\\x01", 2) formulary_tier ;
string("\\\\x01", 2) formulary_status ;
string("\\\\x01", 1) pa_reqd_ind ;
string("\\\\x01", 1) specialty_ind ;
string("\\\\x01", 1) step_therapy_ind ;
string("\\\\x01", 50) formulary_tier_desc ;
string("\\\\x01", 50) formulary_status_desc ;
string("\\\\x01", 1) pa_type_cd ;
string("\\\\x01", 1) step_therapy_type_cd ;
string("\\\\x01", 100) step_therapy_group_name = NULL ;
decimal("\\\\x01", 0, maximum_length=39) step_therapy_step_number = NULL ;
string(1) newline = "\n" ;
end""").map(s => parse(s).asInstanceOf[FFSchemaRecord])
            .map(s => Json.stringify(Json.toJson(s)))
            .getOrElse("")
        )
        .format("io.prophecy.libs.FixedFileFormat")
      df = reader.load(Config.EXTRACT_FILE)
    } catch {
      case e: Error =>
        println(s"Error occurred while reading dataframe: $e")
        throw new Exception(e.getMessage)
      case e: Throwable =>
        println(s"Throwable occurred while reading dataframe: $e")
        throw new Exception(e.getMessage)
    }
    df
  }

}
