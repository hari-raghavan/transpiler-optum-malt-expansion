package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object LKP_Lookup13 {

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
decimal("\\\\x01", 0, maximum_length=10) output_profile_id ;
string("\\\\x01", 20) rxclaim_env_name ;
string("\\\\x01", 20) formulary_name ;
string("\\\\x01", 20) formulary_id = NULL ;
decimal("\\\\x01", 0, maximum_length=10) output_profile_form_dtl_id ;
decimal("\\\\x01", 0, maximum_length=10) output_profile_job_dtl_id ;
string("\\\\x01", 20) output_profile_name ;
string("\\\\x01", 20) alias_name = NULL ;
decimal("\\\\x01", 0, maximum_length=39) alias_priority = NULL ;
string("\\\\x01", 20) carrier = NULL ;
string("\\\\x01", 20) account = NULL ;
string("\\\\x01", 20) group = NULL ;
string("\\\\x01", 20) tal_name ;
string("\\\\x01", 20) tac_name ;
string("\\\\x01", 20) tar_name ;
string("\\\\x01", 20) tsd_name ;
decimal("\\\\x01", 0, maximum_length=10) job_id ;
string("\\\\x01", 20) job_name ;
string("\\\\x01", 20) customer_name = NULL ;
decimal("\\\\x01", 0, maximum_length=39) run_day = NULL ;
string("\\\\x01", 20) lob_name = NULL ;
string("\\\\x01", 4) run_jan1_start_mmdd = NULL ;
string("\\\\x01", 4) run_jan1_end_mmdd = NULL ;
decimal("\\\\x01") future_flg = 0 ;
string("\\\\x01", 30) formulary_pseudonym = NULL ;
decimal("\\\\x01", 0, maximum_length=10) notes_id = NULL ;
string("\\\\x01", 60) output_profile_desc ;
decimal("\\\\x01", 0, maximum_length=39) formulary_option_cd ;
string("\\\\x01", 20) layout_name ;
date("YYYYMMDD")('\\\\x01') as_of_dt = NULL ;
string("\\\\x01", 1) st_tac_ind ;
string(1) newline = "\n" ;
end""").map(s => parse(s).asInstanceOf[FFSchemaRecord])
            .map(s => Json.stringify(Json.toJson(s)))
            .getOrElse("")
        )
        .format("io.prophecy.libs.FixedFileFormat")
      df = reader.load(Config.LKP_FILE)
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
