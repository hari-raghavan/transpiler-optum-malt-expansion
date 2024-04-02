package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Output_Profile_Extract {

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
          Some("""type output_profile_t =
              record
              decimal("\x01",0, maximum_length=10) output_profile_id /*NUMBER(9) NOT NULL*/;
              string("\x01", maximum_length=20) rxclaim_env_name /*VARCHAR2(20) NOT NULL*/;
              string("\x01", maximum_length=20) formulary_name /*VARCHAR2(20) NOT NULL*/;
              string("\x01", maximum_length=20) formulary_id = NULL("") /*VARCHAR2(20)*/;
              decimal("\x01",0, maximum_length=10) output_profile_form_dtl_id /*NUMBER(9) NOT NULL*/;
              decimal("\x01",0, maximum_length=10) output_profile_job_dtl_id /*NUMBER(9) NOT NULL*/;
              string("\x01", maximum_length=20) output_profile_name /*VARCHAR2(20)*/;
              string("\x01", maximum_length=20) alias_name = NULL("") /*VARCHAR2(20) NOT NULL*/;
              decimal("\x01",0, maximum_length=39) alias_priority = NULL /*NUMBER(38) NOT NULL*/;
              string("\x01", maximum_length=20) carrier = NULL("") /*VARCHAR2(20)*/;
              string("\x01", maximum_length=20) account = NULL("") /*VARCHAR2(20)*/;
              string("\x01", maximum_length=20) group = NULL("") /*VARCHAR2(20)*/;
              string("\x01", maximum_length=20) tal_name /*VARCHAR2(20) NOT NULL*/;
              string("\x01", maximum_length=20) tac_name /*VARCHAR2(20) NOT NULL*/;
              string("\x01", maximum_length=20) tar_name /*VARCHAR2(20) NOT NULL*/;
              string("\x01", maximum_length=20) tsd_name /*VARCHAR2(20) NOT NULL*/;
              decimal("\x01",0, maximum_length=10) job_id /*NUMBER(9) NOT NULL*/;
              string("\x01", maximum_length=20) job_name /*VARCHAR2(20) NOT NULL*/;
              string("\x01", maximum_length=20) customer_name = NULL("")/*VARCHAR2(20) NULL*/;
              decimal("\x01",0, maximum_length=39) run_day = NULL("") /*NUMBER(38)*/;
              string("\x01", maximum_length=20) lob_name = NULL("") /*VARCHAR2(20)*/;
              string("\x01", maximum_length=4) run_jan1_start_mmdd = NULL("") /*VARCHAR2(4)*/;
              string("\x01", maximum_length=4) run_jan1_end_mmdd = NULL("") /*VARCHAR2(4)*/;
              decimal("\x01") future_flg = 0;
              string("\x01", maximum_length=30) formulary_pseudonym = NULL("") /*VARCHAR2(30)*/;
              decimal("\x01",0, maximum_length=10) notes_id = NULL("") /*NUMBER(9)*/;
              string("\x01", maximum_length=60) output_profile_desc /*VARCHAR2(60) NOT NULL*/;
              decimal("\x01",0, maximum_length=39 ) formulary_option_cd /*NUMBER(38) NOT NULL*/;
              string("\x01", maximum_length=20) layout_name /*VARCHAR2(20) NOT NULL*/;
              date("YYYYMMDD")("\x01") as_of_dt = NULL("") /*DATE*/;
              string("\x01", maximum_length=1) st_tac_ind /*VARCHAR2(1) NOT NULL*/;
              string(1) newline = "\n";
              end;
              metadata type = output_profile_t ;""")
            .map(s => parse(s).asInstanceOf[FFSchemaRecord])
            .map(s => Json.stringify(Json.toJson(s)))
            .getOrElse("")
        )
        .format("io.prophecy.libs.FixedFileFormat")
      df = reader.load(Config.OUTPUT_PROFILE_FILE)
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
