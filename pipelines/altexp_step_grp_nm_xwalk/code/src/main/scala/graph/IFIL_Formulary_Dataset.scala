package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object IFIL_Formulary_Dataset {

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
          Some("""type form_ovrrd_xwalk_t =
              record
              string("\x01", maximum_length=20) formulary_name /*VARCHAR2(20) NOT NULL*/;
              string("\x01", maximum_length=10) formulary_cd = "" /*VARCHAR2(10)*/;
              string("\x01", maximum_length=20)  carrier = NULL("") /*VARCHAR2(20)*/;
              string("\x01", maximum_length=20)  account = NULL("") /*VARCHAR2(20)*/;
              string("\x01", maximum_length=20)  group = NULL("") /*VARCHAR2(20)*/;
              date("YYYYMMDD")("\x01") last_exp_dt /*DATE*/;
              string("\x01", maximum_length=11) ndc11 /*VARCHAR2(11) NOT NULL*/;
              string("\x01", maximum_length=2) formulary_tier /*VARCHAR2(2) NOT NULL*/;
              string("\x01", maximum_length=2) formulary_status /*VARCHAR2(2) NOT NULL*/;
              string("\x01", maximum_length=1) pa_reqd_ind /*VARCHAR2(1) NOT NULL*/;
              string("\x01", maximum_length=1) specialty_ind /*VARCHAR2(1) NOT NULL*/;
              string("\x01", maximum_length=1) step_therapy_ind /*VARCHAR2(1) NOT NULL*/;
              string("\x01", maximum_length=50) formulary_tier_desc /*VARCHAR2(50) NOT NULL*/;
              string("\x01", maximum_length=50) formulary_status_desc /*VARCHAR2(50) NOT NULL*/;
              string("\x01", maximum_length=1) pa_type_cd /*VARCHAR2(1) NOT NULL*/;
              string("\x01", maximum_length=1) step_therapy_type_cd /*VARCHAR2(1) NOT NULL*/;
              string("\x01", maximum_length=100) step_therapy_group_name = NULL("") /*VARCHAR2(100)*/;
              decimal("\x01",0, maximum_length=39) step_therapy_step_number = NULL("") /*NUMBER(38)*/;
              string(1) newline = "\n";
              end;
              metadata type = form_ovrrd_xwalk_t ;""")
            .map(s => parse(s).asInstanceOf[FFSchemaRecord])
            .map(s => Json.stringify(Json.toJson(s)))
            .getOrElse("")
        )
        .format("io.prophecy.libs.FixedFileFormat")
      df = reader.load(Config.FORM_OVRRD_DATA_PATH)
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
