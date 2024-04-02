package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Formulary_Override_Ref {

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
          Some("""type form_ovrrd_ref_file_t =
        record
        string("\x01", maximum_length=20) formulary_name /*VARCHAR2(20) NOT NULL*/;
        string("\x01", maximum_length=20)  carrier = NULL("") /*VARCHAR2(20)*/;
        string("\x01", maximum_length=20)  account = NULL("") /*VARCHAR2(20)*/;
        string("\x01", maximum_length=20)  group = NULL("") /*VARCHAR2(20)*/;
        string("\x01", maximum_length=20) customer_name = NULL("")/*VARCHAR2(20)  NULL*/;
        decimal("\x01",0, maximum_length=1) is_future_snap = 0;
        string("\x01") data_path;
        date("YYYYMMDD")("\x01") run_eff_dt = NULL("") /*DATE*/;
        string(1) newline = "\n";
        end;
        metadata type = form_ovrrd_ref_file_t ;""")
            .map(s => parse(s).asInstanceOf[FFSchemaRecord])
            .map(s => Json.stringify(Json.toJson(s)))
            .getOrElse("")
        )
        .format("io.prophecy.libs.FixedFileFormat")
      df = reader.load(Config.FORM_REF_FILE)
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
