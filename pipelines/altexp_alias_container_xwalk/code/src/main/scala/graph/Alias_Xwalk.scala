package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Alias_Xwalk {

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
          Some("""type alias_info_t =  record
    decimal("\x01",0, maximum_length=39) qual_priority = 99/*NUMBER(38) NOT NULL*/;
    string("\x01", maximum_length=14) qual_id_value = NULL("") /*VARCHAR2(14)*/;
    string("\x01", maximum_length=70) search_txt /*VARCHAR2(70) NOT NULL*/;
    string("\x01", maximum_length=70) replace_txt  /*VARCHAR2(70)*/;
    date("YYYYMMDD")("\x01") eff_dt/*DATE*/;
    date("YYYYMMDD")("\x01") term_dt/*DATE*/;
    end;

    //type definition for alias xwalk process
    type alias_xwalk_t =
    record
    decimal("\x01",0, maximum_length=10) alias_id /*NUMBER(9) NOT NULL*/;
    string("\x01", maximum_length=20) alias_name /*VARCHAR2(20) NOT NULL*/;
    alias_info_t[int] alias_info;
    end;
    metadata type = alias_xwalk_t ;""")
            .map(s => parse(s).asInstanceOf[FFSchemaRecord])
            .map(s => Json.stringify(Json.toJson(s)))
            .getOrElse("")
        )
        .format("io.prophecy.libs.FixedFileFormat")
      df = reader.load(Config.ALIAS_XWALK_FILE)
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
