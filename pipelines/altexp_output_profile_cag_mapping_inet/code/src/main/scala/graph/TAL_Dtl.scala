package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object TAL_Dtl {

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
          Some("""type tal_dtl_t =
        record
        decimal("\x01",0, maximum_length=10) tal_dtl_id /*NUMBER(9) NOT NULL*/;
        decimal("\x01",0, maximum_length=10) tal_id /*NUMBER(9) NOT NULL*/;
        string("\x01", maximum_length=20) tal_name /*VARCHAR2(20) NOT NULL*/;
        string("\x01", maximum_length=60) tal_desc /*VARCHAR2(60) NOT NULL*/;
        decimal("\x01",0, maximum_length=39) tal_dtl_type_cd /*NUMBER(38) NOT NULL*/;
        string("\x01", maximum_length=20) nested_tal_name = NULL("") /*VARCHAR2(20)*/;
        string("\x01", maximum_length=20) tal_assoc_name = NULL("") /*VARCHAR2(20)*/;
        decimal("\x01", 6, maximum_length=39) priority /*NUMBER(38) NOT NULL*/;
        date("YYYYMMDD")("\x01") eff_dt /*DATE NOT NULL*/;
        date("YYYYMMDD")("\x01") term_dt /*DATE NOT NULL*/;
        string(1) newline = "\n";
        end;
        metadata type = tal_dtl_t ;""")
            .map(s => parse(s).asInstanceOf[FFSchemaRecord])
            .map(s => Json.stringify(Json.toJson(s)))
            .getOrElse("")
        )
        .format("io.prophecy.libs.FixedFileFormat")
      df = reader.load(Config.TAL_DTL_LKP_FILE)
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
