package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Alias_Extract {

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
          Some("""type alias_dtl_t =
                    record
                    decimal("\x01",0, maximum_length=10) alias_dtl_id ;
                    decimal("\x01",0, maximum_length=10) alias_id ;
                    string("\x01", maximum_length=20) alias_name ;
                    decimal("\x01",0, maximum_length=39) qual_priority = 99;
                    string("\x01", maximum_length=10) qual_id_type_cd = NULL("") ;
                    string("\x01", maximum_length=14) qual_id_value = NULL("") ;
                    string("\x01", maximum_length=70) search_txt ;
                    string("\x01", maximum_length=70) replace_txt = NULL("") ;
                    decimal("\x01",0, maximum_length=39) rank ;
                    date("YYYYMMDD")("\x01") eff_dt ;
                    date("YYYYMMDD")("\x01") term_dt ;
                    string(1) newline = "\n";
                    end;
                    metadata type = alias_dtl_t ;""")
            .map(s => parse(s).asInstanceOf[FFSchemaRecord])
            .map(s => Json.stringify(Json.toJson(s)))
            .getOrElse("")
        )
        .format("io.prophecy.libs.FixedFileFormat")
      df = reader.load(Config.ALIAS_EXTRACT)
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
