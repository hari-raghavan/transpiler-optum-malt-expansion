package graph.TAL_Container_Assoc

import io.prophecy.libs._
import graph.TAL_Container_Assoc.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object IFILE_TAL_DTL_XWALK {

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
decimal("\\\\x01", 0, maximum_length=10) tal_dtl_id ;
decimal("\\\\x01", 0, maximum_length=10) tal_id ;
string("\\\\x01", 20) tal_name ;
string("\\\\x01", 60) tal_desc ;
decimal("\\\\x01", 0, maximum_length=39) tal_dtl_type_cd ;
string("\\\\x01", 20) nested_tal_name = NULL ;
string("\\\\x01", 20) tal_assoc_name = NULL ;
decimal("\\\\x01", 6, maximum_length=39) priority ;
date("YYYYMMDD")('\\\\x01') eff_dt ;
date("YYYYMMDD")('\\\\x01') term_dt ;
string(1) newline = "\n" ;
end""").map(s => parse(s).asInstanceOf[FFSchemaRecord])
            .map(s => Json.stringify(Json.toJson(s)))
            .getOrElse("")
        )
        .format("io.prophecy.libs.FixedFileFormat")
      df = reader.load(Config.TAL_XWALK)
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
