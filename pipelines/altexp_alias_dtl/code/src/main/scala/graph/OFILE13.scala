package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object OFILE13 {

  def apply(context: Context, in: DataFrame): Unit = {
    val Config = context.config
    import _root_.io.prophecy.abinitio.dml.DMLSchema.parse
    import _root_.io.prophecy.libs.FFSchemaRecord
    import _root_.io.prophecy.libs.FixedFormatSchemaImplicits._
    import play.api.libs.json.Json
    try {
      val schema =
        Some("""
record
decimal("\\\\x01", 0, maximum_length=10) alias_dtl_id ;
decimal("\\\\x01", 0, maximum_length=10) alias_id ;
string("\\\\x01", 20) alias_name ;
decimal("\\\\x01", 0, maximum_length=39) qual_priority = 99 ;
string("\\\\x01", 10) qual_id_type_cd = NULL ;
string("\\\\x01", 14) qual_id_value = NULL ;
string("\\\\x01", 70) search_txt ;
string("\\\\x01", 70) replace_txt = NULL ;
decimal("\\\\x01", 0, maximum_length=39) rank ;
date("YYYYMMDD")('\\\\x01') eff_dt ;
date("YYYYMMDD")('\\\\x01') term_dt ;
string(1) newline = "\n" ;
end""").map(s => parse(s).asInstanceOf[FFSchemaRecord])
      var writer = in.write.format("io.prophecy.libs.FixedFileFormat")
      writer = writer.mode("overwrite")
      schema
        .map(s => Json.stringify(Json.toJson(s)))
        .foreach(schema => writer = writer.option("schema", schema))
      writer.save(Config.OUTPUT_FILE)
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
