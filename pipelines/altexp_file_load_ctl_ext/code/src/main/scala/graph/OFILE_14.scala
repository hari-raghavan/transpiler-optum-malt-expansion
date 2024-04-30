package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object OFILE_14 {

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
decimal("\\\\x01", 0, maximum_length=16) file_load_cntl_id ;
string("\\\\x01", 4000) component_ids = NULL ;
date("YYYYMMDD")('\\\\x01') as_of_date ;
string("\\\\x01", 20) rxclaim_env_name = NULL ;
string("\\\\x01", 20) carrier = NULL ;
string("\\\\x01", 20) account = NULL ;
string("\\\\x01", 20) group = NULL ;
decimal("\\\\x01", 0, maximum_length=39) component_type_cd = NULL ;
string("\\\\x01", 512) file_name_w = NULL ;
string("\\\\x01", 1) published_ind ;
decimal("\\\\x01", 0, maximum_length=16) alt_run_id = NULL ;
string("\\\\x01", 512) report_file_name = NULL ;
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
