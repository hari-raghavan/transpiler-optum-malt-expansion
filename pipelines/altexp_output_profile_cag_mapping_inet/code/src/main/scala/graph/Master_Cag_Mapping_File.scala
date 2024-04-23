package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Master_Cag_Mapping_File {

  def apply(context: Context, in: DataFrame): Unit = {
    val Config = context.config
    import _root_.io.prophecy.abinitio.dml.DMLSchema.parse
    import _root_.io.prophecy.libs.FFSchemaRecord
    import _root_.io.prophecy.libs.FixedFormatSchemaImplicits._
    import play.api.libs.json.Json
    try {
      val schema = Some("""
record
string("\\\\x01") carrier = NULL ;
string("\\\\x01") account = NULL ;
string("\\\\x01") group = NULL ;
string("\\\\x01") cag_override_data_path = NULL ;
decimal("\\\\x01")[int] output_profile_id ;
record
string("\\\\x01") output_profile_name ;
string("\\\\x01")[int] alias_names ;
string("\\\\x01")[int] job_names ;
string("\\\\x01")[int] formulary_names ;
string("\\\\x01")[int] form_override_data_paths ;
string("\\\\x01")[int] formulary_pseudonyms ;
string("\\\\x01") tal_name ;
string("\\\\x01") tac_name ;
string("\\\\x01") tar_name ;
string("\\\\x01") tsd_name ;
string("\\\\x01", 1) st_tac_ind ;
end[int] op_dtls ;
string("\\\\x01")[int] err_msgs ;
string(1) newline = "\n" ;
end""").map(s => parse(s).asInstanceOf[FFSchemaRecord])
      var writer = in.write.format("io.prophecy.libs.FixedFileFormat")
      writer = writer.mode("overwrite")
      schema
        .map(s => Json.stringify(Json.toJson(s)))
        .foreach(schema => writer = writer.option("schema", schema))
      writer.save(Config.MASTER_REF_FILE)
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
