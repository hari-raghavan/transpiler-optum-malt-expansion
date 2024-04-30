package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object OFILE7 {

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
decimal("\\\\x01", 0, maximum_length=10) tal_assoc_dtl_id ;
decimal("\\\\x01", 0, maximum_length=10) tal_assoc_id ;
string("\\\\x01", 20) tal_assoc_name ;
string("\\\\x01", 60) tal_assoc_desc ;
decimal("\\\\x01", 0, maximum_length=39) tal_assoc_type_cd ;
string("\\\\x01", 20) target_udl_name = NULL ;
string("\\\\x01", 20) alt_udl_name = NULL ;
decimal("\\\\x01", 0, maximum_length=39) alt_rank = NULL ;
decimal("\\\\x01", 0, maximum_length=39) constituent_rank = NULL ;
string("\\\\x01", 2) constituent_group = NULL ;
string("\\\\x01", 1) constituent_reqd = NULL ;
string("\\\\x01", 30) shared_qual ;
string("\\\\x01", 20) override_tac_name = NULL ;
string("\\\\x01", 20) override_tar_name = NULL ;
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
