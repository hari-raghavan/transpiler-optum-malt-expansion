package graph.TAL_Container_Assoc.Expand_TAL_Data_At_UDL_level

import io.prophecy.libs._
import graph.TAL_Container_Assoc.Expand_TAL_Data_At_UDL_level.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object OFILE_Normaize_TAL_data_UDL_level {

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
string("\\\\x01", 20) tal_id ;
string("\\\\x01", 20) tal_assoc_id = NULL ;
string("\\\\x01", 2000) clinical_indn_desc = NULL ;
string("\\\\x01", 60) tal_desc ;
string("\\\\x01", 60) tal_assoc_desc ;
decimal("\\\\x01", 0, maximum_length=39) tal_assoc_type_cd ;
decimal("\\\\x01", 6, maximum_length=39) tal_assoc_rank = NULL ;
string("\\\\x01", 20) udl_id = NULL ;
string("\\\\x01", 60) udl_desc ;
string("\\\\x01", 20) Target_Alternative ;
bit_vector_t products ;
decimal("\\\\x01", 0, maximum_length=39) alt_rank = NULL ;
string("\\\\x01", 30) shared_qual ;
string("\\\\x01", 20) override_tac_name = NULL ;
string("\\\\x01", 20) override_tar_name = NULL ;
string(2) constituent_group = NULL ;
string(1) constituent_reqd = NULL ;
decimal("\\\\x01", 0, maximum_length=39) constituent_rank = NULL ;
string(1) newline = "\n" ;
end""").map(s => parse(s).asInstanceOf[FFSchemaRecord])
      var writer = in.write.format("io.prophecy.libs.FixedFileFormat")
      writer = writer.mode("overwrite")
      schema
        .map(s => Json.stringify(Json.toJson(s)))
        .foreach(schema => writer = writer.option("schema", schema))
      writer.save(Config.CAG_TAL_CONTAINER_FILE)
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
