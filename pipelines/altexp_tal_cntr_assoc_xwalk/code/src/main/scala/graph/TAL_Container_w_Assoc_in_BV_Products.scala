package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object TAL_Container_w_Assoc_in_BV_Products {

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
decimal("\\\\x01", 0, maximum_length=10) tal_id ;
string("\\\\x01", 20) tal_name ;
string("\\\\x01", 20) tal_assoc_name = NULL ;
string("\\\\x01", 20) tar_udl_nm = NULL ;
string("\\\\x01", 60) tal_desc ;
decimal("\\\\x01", 6, maximum_length=39) priority ;
decimal("\\\\x01", 0, maximum_length=39) tal_assoc_type_cd ;
bit_vector_t[int] target_prdcts ;
record
bit_vector_t alt_prdcts ;
string(2) constituent_group = NULL ;
string(1) constituent_reqd = NULL ;
string(20) udl_nm = NULL ;
end[int] alt_constituent_prdcts ;
string("\\\\x01", 30) shared_qual ;
string("\\\\x01", 20) override_tac_name = NULL ;
string("\\\\x01", 20) override_tar_name = NULL ;
string(2)[int] constituent_grp_vec ;
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
