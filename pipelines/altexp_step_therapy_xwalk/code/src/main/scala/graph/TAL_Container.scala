package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object TAL_Container {

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
            .map(s => Json.stringify(Json.toJson(s)))
            .getOrElse("")
        )
        .format("io.prophecy.libs.FixedFileFormat")
      df = reader.load(Config.CAG_TAL_CONTAINER_FILE)
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
