package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object REJECT_FILE9 {

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
decimal("\\\\x01", 0, maximum_length=10) tar_id ;
decimal("\\\\x01", 0, maximum_length=10) tar_dtl_id ;
string("\\\\x01", 20) tar_name ;
string("\\\\x01", 1) sort_ind ;
string("\\\\x01", 1) filter_ind ;
decimal("\\\\x01", 0, maximum_length=39) priority ;
decimal("\\\\x01", 0, maximum_length=39) tar_dtl_type_cd ;
decimal("\\\\x01", 0, maximum_length=10) tar_roa_df_set_id = NULL ;
string("\\\\x01", 4000) target_rule = NULL ;
string("\\\\x01", 4000) alt_rule = NULL ;
string("\\\\x01", 15) rebate_elig_cd = NULL ;
date("YYYYMMDD")('\\\\x01') eff_dt ;
date("YYYYMMDD")('\\\\x01') term_dt ;
string(1) newline = "\n" ;
end""").map(s => parse(s).asInstanceOf[FFSchemaRecord])
      var writer = in.write.format("io.prophecy.libs.FixedFileFormat")
      writer = writer.mode("overwrite")
      schema
        .map(s => Json.stringify(Json.toJson(s)))
        .foreach(schema => writer = writer.option("schema", schema))
      writer.save(Config.REJECT_FILE)
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
