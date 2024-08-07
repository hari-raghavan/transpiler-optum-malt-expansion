package io.prophecy.pipelines.altexp_tar_tax_xml_rule_parser_2.graph

import io.prophecy.libs._
import io.prophecy.pipelines.altexp_tar_tax_xml_rule_parser_2.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object OFILE_Rule_Xwalk {

  def apply(context: Context, in: DataFrame): Unit = {
    val Config = context.config
    import _root_.io.prophecy.abinitio.dml.DMLSchema.parse
    import _root_.io.prophecy.libs.FFSchemaRecord
    import _root_.io.prophecy.libs.FixedFormatSchemaImplicits._
    import play.api.libs.json.Json
    try {
      val schema = Some("""
record
decimal("\\\\x01", 0, maximum_length=10) tac_id ;
string("\\\\x01", 20) tac_name ;
decimal("\\\\x01", 0, maximum_length=39) priority ;
date("YYYYMMDD")('\\\\x01') eff_dt ;
date("YYYYMMDD")('\\\\x01') term_dt ;
record
string(integer(4)) qualifier_cd ;
string(integer(4)) operator ;
string(integer(4)) compare_value ;
string(1) conjunction_cd ;
end[int] target_rule_def ;
record
string(integer(4)) qualifier_cd ;
string(integer(4)) operator ;
string(integer(4)) compare_value ;
string(1) conjunction_cd ;
end[int] alt_rule_def ;
string(1) newline = "\n" ;
end""").map(s => parse(s).asInstanceOf[FFSchemaRecord])
      var writer = in.write.format("io.prophecy.libs.FixedFileFormat")
      writer = writer.mode("overwrite")
      schema
        .map(s => Json.stringify(Json.toJson(s)))
        .foreach(schema => writer = writer.option("schema", schema))
      writer.save(Config.RULE_XWALK)
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
