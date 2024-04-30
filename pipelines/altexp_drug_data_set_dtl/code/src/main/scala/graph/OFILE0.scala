package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object OFILE0 {

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
string("\\\\x01", 20) rxclaim_env_name ;
string("\\\\x01", 20) carrier = NULL ;
string("\\\\x01", 20) account = NULL ;
string("\\\\x01", 20) group = NULL ;
date("YYYYMMDD")('\\\\x01') run_eff_dt = NULL ;
decimal("\\\\x01", 0, maximum_length=10) drug_data_set_dtl_id ;
decimal("\\\\x01", 0, maximum_length=10) drug_data_set_id ;
string("\\\\x01", 11) ndc11 ;
string("\\\\x01", 14) gpi14 ;
string("\\\\x01", 1) status_cd ;
string("\\\\x01", 8) eff_dt = NULL ;
string("\\\\x01", 8) term_dt = NULL ;
string("\\\\x01", 8) inactive_dt = NULL ;
string("\\\\x01", 1) msc ;
string("\\\\x01", 70) drug_name ;
string("\\\\x01", 30) prod_short_desc ;
string("\\\\x01", 3) rx_otc ;
string("\\\\x01", 1) rx_otc_cd ;
string("\\\\x01", 1) desi ;
string("\\\\x01", 2) roa_cd ;
string("\\\\x01", 4) dosage_form_cd ;
decimal("\\\\x01", 5, maximum_length=14) prod_strength ;
string("\\\\x01", 1) repack_cd ;
string("\\\\x01", 60) gpi14_desc ;
string("\\\\x01", 60) gpi8_desc ;
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
