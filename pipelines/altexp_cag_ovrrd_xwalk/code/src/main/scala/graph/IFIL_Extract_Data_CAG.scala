package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object IFIL_Extract_Data_CAG {

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
          Some("""record
  string("\\\\x01", maximum_length=20) rxclaim_env_name ;
  string("\\\\x01", maximum_length=20) carrier = NULL("") ;
  string("\\\\x01", maximum_length=20) account = NULL("") ;
  string("\\\\x01", maximum_length=20) group = NULL("") ;
  date("YYYYMMDD")("\\\\x01") run_eff_dt = NULL("") ;
  decimal("\\\\x01",0, maximum_length=10) drug_data_set_dtl_id ;
  decimal("\\\\x01",0, maximum_length=10) drug_data_set_id ;
  string("\\\\x01", maximum_length=11) ndc11 ;
  string("\\\\x01", maximum_length=14) gpi14 ;
  string("\\\\x01", maximum_length=1) status_cd ;
  string("\\\\x01", maximum_length=8) eff_dt = NULL("") ;
  string("\\\\x01", maximum_length=8) term_dt = NULL("") ;
  string("\\\\x01", maximum_length=8) inactive_dt = NULL("") ;
  string("\\\\x01", maximum_length=1) msc ;
  string("\\\\x01", maximum_length=70) drug_name ;
  string("\\\\x01", maximum_length=30) prod_short_desc ;   
  string("\\\\x01", maximum_length=3) rx_otc ;
  string("\\\\x01", maximum_length=1) rx_otc_cd ;
  string("\\\\x01", maximum_length=1) desi ;
  string("\\\\x01", maximum_length=2) roa_cd ;
  string("\\\\x01", maximum_length=4) dosage_form_cd ;
  decimal("\\\\x01".5, maximum_length=15) prod_strength ;
  string("\\\\x01", maximum_length=1) repack_cd ;   
  string("\\\\x01", maximum_length=60) gpi14_desc ;
  string("\\\\x01", maximum_length=60) gpi8_desc ;
  string(1) newline = "\n";
end;""").map(s => parse(s).asInstanceOf[FFSchemaRecord])
            .map(s => Json.stringify(Json.toJson(s)))
            .getOrElse("")
        )
        .format("io.prophecy.libs.FixedFileFormat")
      df = reader.load(Config.EXTRACT_FILE)
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
