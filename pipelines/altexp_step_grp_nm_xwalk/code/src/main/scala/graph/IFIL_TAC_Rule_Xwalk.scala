package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object IFIL_TAC_Rule_Xwalk {

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
          Some("""type tac_rule_xwalk_t =
              record
              decimal("\x01",0, maximum_length=10) tac_id /*NUMBER(9) NOT NULL*/;
              string("\x01", maximum_length=20) tac_name /*VARCHAR2(20) NOT NULL*/;
              decimal("\x01",0, maximum_length=39) priority /*NUMBER(38) NOT NULL*/;
              date("YYYYMMDD")("\x01") eff_dt /*DATE NOT NULL*/;
              date("YYYYMMDD")("\x01") term_dt /*DATE NOT NULL*/;
              record
              string(int) qualifier_cd;
              string(int) operator;
              string(int) compare_value;
              string(1) conjunction_cd;
              end[int] target_rule_def;
              record
              string(int) qualifier_cd;
              string(int) operator;
              string(int) compare_value;
              string(1) conjunction_cd;
              end[int] alt_rule_def;
              string(1) newline = "\n";
              end;
              metadata type = tac_rule_xwalk_t ;""")
            .map(s => parse(s).asInstanceOf[FFSchemaRecord])
            .map(s => Json.stringify(Json.toJson(s)))
            .getOrElse("")
        )
        .format("io.prophecy.libs.FixedFileFormat")
      df = reader.load(Config.TAC_RULE_XWALK)
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
