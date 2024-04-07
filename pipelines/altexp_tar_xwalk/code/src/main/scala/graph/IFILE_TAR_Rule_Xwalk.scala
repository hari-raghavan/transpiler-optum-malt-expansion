package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object IFILE_TAR_Rule_Xwalk {

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
          Some(
            """
record
decimal("\\\\x01", 0, maximum_length=10) tar_id ;
decimal("\\\\x01", 0, maximum_length=10) tar_dtl_id ;
string("\\\\x01", 20) tar_name ;
decimal("\\\\x01", 0, maximum_length=10) tar_roa_df_set_id = NULL ;
decimal("\\\\x01", 0, maximum_length=39) tar_dtl_type_cd ;
decimal("\\\\x01", 0, maximum_length=39) priority ;
string("\\\\x01", 1) sort_ind ;
string("\\\\x01", 1) filter_ind ;
string("\\\\x01", 15) rebate_elig_cd = NULL ;
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
end"""
          ).map(s => parse(s).asInstanceOf[FFSchemaRecord])
            .map(s => Json.stringify(Json.toJson(s)))
            .getOrElse("")
        )
        .format("io.prophecy.libs.FixedFileFormat")
      df = reader.load(Config.TAR_RULE_XWALK)
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
