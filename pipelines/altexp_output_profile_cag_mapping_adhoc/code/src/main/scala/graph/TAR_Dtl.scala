package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object TAR_Dtl {

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
end"""
          ).map(s => parse(s).asInstanceOf[FFSchemaRecord])
            .map(s => Json.stringify(Json.toJson(s)))
            .getOrElse("")
        )
        .format("io.prophecy.libs.FixedFileFormat")
      df = reader.load(Config.TAR_DTL_LKP_FILE)
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
