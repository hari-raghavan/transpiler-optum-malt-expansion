package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object LKP_TAR_Exp {

  def apply(context: Context): DataFrame = {
    val spark = context.spark
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
decimal("\\\\x01", 0, maximum_length=10) tar_id ;
decimal("\\\\x01", 0, maximum_length=10) tar_dtl_id ;
string("\\\\x01", 20) tar_name ;
decimal("\\\\x01", 0, maximum_length=39) tar_dtl_type_cd ;
record
bit_vector_t[int]  target_prdcts ;
record
bit_vector_t[int] alt_prdcts ;
bit_vector_t[int][int] alt_prdcts_all_prio ;
bit_vector_t common_prdcts ;
end[int] alt_contents ;
end[int] contents ;
bit_vector_t common_alt_prdcts ;
bit_vector_t common_target_prdcts ;
decimal(1) keep_all_targets = 0 ;
string(1) newline = "\n" ;
end""").map(s => parse(s).asInstanceOf[FFSchemaRecord])
            .map(s => Json.stringify(Json.toJson(s)))
            .getOrElse("")
        )
        .format("io.prophecy.libs.FixedFileFormat")
      df = reader.load("/~null")
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
