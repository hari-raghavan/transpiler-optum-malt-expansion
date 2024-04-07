package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object OFILE_TAR_Expansion_Xwalk {

  def apply(context: Context, in: DataFrame): Unit = {
    val Config = context.config
    import _root_.io.prophecy.abinitio.dml.DMLSchema.parse
    import _root_.io.prophecy.libs.FFSchemaRecord
    import _root_.io.prophecy.libs.FixedFormatSchemaImplicits._
    import play.api.libs.json.Json
    try {
      val schema = Some("""type tar_xwalk_t =
record
decimal("\x01",0, maximum_length=10) tar_id;
decimal("\x01",0, maximum_length=10) tar_dtl_id;
string("\x01", maximum_length=20) tar_name;
decimal("\x01",0, maximum_length=39) tar_dtl_type_cd;
record
bit_vector_t[int] target_prdcts;
record
bit_vector_t[int] alt_prdcts;
bit_vector_t[int][int] alt_prdcts_all_prio;
bit_vector_t common_prdcts;
end[int] alt_contents;
end[int] contents;
bit_vector_t common_alt_prdcts;
bit_vector_t common_target_prdcts;
decimal(1) keep_all_targets = 0;
string(1) newline = '\n';
end;
metadata type = tar_xwalk_t;""").map(s => parse(s).asInstanceOf[FFSchemaRecord])
      var writer = in.write.format("io.prophecy.libs.FixedFileFormat")
      writer = writer.mode("overwrite")
      schema
        .map(s => Json.stringify(Json.toJson(s)))
        .foreach(schema => writer = writer.option("schema", schema))
      writer.save(Config.TAR_EXP_XWALK)
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
