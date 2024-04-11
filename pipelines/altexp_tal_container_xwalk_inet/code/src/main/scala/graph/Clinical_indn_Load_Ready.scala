package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Clinical_indn_Load_Ready {

  def apply(context: Context, in: DataFrame): Unit = {
    val Config = context.config
    import _root_.io.prophecy.abinitio.dml.DMLSchema.parse
    import _root_.io.prophecy.libs.FFSchemaRecord
    import _root_.io.prophecy.libs.FixedFormatSchemaImplicits._
    import play.api.libs.json.Json
    try {
      val schema = Some(
        """
record
decimal("\\\\x01", 0, maximum_length=16) alt_run_id = -1 ;
decimal("\\\\x01", 0, maximum_length=16) alt_run_target_dtl_id = -1 ;
string("\\\\x01", 20) formulary_name ;
string("\\\\x01", 11) target_ndc ;
string("\\\\x01", 40) tal_assoc_name ;
decimal("\\\\x01", 0, maximum_length=39) rank ;
datetime("YYYY-MM-DD HH1224:MI:SS")('\\\\x01') rec_crt_ts ;
string("\\\\x01", 30) rec_crt_user_id ;
string("\\\\x01", 200) clinical_indn_desc = NULL ;
string(1) newline = "\n" ;
end"""
      ).map(s => parse(s).asInstanceOf[FFSchemaRecord])
      var writer = in.write.format("io.prophecy.libs.FixedFileFormat")
      writer = writer.mode("overwrite")
      schema
        .map(s => Json.stringify(Json.toJson(s)))
        .foreach(schema => writer = writer.option("schema", schema))
      writer.save(Config.ALT_CLINC_LOAD_READY_FILE)
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
