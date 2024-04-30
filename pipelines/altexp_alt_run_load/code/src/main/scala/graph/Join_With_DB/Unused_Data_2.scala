package graph.Join_With_DB

import io.prophecy.libs._
import graph.Join_With_DB.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Unused_Data_2 {

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
decimal("\\\\x01", 0, maximum_length=10) output_profile_id ;
datetime("YYYY-MM-DD HH1224:MI:SS")('\\\\x01') run_start_ts = NULL ;
datetime("YYYY-MM-DD HH1224:MI:SS")('\\\\x01') run_complete_ts = NULL ;
decimal("\\\\x01", 0, maximum_length=39) alt_run_status_cd ;
string("\\\\x01", 1) published_ind ;
datetime("YYYY-MM-DD HH1224:MI:SS")('\\\\x01') rec_crt_ts ;
string("\\\\x01", 30) rec_crt_user_id ;
datetime("YYYY-MM-DD HH1224:MI:SS")('\\\\x01') rec_last_upd_ts ;
string("\\\\x01", 30) rec_last_upd_user_id ;
date("YYYYMMDD")('\\\\x01') run_eff_dt ;
date("YYYYMMDD")('\\\\x01') as_of_dt = NULL ;
string(1) newline = "\n" ;
end"""
      ).map(s => parse(s).asInstanceOf[FFSchemaRecord])
      var writer = in.write.format("io.prophecy.libs.FixedFileFormat")
      writer = writer.mode("overwrite")
      schema
        .map(s => Json.stringify(Json.toJson(s)))
        .foreach(schema => writer = writer.option("schema", schema))
      writer.save(Config.UNUSED_DATA_FILE_NM)
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
