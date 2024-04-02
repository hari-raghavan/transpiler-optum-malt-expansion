package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object ALT_RUN_JOB_DETAILS_load_ready {

  def apply(context: Context, in: DataFrame): Unit = {
    val Config = context.config
    import _root_.io.prophecy.abinitio.dml.DMLSchema.parse
    import _root_.io.prophecy.libs.FFSchemaRecord
    import _root_.io.prophecy.libs.FixedFormatSchemaImplicits._
    import play.api.libs.json.Json
    try {
      val schema = Some(
        """type alt_run_job_details_load_t =
              record
              decimal("\x01",0, maximum_length=16) alt_run_id /*NUMBER(15) NOT NULL*/;
              decimal("\x01",0, maximum_length=16) job_run_id /*NUMBER(15) NOT NULL*/;
              datetime("YYYY-MM-DD HH24:MI:SS")("\x01") rec_crt_ts /*TIMESTAMP(6) NOT NULL*/;
              string("\x01", maximum_length=30) rec_crt_user_id /*VARCHAR2(30) NOT NULL*/;
              string(1) newline = "\n";
              end;
              metadata type = alt_run_job_details_load_t ;"""
      ).map(s => parse(s).asInstanceOf[FFSchemaRecord])
      var writer = in.write.format("io.prophecy.libs.FixedFileFormat")
      writer = writer.mode("overwrite")
      schema
        .map(s => Json.stringify(Json.toJson(s)))
        .foreach(schema => writer = writer.option("schema", schema))
      writer.save(Config.ALT_RUN_JOB_DETAILS_FILE)
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
