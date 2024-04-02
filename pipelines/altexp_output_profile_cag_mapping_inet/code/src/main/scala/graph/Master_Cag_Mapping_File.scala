package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Master_Cag_Mapping_File {

  def apply(context: Context, in: DataFrame): Unit = {
    val Config = context.config
    import _root_.io.prophecy.abinitio.dml.DMLSchema.parse
    import _root_.io.prophecy.libs.FFSchemaRecord
    import _root_.io.prophecy.libs.FixedFormatSchemaImplicits._
    import play.api.libs.json.Json
    try {
      val schema = Some(
        """type master_cag_mapping_t =
              record
              string("\x01") carrier = NULL("") /*VARCHAR2(20)*/;
              string("\x01") account = NULL("") /*VARCHAR2(20)*/;
              string("\x01") group = NULL("") /*VARCHAR2(20)*/;
              string("\x01") future_flg = 'C';
              string("\x01") cag_override_data_path= NULL("");
              decimal("\x01")[int] qual_output_profile_ids /*NUMBER(9) NOT NULL*/;
              record
              string("\x01") qual_output_profile_name /*VARCHAR2(20)*/;
              string("\x01") rxclaim_env_name;
              decimal("\x01") [int] job_ids;
              string("\x01") [int] alias_names;
              string("\x01") [int] job_names;
              string("\x01") [int] formulary_names /*VARCHAR2(20) NOT NULL*/;
              string("\x01") [int] form_override_data_paths;
              string("\x01", maximum_length=30)[int] formulary_pseudonyms;
              string("\x01", maximum_length=20) tal_name /*VARCHAR2(20) NOT NULL*/;
              string("\x01") tac_name /*VARCHAR2(20) NOT NULL*/;
              string("\x01") tar_name /*VARCHAR2(20) NOT NULL*/;
              string("\x01") tsd_name /*VARCHAR2(20) NOT NULL*/;
              string("\x01", maximum_length=1) st_tac_ind /*VARCHAR2(1) NOT NULL*/;
              end[int] op_dtls;
              record
              decimal("\x01") non_qual_op_id;
              string("\x01") rxclaim_env_name;
              decimal("\x01") [int] job_ids;
              string("\x01")[int] formulary_names;
              string("\x01", maximum_length=30)[int] formulary_pseudonyms;
              string("\x01") [int] alias_names;
              string("\x01") [int] job_names;
              //string("\x01", maximum_length=30)[int] formulary_pseudonyms;
              end[int] non_qual_output_profile_ids /*NUMBER(9) NOT NULL*/;
              string("\x01") [int] err_msgs;
              date("YYYYMMDD")("\x01") as_of_dt = NULL("");
              string(1) newline = "\n";
              end;
              metadata type = master_cag_mapping_t ;"""
      ).map(s => parse(s).asInstanceOf[FFSchemaRecord])
      var writer = in.write.format("io.prophecy.libs.FixedFileFormat")
      writer = writer.mode("overwrite")
      schema
        .map(s => Json.stringify(Json.toJson(s)))
        .foreach(schema => writer = writer.option("schema", schema))
      writer.save(Config.MASTER_REF_FILE)
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
