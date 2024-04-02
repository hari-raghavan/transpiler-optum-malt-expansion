package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object OFIL_Step_Group_Number_Xwalk {

  def apply(context: Context, in: DataFrame): Unit = {
    val Config = context.config
    import _root_.io.prophecy.abinitio.dml.DMLSchema.parse
    import _root_.io.prophecy.libs.FFSchemaRecord
    import _root_.io.prophecy.libs.FixedFormatSchemaImplicits._
    import play.api.libs.json.Json
    try {
      val schema = Some(
        """type step_grp_num_xwalk_t =
              record
              decimal("\x01",0, maximum_length=10) tac_id /*NUMBER(9) NOT NULL*/;
              string("\x01", maximum_length=20) tac_name /*VARCHAR2(20) NOT NULL*/;
              decimal("\x01",0, maximum_length=39) priority /*NUMBER(38) NOT NULL*/;
              decimal("\x01") _target_st_grp_num = allocate();
              decimal("\x01")[int] _alt_st_grp_nums = allocate();
              end;
              metadata type = step_grp_num_xwalk_t ;"""
      ).map(s => parse(s).asInstanceOf[FFSchemaRecord])
      var writer = in.write.format("io.prophecy.libs.FixedFileFormat")
      writer = writer.mode("overwrite")
      schema
        .map(s => Json.stringify(Json.toJson(s)))
        .foreach(schema => writer = writer.option("schema", schema))
      writer.save(Config.STEP_GRP_NUM_XWALK)
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
