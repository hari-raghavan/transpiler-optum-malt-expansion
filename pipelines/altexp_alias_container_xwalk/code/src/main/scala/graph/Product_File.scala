package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Product_File {

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
          Some("""type product_lkp_t =
          record
          int dl_bit;
          string("\x01", maximum_length=11)  ndc11 /*VARCHAR2(11) NOT NULL*/;
          string("\x01", maximum_length=14)  gpi14 /*VARCHAR2(14) NOT NULL*/;
          string("\x01", maximum_length=1)  status_cd /*VARCHAR2(1) NOT NULL*/;
          string("\x01", maximum_length=8) inactive_dt /*NUMBER(38) NOT NULL*/;
          string("\x01", maximum_length=1)  msc /*VARCHAR2(1) NOT NULL*/;
          string("\x01", maximum_length=70)  drug_name /*VARCHAR2(70) NOT NULL*/;
          string("\x01", maximum_length=3)  rx_otc /*VARCHAR2(3) NOT NULL*/;
          string("\x01", maximum_length=1)  desi /*VARCHAR2(1) NOT NULL*/;
          string("\x01", maximum_length=2)  roa_cd /*VARCHAR2(2) NOT NULL*/;
          string("\x01", maximum_length=4)  dosage_form_cd /*VARCHAR2(4) NOT NULL*/;
          decimal("\x01".5, maximum_length=15) prod_strength /*NUMBER(13,5) NOT NULL*/;
          string("\x01", maximum_length=1)  repack_cd /*VARCHAR2(1) NOT NULL*/;
          string("\x01", maximum_length=30) prod_short_desc /*VARCHAR2(30) NOT NULL*/;
          string("\x01", maximum_length=60) gpi14_desc /*VARCHAR2(60) NOT NULL*/;
          string("\x01", maximum_length=60) gpi8_desc /*VARCHAR2(60) NOT NULL*/;
          string(1) newline = "\n";
          end;
          metadata type = product_lkp_t ;""")
            .map(s => parse(s).asInstanceOf[FFSchemaRecord])
            .map(s => Json.stringify(Json.toJson(s)))
            .getOrElse("")
        )
        .format("io.prophecy.libs.FixedFileFormat")
      df = reader.load(Config.PRODUCTS_FILE)
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
