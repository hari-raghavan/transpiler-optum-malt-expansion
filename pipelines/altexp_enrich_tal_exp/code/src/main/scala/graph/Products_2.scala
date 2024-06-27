package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Products_2 {

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
            """type udl_exp_t =
record
  decimal("\x01",0, maximum_length=10) udl_id;
  string("\x01", maximum_length=20) udl_nm; 
  string("\x01", maximum_length=60) udl_desc; 
  bit_vector_t products;
  date("YYYYMMDD")("\x01") eff_dt ;
  date("YYYYMMDD")("\x01") term_dt ;
  bit_vector_t[int] contents;
  string(1) newline = "\n";
end;
type product_lkp_t =
record
  int dl_bit;
  string("\x01", maximum_length=11)  ndc11 ;
  string("\x01", maximum_length=14)  gpi14 ;
  string("\x01", maximum_length=1)  status_cd ;
  string("\x01", maximum_length=8) inactive_dt ;
  string("\x01", maximum_length=1)  msc ;
  string("\x01", maximum_length=70)  drug_name ;
  string("\x01", maximum_length=3)  rx_otc ;
  string("\x01", maximum_length=1)  desi ;
  string("\x01", maximum_length=2)  roa_cd ;
  string("\x01", maximum_length=4)  dosage_form_cd ;
  decimal("\x01".5, maximum_length=15) prod_strength ;
  string("\x01", maximum_length=1)  repack_cd ; 
  string("\x01", maximum_length=30) prod_short_desc ;
  string("\x01", maximum_length=60) gpi14_desc ;
  string("\x01", maximum_length=60) gpi8_desc ;
  string(1) newline = "\n";
end;
type tal_assoc_adhoc_enrich_t =
record
  decimal("\x01",0, maximum_length=10) tal_assoc_dtl_id ;
  decimal("\x01",0, maximum_length=10) tal_assoc_id ;
  string("\x01", maximum_length=20) tal_assoc_name ;
  string("\x01", maximum_length=2000) clinical_indn_desc = NULL("");
  string("\x01", maximum_length=60) tal_assoc_desc ;
  decimal("\x01",0, maximum_length=39) tal_assoc_type_cd ;
  string("\x01", maximum_length=20) udl_id = NULL("") ;  
  string("\x01", maximum_length=60) udl_desc; 
  string("\x01", maximum_length=20) Target_Alternative;
  bit_vector_t products;
  decimal("\x01",0, maximum_length=39) alt_rank = NULL("") ;
  string("\x01", maximum_length=30) shared_qual ;
  string("\x01", maximum_length=20) override_tac_name = NULL("") ;
  string("\x01", maximum_length=20) override_tar_name = NULL("") ;
  string(2) constituent_group = NULL("");
  string(1) constituent_reqd = NULL("");
  decimal("\x01",0, maximum_length=39) constituent_rank = NULL("");
  string(1) newline = "\n";
end;
type tal_container_adhoc_enrich_t =
record
  string("\x01", maximum_length=20) tal_id ;
  string("\x01", maximum_length=20) tal_assoc_id = NULL("") ;
  string("\x01", maximum_length=2000) clinical_indn_desc = NULL("");
  string("\x01", maximum_length=60) tal_desc ;
  string("\x01", maximum_length=60) tal_assoc_desc ;
  decimal("\x01",0, maximum_length=39) tal_assoc_type_cd ;
  decimal("\x01", 6, maximum_length=39) tal_assoc_rank = NULL("") ;
  string("\x01", maximum_length=20) udl_id = NULL("") ;  
  string("\x01", maximum_length=60) udl_desc; 
  string("\x01", maximum_length=20) Target_Alternative;
  bit_vector_t products;
  decimal("\x01",0, maximum_length=39) alt_rank = NULL("") ;
  string("\x01", maximum_length=30) shared_qual ;
  string("\x01", maximum_length=20) override_tac_name = NULL("") ;
  string("\x01", maximum_length=20) override_tar_name = NULL("") ;
  string(2) constituent_group = NULL("");
  string(1) constituent_reqd = NULL("");
  decimal("\x01",0, maximum_length=39) constituent_rank = NULL("");
  string(1) newline = "\n";
end;
type tal_container_adhoc_enrich_t =
record
  string("\x01", maximum_length=20) tal_id ;
  string("\x01", maximum_length=20) tal_assoc_id = NULL("") ;
  string("\x01", maximum_length=2000) clinical_indn_desc = NULL("");
  string("\x01", maximum_length=60) tal_desc ;
  string("\x01", maximum_length=60) tal_assoc_desc ;
  decimal("\x01",0, maximum_length=39) tal_assoc_type_cd ;
  decimal("\x01", 6, maximum_length=39) tal_assoc_rank = NULL("") ;
  string("\x01", maximum_length=20) udl_id = NULL("") ;  
  string("\x01", maximum_length=60) udl_desc; 
  string("\x01", maximum_length=20) Target_Alternative;
  bit_vector_t products;
  decimal("\x01",0, maximum_length=39) alt_rank = NULL("") ;
  string("\x01", maximum_length=30) shared_qual ;
  string("\x01", maximum_length=20) override_tac_name = NULL("") ;
  string("\x01", maximum_length=20) override_tar_name = NULL("") ;
  string(2) constituent_group = NULL("");
  string(1) constituent_reqd = NULL("");
  decimal("\x01",0, maximum_length=39) constituent_rank = NULL("");
  string(1) newline = "\n";
end;
metadata type = product_lkp_t ;"""
          ).map(s => parse(s).asInstanceOf[FFSchemaRecord])
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
