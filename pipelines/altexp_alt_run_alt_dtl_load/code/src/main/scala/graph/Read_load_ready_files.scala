package graph

import io.prophecy.libs._
import config.Context
import udfs.UDFs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Read_load_ready_files {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
      val windowSpec        = Window.partitionBy(lit(1)).rowsBetween(Window.unboundedPreceding, Window.currentRow)
      val windowSpecPrevRow = Window.partitionBy(lit(1)).rowsBetween(Window.unboundedPreceding, -1)
      val windowSpecL       = Window.partitionBy(lit(1)).orderBy(lit(1))
    
      lazy val dfWithUniqueId = in.zipWithIndex(0, 1, "tempId", spark)
    
      val fileDF = dfWithUniqueId.select(col("tempId").as("tempId"),
                                         concat(lit(Config.INPUT_FILE_PATH), lit("/"), col("line")).as("fileName")
      )
    
      val fileRecordDF = dfWithUniqueId.mergeMultipleFileContentInDataFrame(
        fileDF,
        spark,
        delimiter = ",",
        abinitioSchema =
          """
    type alt_run_alt_dtl_load_t =
    record
      decimal("\x01",0, maximum_length=16 ) alt_run_alt_dtl_id = -1 ;
      decimal("\x01",0, maximum_length=16 ) alt_run_id = -1;
      decimal("\x01",0, maximum_length=16 ) alt_run_target_dtl_id = -1 ;
      string("\x01", maximum_length=20) formulary_name ;
      string("\x01", maximum_length=11) target_ndc ;
      string("\x01", maximum_length=11) alt_ndc ;
      string("\x01", maximum_length=2) alt_formulary_tier ;
      string("\x01", maximum_length=1) alt_multi_src_cd ;
      string("\x01", maximum_length=2) alt_roa_cd ;
      string("\x01", maximum_length=4) alt_dosage_form_cd ;
      decimal("\x01",0, maximum_length=39 ) rank ;
      string("\x01", maximum_length=1) alt_step_therapy_ind ;
      string("\x01", maximum_length=1) alt_pa_reqd_ind ;
      string("\x01", maximum_length=2) alt_formulary_status ;
      string("\x01", maximum_length=1) alt_specialty_ind ;
      string("\x01", maximum_length=14) alt_gpi14 ;
      string("\x01", maximum_length=70) alt_prod_name_ext ;
      string("\x01", maximum_length=30) alt_prod_short_desc ;
      string("\x01", maximum_length=30) alt_prod_short_desc_grp;
      string("\x01", maximum_length=60) alt_gpi14_desc ;
      string("\x01", maximum_length=60) alt_gpi8_desc ;
      string("\x01", maximum_length=50) alt_formulary_tier_desc ;
      string("\x01", maximum_length=50) alt_formulary_status_desc ;
      string("\x01", maximum_length=1) alt_pa_type_cd ;
      string("\x01", maximum_length=1) alt_step_therapy_type_cd ;
      string("\x01", maximum_length=1000) alt_step_therapy_group_name = NULL("") ;
      string("\x01", maximum_length=100) alt_step_therapy_step_number = NULL("") ;
      datetime("YYYY-MM-DD HH24:MI:SS")("\x01") rec_crt_ts ;
      string("\x01", maximum_length=30) rec_crt_user_id ;
      string("\x01", maximum_length=15) rebate_elig_cd = NULL("") ;
      string("\x01", maximum_length=15) tad_eligible_cd= NULL("");
      decimal("\x01".3, maximum_length=7) alt_qty_adj = NULL("") ;
      string("\x01", maximum_length=40) tal_assoc_name = NULL("") ;
      string("\x01", maximum_length=40) tala = NULL("") ;
      string("\x01", maximum_length=20) alt_udl = NULL("") ;
      decimal("\x01".6, maximum_length=39) tal_assoc_rank ;
      string("\x01", maximum_length=2) constituent_group = NULL("") ;
      string("\x01", maximum_length=1) constituent_reqd = NULL("") ;
      string(1) newline = "\n";
    end;
    metadata type = alt_run_alt_dtl_load_t ;""",
        readFormat = "fixedFormat",
        joinWithInputDataframe = false
      )
    
      val out = fileRecordDF
    out
  }

}
