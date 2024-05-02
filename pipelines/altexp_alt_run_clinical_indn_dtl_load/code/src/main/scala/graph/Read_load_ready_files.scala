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
    type alt_run_clinical_indn_dtl_load_t =
    record
      decimal("\x01",0, maximum_length=16) alt_run_id = -1 ;
      decimal("\x01",0, maximum_length=16) alt_run_target_dtl_id = -1 ;
      string("\x01", maximum_length=20) formulary_name ;
      string("\x01", maximum_length=11) target_ndc ;
      string("\x01", maximum_length=40) tal_assoc_name ;
      decimal("\x01",0, maximum_length=39) rank ;
      datetime("YYYY-MM-DD HH24:MI:SS")("\x01") rec_crt_ts ;
      string("\x01", maximum_length=30) rec_crt_user_id ;
      string("\x01", maximum_length=200) clinical_indn_desc = NULL("") ;
      string(1) newline = "\n";
    end;
    metadata type = alt_run_clinical_indn_dtl_load_t ;""",
        readFormat = "fixedFormat",
        joinWithInputDataframe = false
      )
    
      val out = fileRecordDF
    out
  }

}
