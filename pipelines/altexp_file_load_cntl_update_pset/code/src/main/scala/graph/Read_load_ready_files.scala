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
    type file_load_ctl_dtl_t =
    record
      decimal("\x01",0, maximum_length=16) file_load_cntl_id ;
      string("\x01", maximum_length=4000) component_ids = NULL("") ;
      date("YYYYMMDD")("\x01") as_of_date ;
      string("\x01", maximum_length=20) rxclaim_env_name = NULL("") ;
      string("\x01", maximum_length=20) carrier = NULL("") ;
      string("\x01", maximum_length=20) account = NULL("") ;
      string("\x01", maximum_length=20) group = NULL("") ;
      decimal("\x01",0, maximum_length=39) component_type_cd = NULL("") ;
      string("\x01", maximum_length=512) file_name_w = NULL("") ;
      string("\x01", maximum_length=1) published_ind ;
      decimal("\x01",0, maximum_length=16) alt_run_id = NULL("") ;
      string("\x01", maximum_length=512) report_file_name = NULL("") ;
      string(1) newline = "\n";
    end;
    metadata type = file_load_ctl_dtl_t ;""",
        readFormat = "fixedFormat",
        joinWithInputDataframe = false
      )
    
      val out = fileRecordDF
    out
  }

}
