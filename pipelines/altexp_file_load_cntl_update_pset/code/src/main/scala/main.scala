import io.prophecy.libs._
import config._
import udfs.UDFs._
import udfs.PipelineInitCode._
import graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_Join_With_Input_Surrogate_Key_File__Input_Surrogate_Key_File_5 =
      if (
        _root_.io.prophecy.abinitio.ScalaFunctions._not(
          _root_.io.prophecy.abinitio.ScalaFunctions.convertToBoolean(
            _root_.io.prophecy.abinitio.ScalaFunctions
              ._is_blank(context.config.JOIN_KEY)
          )
        )
      )
        Join_With_Input_Surrogate_Key_File__Input_Surrogate_Key_File_5(context)
      else null
    val df_list_load_ready_files = list_load_ready_files(context)
    val df_Read_load_ready_files =
      Read_load_ready_files(context, df_list_load_ready_files)
    val df_SORT =
      if (
        _root_.io.prophecy.abinitio.ScalaFunctions._not(
          _root_.io.prophecy.abinitio.ScalaFunctions.convertToBoolean(
            _root_.io.prophecy.abinitio.ScalaFunctions
              ._is_blank(context.config.SORT_KEY)
          )
        )
      ) SORT(context, df_Read_load_ready_files)
      else df_Read_load_ready_files
    val df_Transform_Logic = Transform_Logic(context, df_SORT)
    val (df_Partition_Data_out1, df_Partition_Data_out0) =
      if (
        _root_.io.prophecy.abinitio.ScalaFunctions._not(
          _root_.io.prophecy.abinitio.ScalaFunctions.convertToBoolean(
            _root_.io.prophecy.abinitio.ScalaFunctions
              ._is_blank(context.config.JOIN_KEY)
          )
        )
      ) {
        val df_Join_With_Input_Surrogate_Key_File__Join =
          Join_With_Input_Surrogate_Key_File__Join(
            context,
            df_Join_With_Input_Surrogate_Key_File__Input_Surrogate_Key_File_5,
            df_Transform_Logic
          )
        Partition_Data(context, df_Join_With_Input_Surrogate_Key_File__Join)
      } else
        (null, null)
    val df_Join_With_DB =
      if (
        _root_.io.prophecy.abinitio.ScalaFunctions._not(
          _root_.io.prophecy.abinitio.ScalaFunctions.convertToBoolean(
            _root_.io.prophecy.abinitio.ScalaFunctions
              ._is_blank(context.config.JOIN_KEY)
          )
        )
      )
        if (
          _root_.io.prophecy.abinitio.ScalaFunctions._not(
            _root_.io.prophecy.abinitio.ScalaFunctions.convertToBoolean(
              _root_.io.prophecy.abinitio.ScalaFunctions
                ._is_blank(context.config.JOIN_DB_SQL)
            )
          )
        )
          Join_With_DB.apply(
            Join_With_DB.config
              .Context(context.spark, context.config.Join_With_DB),
            df_Partition_Data_out1
          )
        else df_Partition_Data_out1
      else null
    val df_s =
      if (
        _root_.io.prophecy.abinitio.ScalaFunctions._not(
          _root_.io.prophecy.abinitio.ScalaFunctions.convertToBoolean(
            _root_.io.prophecy.abinitio.ScalaFunctions
              ._is_blank(context.config.JOIN_KEY)
          )
        )
      )
        s(context, df_Partition_Data_out0, df_Join_With_DB)
      else null
    val df_Append_Mode_DateTimeNormalize =
      if (
        _root_.io.prophecy.abinitio.ScalaFunctions._not(
          _root_.io.prophecy.abinitio.ScalaFunctions.convertToBoolean(
            _root_.io.prophecy.abinitio.ScalaFunctions
              ._is_blank(context.config.JOIN_KEY)
          )
        )
      )
        Append_Mode_DateTimeNormalize(context, df_s)
      else null
    val df_Update_Mode_Log =
      if (
        _root_.io.prophecy.abinitio.ScalaFunctions._not(
          _root_.io.prophecy.abinitio.ScalaFunctions.convertToBoolean(
            _root_.io.prophecy.abinitio.ScalaFunctions
              ._is_blank(context.config.JOIN_KEY)
          )
        ) && context.config.LOAD_MODE == "U"
      )
        Update_Mode_Log(context, df_s)
      else null
    val df_Update_Mode =
      if (
        _root_.io.prophecy.abinitio.ScalaFunctions._not(
          _root_.io.prophecy.abinitio.ScalaFunctions.convertToBoolean(
            _root_.io.prophecy.abinitio.ScalaFunctions
              ._is_blank(context.config.JOIN_KEY)
          )
        ) && context.config.LOAD_MODE == "U"
      )
        Update_Mode(context, df_s)
      else null
    val df_Append_Mode_Log =
      if (
        _root_.io.prophecy.abinitio.ScalaFunctions._not(
          _root_.io.prophecy.abinitio.ScalaFunctions.convertToBoolean(
            _root_.io.prophecy.abinitio.ScalaFunctions
              ._is_blank(context.config.JOIN_KEY)
          )
        ) && context.config.LOAD_MODE == "A"
      )
        Append_Mode_Log(context, df_s)
      else null
    if (
      _root_.io.prophecy.abinitio.ScalaFunctions._not(
        _root_.io.prophecy.abinitio.ScalaFunctions.convertToBoolean(
          _root_.io.prophecy.abinitio.ScalaFunctions
            ._is_blank(context.config.JOIN_KEY)
        )
      ) && context.config.LOAD_MODE == "U" || _root_.io.prophecy.abinitio.ScalaFunctions
        ._not(
          _root_.io.prophecy.abinitio.ScalaFunctions.convertToBoolean(
            _root_.io.prophecy.abinitio.ScalaFunctions
              ._is_blank(context.config.JOIN_KEY)
          )
        ) && context.config.LOAD_MODE == "A"
    ) {
      val df_Handle_Serial_Logs =
        Handle_Serial_Logs(context,   df_Update_Mode_Log, df_Append_Mode_Log)
      Load_Serial_log_file_5(context, df_Handle_Serial_Logs)
    }
    val df_Output_Surrogate_Key_File_Creation__Fetching_Required_Fields =
      if (
        _root_.io.prophecy.abinitio.ScalaFunctions._not(
          _root_.io.prophecy.abinitio.ScalaFunctions.convertToBoolean(
            _root_.io.prophecy.abinitio.ScalaFunctions
              ._is_blank(context.config.JOIN_KEY)
          )
        ) && context.config.LOAD_MODE == "U" && _root_.io.prophecy.abinitio.ScalaFunctions
          .convertToBoolean(
            Array("alt_run_target_dtl", "alt_run_alt_dtl")
              .contains(context.config.TABLE_NAME)
          )
      )
        Output_Surrogate_Key_File_Creation__Fetching_Required_Fields(
          context,
          df_Update_Mode
        )
      else null
    if (
      _root_.io.prophecy.abinitio.ScalaFunctions._not(
        _root_.io.prophecy.abinitio.ScalaFunctions.convertToBoolean(
          _root_.io.prophecy.abinitio.ScalaFunctions
            ._is_blank(context.config.JOIN_KEY)
        )
      ) && context.config.LOAD_MODE == "A"
    )
      Append_Mode_5(context, df_Append_Mode_DateTimeNormalize)
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri",
                   "pipelines/altexp_file_load_cntl_update_pset"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark,
                                "pipelines/altexp_file_load_cntl_update_pset"
    ) {
      apply(context)
    }
  }

}
