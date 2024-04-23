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

object Map_Cag_and_Formulary_Override_URLs {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import _root_.io.prophecy.abinitio.ScalaFunctions._
    import scala.util.control.Breaks
    
    def frmlry_url() = {
      coalesce(
        lookup(
          "Formulary_Override_Ref",
          col("formulary_name"),
          col("carrier"),
          col("account"),
          col("group"),
          col("customer_name"),
          when(string_substring(lit(Config.BUSINESS_DATE), lit(1), lit(6)) === string_substring(lit(Config.COMPARE_DATE),
                                                                                                lit(1),
                                                                                                lit(6)
               ),
               lit(0)
          ).otherwise(lit(1)).cast(StringType)
        ).getField("data_path"),
        lookup(
          "Formulary_Override_Ref",
          col("formulary_name"),
          col("carrier"),
          col("account"),
          lit("*ALL"),
          col("customer_name"),
          when(string_substring(lit(Config.BUSINESS_DATE), lit(1), lit(6)) === string_substring(lit(Config.COMPARE_DATE),
                                                                                                lit(1),
                                                                                                lit(6)
               ),
               lit(0)
          ).otherwise(lit(1)).cast(StringType)
        ).getField("data_path"),
        lookup(
          "Formulary_Override_Ref",
          col("formulary_name"),
          col("carrier"),
          lit("*ALL"),
          lit("*ALL"),
          col("customer_name"),
          when(string_substring(lit(Config.BUSINESS_DATE), lit(1), lit(6)) === string_substring(lit(Config.COMPARE_DATE),
                                                                                                lit(1),
                                                                                                lit(6)
               ),
               lit(0)
          ).otherwise(lit(1)).cast(StringType)
        ).getField("data_path"),
        lookup(
          "Formulary_Override_Ref",
          col("formulary_name"),
          lit("*ALL"),
          lit("*ALL"),
          lit("*ALL"),
          col("customer_name"),
          when(string_substring(lit(Config.BUSINESS_DATE), lit(1), lit(6)) === string_substring(lit(Config.COMPARE_DATE),
                                                                                                lit(1),
                                                                                                lit(6)
               ),
               lit(0)
          ).otherwise(lit(1)).cast(StringType)
        ).getField("data_path"),
        lookup(
          "Formulary_Override_Ref",
          col("formulary_name"),
          lit(null),
          lit(null),
          lit(null),
          col("customer_name"),
          when(string_substring(lit(Config.BUSINESS_DATE), lit(1), lit(6)) === string_substring(lit(Config.COMPARE_DATE),
                                                                                                lit(1),
                                                                                                lit(6)
               ),
               lit(0)
          ).otherwise(lit(1)).cast(StringType)
        ).getField("data_path"),
        lit("FORMULARY_NOT_FOUND")
      )
    }
    
    def cag_url() = {
      when(
        col("future_flg") === lit(2),
        when(
          isnull(col("carrier")),
          coalesce(
            lookup("CAG_Override_Ref", lit(null), lit(null), lit(null), col("future_flg")).getField("data_path"),
            lookup("CAG_Override_Ref", lit(null), lit(null), lit(null), lit(0)).getField("data_path"),
            lit("CAG_NOT_FOUND")
          )
        ).otherwise(
          coalesce(
            lookup("CAG_Override_Ref", col("carrier"), col("account"), col("group"), col("future_flg"))
              .getField("data_path"),
            lookup("CAG_Override_Ref", col("carrier"), col("account"), lit("*ALL"), col("future_flg"))
              .getField("data_path"),
            lookup("CAG_Override_Ref", col("carrier"), lit("*ALL"), lit("*ALL"), col("future_flg")).getField("data_path"),
            lookup("CAG_Override_Ref", lit("*ALL"),    lit("*ALL"), lit("*ALL"), col("future_flg")).getField("data_path"),
            when(
              month(date_format(to_date(lit(Config.BUSINESS_DATE), "yyyyMMdd"), "yyyy-MM-dd")) === lit(12),
              lookup("CAG_Override_Ref", col("carrier"), col("account"), col("group"), lit(1)).getField("data_path")
            ),
            when(
              month(date_format(to_date(lit(Config.BUSINESS_DATE), "yyyyMMdd"), "yyyy-MM-dd")) === lit(12),
              lookup("CAG_Override_Ref", col("carrier"), col("account"), lit("*ALL"), lit(1)).getField("data_path")
            ),
            when(
              month(date_format(to_date(lit(Config.BUSINESS_DATE), "yyyyMMdd"), "yyyy-MM-dd")) === lit(12),
              lookup("CAG_Override_Ref", col("carrier"), lit("*ALL"), lit("*ALL"), lit(1)).getField("data_path")
            ),
            when(
              month(date_format(to_date(lit(Config.BUSINESS_DATE), "yyyyMMdd"), "yyyy-MM-dd")) === lit(12),
              lookup("CAG_Override_Ref", lit("*ALL"), lit("*ALL"), lit("*ALL"), lit(1)).getField("data_path")
            ),
            lookup("CAG_Override_Ref", col("carrier"), col("account"), col("group"), lit(0)).getField("data_path"),
            lookup("CAG_Override_Ref", lit(null),      lit(null),      lit(null),    col("future_flg")).getField("data_path"),
            lookup("CAG_Override_Ref", lit(null),      lit(null),      lit(null),    lit(0)).getField("data_path"),
            lit("CAG_NOT_FOUND")
          )
        )
      ).when(isnull(col("carrier")),
             lookup("CAG_Override_Ref", lit(null), lit(null), lit(null), lit(0)).getField("data_path")
      ).otherwise(
        coalesce(
          lookup("CAG_Override_Ref",
                 col("carrier"),
                 col("account"),
                 col("group"),
                 when(col("future_flg") === lit(0), lit(0)).otherwise(lit(1)).cast(IntegerType)
          ).getField("data_path"),
          lookup("CAG_Override_Ref",
                 col("carrier"),
                 col("account"),
                 lit("*ALL"),
                 when(col("future_flg") === lit(0), lit(0)).otherwise(lit(1)).cast(IntegerType)
          ).getField("data_path"),
          lookup("CAG_Override_Ref",
                 col("carrier"),
                 lit("*ALL"),
                 lit("*ALL"),
                 when(col("future_flg") === lit(0), lit(0)).otherwise(lit(1)).cast(IntegerType)
          ).getField("data_path"),
          lookup("CAG_Override_Ref",
                 lit("*ALL"),
                 lit("*ALL"),
                 lit("*ALL"),
                 when(col("future_flg") === lit(0), lit(0)).otherwise(lit(1)).cast(IntegerType)
          ).getField("data_path"),
          lookup("CAG_Override_Ref",
                 lit(null),
                 lit(null),
                 lit(null),
                 when(col("future_flg") === lit(0), lit(0)).otherwise(lit(1)).cast(IntegerType)
          ).getField("data_path"),
          lit("CAG_NOT_FOUND")
        )
      )
    }
    
    def filter_output_profile_record(
      frmlry_url:          org.apache.spark.sql.Column,
      cag_url:             org.apache.spark.sql.Column,
      is_future_snap_form: org.apache.spark.sql.Column,
      is_future_snap_cag:  org.apache.spark.sql.Column
    ) = {
      var error_message: org.apache.spark.sql.Column = lit("")
      var is_current_form: org.apache.spark.sql.Column =
        when(is_future_snap_form.cast(BooleanType), lit("Future"))
          .otherwise(lit("Current"))
      var is_current_cag: org.apache.spark.sql.Column =
        when(is_future_snap_cag.cast(BooleanType), lit("Future"))
          .otherwise(lit("Current"))
      error_message = array_join(
        array(
          lit(""),
          array_join(
            array(
              when(
                !lookup_match("TAL_Dtl", col("tal_name")).cast(BooleanType),
                array_join(
                  array(
                    lit("Invalid TAL="),
                    col("tal_name"),
                    lit(" used for Output Profile="),
                    col("output_profile_name"),
                    lit(".")
                  ),
                  ""
                )
              ),
              when(
                !lookup_match("TAC_Dtl", col("tac_name")).cast(BooleanType),
                array_join(
                  array(
                    lit("Invalid TAC="),
                    col("tac_name"),
                    lit(" used for Output Profile="),
                    col("output_profile_name"),
                    lit(".")
                  ),
                  ""
                )
              ),
              when(
                !lookup_match("TAR_Dtl", col("tar_name")).cast(BooleanType),
                array_join(
                  array(
                    lit("Invalid TAR="),
                    col("tar_name"),
                    lit(" used for Output Profile="),
                    col("output_profile_name"),
                    lit(".")
                  ),
                  ""
                )
              ),
              when(
                !lookup_match("TSD_Dtl", col("tsd_name")).cast(BooleanType),
                array_join(
                  array(
                    lit("Invalid TSD="),
                    col("tsd_name"),
                    lit(" used for Output Profile="),
                    col("output_profile_name"),
                    lit(".")
                  ),
                  ""
                )
              ),
              when(
                cag_url === lit("CAG_NOT_FOUND"),
                array_join(
                  array(
                    is_current_cag,
                    lit(
                      " Dated Drug data snapshot is not available for Output Profile="
                    ),
                    col("output_profile_name"),
                    lit(".")
                  ),
                  ""
                )
              ),
              when(
                frmlry_url === lit("FORMULARY_NOT_FOUND"),
                array_join(
                  array(
                    is_current_form,
                    lit(
                      " Dated Formulary data snapshot is not available for Output Profile="
                    ),
                    col("output_profile_name"),
                    lit(" and Formulary="),
                    col("formulary_name"),
                    lit(" and Customer="),
                    coalesce(col("customer_name"), lit("NULL"))
                  ),
                  ""
                )
              )
            ),
            ""
          )
        ),
        ""
      )
      error_message
    }
    
    def error_message() = {
        filter_output_profile_record(
        frmlry_url(),
        cag_url(),
        when(string_substring(lit(Config.BUSINESS_DATE), lit(1), lit(6)) === string_substring(lit(Config.COMPARE_DATE),
          lit(1),
          lit(6)
        ),
          lit(0)
        ).otherwise(lit(1)).cast(StringType),
        when(string_substring(lit(Config.BUSINESS_DATE), lit(1), lit(4)) === string_substring(lit(Config.COMPARE_DATE),
          lit(1),
          lit(4)
        ),
          lit(0)
        ).otherwise(lit(1)).cast(StringType)
      )
    }
    
    val process_udf = udf(
      (inputRows: Seq[Row], cag_url: String) ⇒ {
        var op_dtls           = Array[Row]()
        var index             = 0
        var output_profile_id = Array[java.math.BigDecimal]()
        var error_message_vec = Array[String]()
        var err_vec_gv        = Array[String]()
    
        inputRows.zipWithIndex.foreach {
          case (in, jdx) ⇒
            var error_msg           = in.getAs[String]("error_message")
            var formulary_pseudonym = in.getAs[String]("formulary_pseudonym")
            var frmlry_url          = in.getAs[String]("frmlry_url")
    
            if (output_profile_id.contains(in.getAs[java.math.BigDecimal]("output_profile_id"))) {
              if (!op_dtls(convertToInt(index)).getAs[Seq[String]](3).toArray.contains(in.getAs[String]("formulary_name")))
                op_dtls(index) = Row(
                  op_dtls(convertToInt(index)).getAs[String](0),
                  op_dtls(convertToInt(index)).getAs[Seq[String]](1).toArray,
                  op_dtls(convertToInt(index)).getAs[Seq[String]](2).toArray,
                  Array.concat(op_dtls(convertToInt(index)).getAs[Seq[String]](3).toArray,
                               Array.fill(1)(in.getAs[String]("formulary_name"))
                  ),
                  Array.concat(op_dtls(convertToInt(index)).getAs[Seq[String]](4).toArray, Array.fill(1)(frmlry_url)),
                  Array.concat(op_dtls(convertToInt(index)).getAs[Seq[String]](5).toArray,
                               Array.fill(1)(formulary_pseudonym)
                  ),
                  op_dtls(convertToInt(index)).getAs[String](6),
                  op_dtls(convertToInt(index)).getAs[String](7),
                  op_dtls(convertToInt(index)).getAs[String](8),
                  op_dtls(convertToInt(index)).getAs[String](9),
                  op_dtls(convertToInt(index)).getAs[String](10)
                )
              if (
                (!_isnull(in.getAs[String]("alias_name"))) && (!op_dtls(convertToInt(index))
                  .getAs[Seq[String]](1)
                  .toArray
                  .contains(in.getAs[String]("alias_name")))
              )
                op_dtls(index) = Row(
                  op_dtls(convertToInt(index)).getAs[String](0),
                  Array.concat(op_dtls(convertToInt(index)).getAs[Seq[String]](1).toArray,
                               Array.fill(1)(in.getAs[String]("alias_name"))
                  ),
                  op_dtls(convertToInt(index)).getAs[Seq[String]](2).toArray,
                  op_dtls(convertToInt(index)).getAs[Seq[String]](3).toArray,
                  op_dtls(convertToInt(index)).getAs[Seq[String]](4).toArray,
                  op_dtls(convertToInt(index)).getAs[Seq[String]](5).toArray,
                  op_dtls(convertToInt(index)).getAs[String](6),
                  op_dtls(convertToInt(index)).getAs[String](7),
                  op_dtls(convertToInt(index)).getAs[String](8),
                  op_dtls(convertToInt(index)).getAs[String](9),
                  op_dtls(convertToInt(index)).getAs[String](10)
                )
              if (
                (!_isnull(in.getAs[String]("job_name"))) && (!op_dtls(convertToInt(index))
                  .getAs[Seq[String]](2)
                  .toArray
                  .contains(in.getAs[String]("job_name")))
              )
                op_dtls(index) = Row(
                  op_dtls(convertToInt(index)).getAs[String](0),
                  op_dtls(convertToInt(index)).getAs[Seq[String]](1).toArray,
                  Array.concat(op_dtls(convertToInt(index)).getAs[Seq[String]](2).toArray,
                               Array.fill(1)(in.getAs[String]("job_name"))
                  ),
                  op_dtls(convertToInt(index)).getAs[Seq[String]](3).toArray,
                  op_dtls(convertToInt(index)).getAs[Seq[String]](4).toArray,
                  op_dtls(convertToInt(index)).getAs[Seq[String]](5).toArray,
                  op_dtls(convertToInt(index)).getAs[String](6),
                  op_dtls(convertToInt(index)).getAs[String](7),
                  op_dtls(convertToInt(index)).getAs[String](8),
                  op_dtls(convertToInt(index)).getAs[String](9),
                  op_dtls(convertToInt(index)).getAs[String](10)
                )
            } else {
              output_profile_id = Array.concat(output_profile_id, Array.fill(1)(in.getAs[java.math.BigDecimal]("output_profile_id")))
              op_dtls = Array.concat(
                op_dtls,
                Array.fill(1)(
                  Row(
                    in.getAs[String]("output_profile_name"),
                    if (
                      (try Array(in.getAs[String]("alias_name"))
                      catch {
                        case error: Throwable ⇒ null
                      }) == null
                    )
                      Array[Any]()
                    else
                      in.getAs[String]("alias_name"),
                    Array(in.getAs[String]("job_name")),
                    Array(in.getAs[String]("formulary_name")),
                    Array(frmlry_url),
                    Array(formulary_pseudonym),
                    in.getAs[String]("tal_name"),
                    in.getAs[String]("tac_name"),
                    in.getAs[String]("tar_name"),
                    in.getAs[String]("tsd_name"),
                    in.getAs[String]("st_tac_ind")
                  )
                )
              )
              index = convertToInt(op_dtls.length - 1)
            }
    
            if (!error_msg.isBlank) {
              var err_vec = _string_split_no_empty(error_msg, ".").diff(error_message_vec)
              if (err_vec.nonEmpty) {
                err_vec_gv = Array.concat(err_vec_gv,               err_vec)
                error_message_vec = Array.concat(error_message_vec, err_vec)
              }
            }
        }
        var in = inputRows.last
        Row(
          in.getAs[String]("carrier"), 
          in.getAs[String]("account"), 
          in.getAs[String]("group"),
          if (cag_url != "CAG_NOT_FOUND") cag_url else "",
          output_profile_id,
          op_dtls,
          err_vec_gv,
          in.getAs[String]("newline")
        )
      },
      StructType(
        List(
          StructField("carrier", StringType, true),
          StructField("account", StringType, true),
          StructField("group", StringType, true),
          StructField("cag_override_data_path", StringType, true),
          StructField("output_profile_id", ArrayType(DecimalType(10, 0), true), true),
          StructField(
            "op_dtls",
            ArrayType(
              StructType(
                List(
                  StructField("output_profile_name", StringType, true),
                  StructField("alias_names", ArrayType(StringType, true), true),
                  StructField("job_names", ArrayType(StringType, true), true),
                  StructField("formulary_names", ArrayType(StringType, true), true),
                  StructField("form_override_data_paths", ArrayType(StringType, true), true),
                  StructField("formulary_pseudonyms", ArrayType(StringType, true), true),
                  StructField("tal_name", StringType, true),
                  StructField("tac_name", StringType, true),
                  StructField("tar_name", StringType, true),
                  StructField("tsd_name", StringType, true),
                  StructField("st_tac_ind", StringType, true)
                )
              ),
              true
            ),
            true
          ),
          StructField("err_msgs", ArrayType(StringType, true), true),
          StructField("newline", StringType, true)
        )
      )
    )
    
    val origColumns = in.columns.map(col)
    val out = in
      .groupBy("carrier", "account", "group", "future_flg", "as_of_dt")
      .agg(
        collect_list(
          struct(
            (origColumns :+ frmlry_url().alias("frmlry_url") :+ error_message()
              .alias("error_message")): _*
          )
        ).alias("inputRows"),
        first(cag_url()).alias("cag_url")
      )
      .select(process_udf(col("inputRows"), col("cag_url")).alias("output"))
      .select(col("output.*"))
    out
  }

}
