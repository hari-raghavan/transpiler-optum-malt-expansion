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
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    def frmlry_url() = {
      coalesce(
        lookup(
          "Formulary_Override_Ref",
          col("formulary_name"),
          col("carrier"),
          col("account"),
          col("group"),
          col("customer_name"),
          col("future_flg")
        ).getField("data_path"),
        lookup(
          "Formulary_Override_Ref",
          col("formulary_name"),
          col("carrier"),
          col("account"),
          lit("*ALL"),
          col("customer_name"),
          col("future_flg")
        ).getField("data_path"),
        lookup(
          "Formulary_Override_Ref",
          col("formulary_name"),
          col("carrier"),
          lit("*ALL"),
          lit("*ALL"),
          col("customer_name"),
          col("future_flg")
        ).getField("data_path"),
        lookup(
          "Formulary_Override_Ref",
          col("formulary_name"),
          lit("*ALL"),
          lit("*ALL"),
          lit("*ALL"),
          col("customer_name"),
          col("future_flg")
        ).getField("data_path"),
        lookup(
          "Formulary_Override_Ref",
          col("formulary_name"),
          lit(null),
          lit(null),
          lit(null),
          col("customer_name"),
          col("future_flg")
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
            lookup(
              "CAG_Override_Ref",
              lit(null),
              lit(null),
              lit(null),
              col("future_flg")
            ).getField("data_path"),
            lookup("CAG_Override_Ref", lit(null), lit(null), lit(null), lit(0))
              .getField("data_path"),
            lit("CAG_NOT_FOUND")
          )
        ).otherwise(
          coalesce(
            lookup(
              "CAG_Override_Ref",
              col("carrier"),
              col("account"),
              col("group"),
              col("future_flg")
            )
              .getField("data_path"),
            lookup(
              "CAG_Override_Ref",
              col("carrier"),
              col("account"),
              lit("*ALL"),
              col("future_flg")
            )
              .getField("data_path"),
            lookup(
              "CAG_Override_Ref",
              col("carrier"),
              lit("*ALL"),
              lit("*ALL"),
              col("future_flg")
            ).getField("data_path"),
            lookup(
              "CAG_Override_Ref",
              lit("*ALL"),
              lit("*ALL"),
              lit("*ALL"),
              col("future_flg")
            ).getField("data_path"),
            when(
              month(
                date_format(to_date(Config.BUSINESS_DATE, "yyyyMMdd"), "yyyy-MM-dd")
              ) === lit(12),
              lookup(
                "CAG_Override_Ref",
                col("carrier"),
                col("account"),
                col("group"),
                lit(1)
              ).getField("data_path")
            ),
            when(
              month(
                date_format(to_date(Config.BUSINESS_DATE, "yyyyMMdd"), "yyyy-MM-dd")
              ) === lit(12),
              lookup(
                "CAG_Override_Ref",
                col("carrier"),
                col("account"),
                lit("*ALL"),
                lit(1)
              ).getField("data_path")
            ),
            when(
              month(
                date_format(to_date(Config.BUSINESS_DATE, "yyyyMMdd"), "yyyy-MM-dd")
              ) === lit(12),
              lookup(
                "CAG_Override_Ref",
                col("carrier"),
                lit("*ALL"),
                lit("*ALL"),
                lit(1)
              ).getField("data_path")
            ),
            when(
              month(
                date_format(to_date(Config.BUSINESS_DATE, "yyyyMMdd"), "yyyy-MM-dd")
              ) === lit(12),
              lookup(
                "CAG_Override_Ref",
                lit("*ALL"),
                lit("*ALL"),
                lit("*ALL"),
                lit(1)
              ).getField("data_path")
            ),
            lookup(
              "CAG_Override_Ref",
              col("carrier"),
              col("account"),
              col("group"),
              lit(0)
            ).getField("data_path"),
            lookup(
              "CAG_Override_Ref",
              lit(null),
              lit(null),
              lit(null),
              col("future_flg")
            ).getField("data_path"),
            lookup("CAG_Override_Ref", lit(null), lit(null), lit(null), lit(0))
              .getField("data_path"),
            lit("CAG_NOT_FOUND")
          )
        )
      ).when(
        isnull(col("carrier")),
        lookup("CAG_Override_Ref", lit(null), lit(null), lit(null), lit(0))
          .getField("data_path")
      ).otherwise(
        coalesce(
          lookup(
            "CAG_Override_Ref",
            col("carrier"),
            col("account"),
            col("group"),
            when(col("future_flg") === lit(0), lit(0))
              .otherwise(lit(1))
              .cast(IntegerType)
          ).getField("data_path"),
          lookup(
            "CAG_Override_Ref",
            col("carrier"),
            col("account"),
            lit("*ALL"),
            when(col("future_flg") === lit(0), lit(0))
              .otherwise(lit(1))
              .cast(IntegerType)
          ).getField("data_path"),
          lookup(
            "CAG_Override_Ref",
            col("carrier"),
            lit("*ALL"),
            lit("*ALL"),
            when(col("future_flg") === lit(0), lit(0))
              .otherwise(lit(1))
              .cast(IntegerType)
          ).getField("data_path"),
          lookup(
            "CAG_Override_Ref",
            lit("*ALL"),
            lit("*ALL"),
            lit("*ALL"),
            when(col("future_flg") === lit(0), lit(0))
              .otherwise(lit(1))
              .cast(IntegerType)
          ).getField("data_path"),
          lookup(
            "CAG_Override_Ref",
            lit(null),
            lit(null),
            lit(null),
            when(col("future_flg") === lit(0), lit(0))
              .otherwise(lit(1))
              .cast(IntegerType)
          ).getField("data_path"),
          lit("CAG_NOT_FOUND")
        )
      )
    }
    
    def error_message() = {
      concat(
        lit(""),
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
          !lookup_match("TAR_Dtl", col("tar_name"), col("as_of_dt"))
            .cast(BooleanType),
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
          cag_url() === lit("CAG_NOT_FOUND"),
          array_join(
            array(
              when(col("future_flg") === lit(2), lit("Next Month"))
                .when(col("future_flg") === lit(1), lit("Next Year"))
                .otherwise(lit("Current")),
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
          frmlry_url() === lit("FORMULARY_NOT_FOUND"),
          array_join(
            array(
              when(col("future_flg") === lit(2), lit("Next Month"))
                .when(col("future_flg") === lit(1), lit("Next Year"))
                .otherwise(lit("Current")),
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
      )
    }
    
    val processUDF = udf(
      (inputRows: Seq[Row], cag_url: String) => {
        var non_qual_output_profile_ids = Array[Row]()
        var qual_output_profile_ids = Array[String]()
        var op_dtls = Array[Row]()
        var index = 0
        var err_msgs = Array[String]()
    
        var carrier = ""
        var account = ""
        var group = ""
        var future_flg = ""
        var as_of_dt = ""
        var cag_override_data_path = ""
        var newline = ""
        inputRows.zipWithIndex.foreach { case (in, jdx) =>
          var error_msg = in.getAs[String]("error_message")
          var formulary_pseudonym = in.getAs[String]("formulary_pseudonym")
          var frmlry_url = in.getAs[String]("frmlry_url")
          var is_current = (if (in.getAs[String]("future_flg").toInt == 2)
                              "Next Month"
                            else {
                              if (in.getAs[String]("future_flg").toInt == 1)
                                "Next Year"
                              else
                                "Current"
                            }).toString
    
          carrier = in.getAs[String]("carrier")
          account = in.getAs[String]("account")
          group = in.getAs[String]("group")
          future_flg =
            if (in.getAs[String]("future_flg") == "2") "NM"
            else if (in.getAs[String]("future_flg") == "1") "NY"
            else in.getAs[String]("future_flg")
          as_of_dt = in.getAs[String]("as_of_dt")
          cag_override_data_path = if (cag_url != "CAG_NOT_FOUND") cag_url else ""
          newline = in.getAs[String]("newline")
          if (_is_blank(error_msg.toString)) {
            if (
              qual_output_profile_ids
                .contains(in.getAs[String]("output_profile_id"))
            ) {
              if (
                !op_dtls(convertToInt(index))
                  .getAs[Seq[String]]("formulary_names")
                  .toArray
                  .contains(in.getAs[String]("formulary_name"))
              )
                op_dtls(index) = Row(
                  op_dtls(convertToInt(index)).getAs[String](0),
                  op_dtls(convertToInt(index)).getAs[String](1),
                  op_dtls(convertToInt(index)).getAs[Seq[String]](2).toArray,
                  op_dtls(convertToInt(index)).getAs[Seq[String]](3).toArray,
                  op_dtls(convertToInt(index)).getAs[Seq[String]](4).toArray,
                  Array.concat(
                    op_dtls(convertToInt(index)).getAs[Seq[String]](5).toArray,
                    Array.fill(1)(in.getAs[String]("formulary_name"))
                  ),
                  Array.concat(
                    op_dtls(convertToInt(index)).getAs[Seq[String]](6).toArray,
                    Array.fill(1)(frmlry_url)
                  ),
                  Array.concat(
                    op_dtls(convertToInt(index)).getAs[Seq[String]](7).toArray,
                    Array.fill(1)(formulary_pseudonym)
                  ),
                  op_dtls(convertToInt(index)).getAs[String](8),
                  op_dtls(convertToInt(index)).getAs[String](9),
                  op_dtls(convertToInt(index)).getAs[String](10),
                  op_dtls(convertToInt(index)).getAs[String](11),
                  op_dtls(convertToInt(index)).getAs[String](12)
                )
              if (
                !op_dtls(convertToInt(index))
                  .getAs[Seq[String]](2)
                  .toArray
                  .contains(in.getAs[String]("job_id"))
              )
                op_dtls(index) = Row(
                  op_dtls(convertToInt(index)).getAs[String](0),
                  op_dtls(convertToInt(index)).getAs[String](1),
                  Array.concat(
                    op_dtls(convertToInt(index)).getAs[Seq[String]](2).toArray,
                    Array.fill(1)(in.getAs[String]("job_id"))
                  ),
                  op_dtls(convertToInt(index)).getAs[Seq[String]](3).toArray,
                  op_dtls(convertToInt(index)).getAs[Seq[String]](4).toArray,
                  op_dtls(convertToInt(index)).getAs[Seq[String]](5).toArray,
                  op_dtls(convertToInt(index)).getAs[Seq[String]](6).toArray,
                  op_dtls(convertToInt(index)).getAs[Seq[String]](7).toArray,
                  op_dtls(convertToInt(index)).getAs[String](8),
                  op_dtls(convertToInt(index)).getAs[String](9),
                  op_dtls(convertToInt(index)).getAs[String](10),
                  op_dtls(convertToInt(index)).getAs[String](11),
                  op_dtls(convertToInt(index)).getAs[String](12)
                )
              if (
                (!_isnull(in.getAs[String]("alias_name"))) && (!op_dtls(
                  convertToInt(index)
                )
                  .getAs[Seq[String]]("alias_names")
                  .toArray
                  .contains(in.getAs[String]("alias_name")))
              )
                op_dtls(index) = Row(
                  op_dtls(convertToInt(index)).getAs[String](0),
                  op_dtls(convertToInt(index)).getAs[String](1),
                  op_dtls(convertToInt(index)).getAs[Seq[String]](2).toArray,
                  Array.concat(
                    op_dtls(convertToInt(index)).getAs[Seq[String]](3).toArray,
                    Array.fill(1)(in.getAs[String]("alias_name"))
                  ),
                  op_dtls(convertToInt(index)).getAs[Seq[String]](4).toArray,
                  op_dtls(convertToInt(index)).getAs[Seq[String]](5).toArray,
                  op_dtls(convertToInt(index)).getAs[Seq[String]](6).toArray,
                  op_dtls(convertToInt(index)).getAs[Seq[String]](7).toArray,
                  op_dtls(convertToInt(index)).getAs[String](8),
                  op_dtls(convertToInt(index)).getAs[String](9),
                  op_dtls(convertToInt(index)).getAs[String](10),
                  op_dtls(convertToInt(index)).getAs[String](11),
                  op_dtls(convertToInt(index)).getAs[String](12)
                )
              if (
                (!_isnull(in.getAs[String]("job_name"))) && (!op_dtls(
                  convertToInt(index)
                )
                  .getAs[Seq[String]]("job_names")
                  .toArray
                  .contains(in.getAs[String]("job_name")))
              )
                op_dtls(index) = Row(
                  op_dtls(convertToInt(index)).getAs[String](0),
                  op_dtls(convertToInt(index)).getAs[String](1),
                  op_dtls(convertToInt(index)).getAs[Seq[String]](2).toArray,
                  op_dtls(convertToInt(index)).getAs[Seq[String]](3).toArray,
                  Array.concat(
                    op_dtls(convertToInt(index)).getAs[Seq[String]](4).toArray,
                    Array.fill(1)(in.getAs[String]("job_name"))
                  ),
                  op_dtls(convertToInt(index)).getAs[Seq[String]](5).toArray,
                  op_dtls(convertToInt(index)).getAs[Seq[String]](6).toArray,
                  op_dtls(convertToInt(index)).getAs[Seq[String]](7).toArray,
                  op_dtls(convertToInt(index)).getAs[String](8),
                  op_dtls(convertToInt(index)).getAs[String](9),
                  op_dtls(convertToInt(index)).getAs[String](10),
                  op_dtls(convertToInt(index)).getAs[String](11),
                  op_dtls(convertToInt(index)).getAs[String](12)
                )
            } else {
              idx = non_qual_output_profile_ids.indexWhere(xx =>
                xx.getAs[String](0) == in.getAs[String]("output_profile_id")
              )
              if (idx == -1) {
                qual_output_profile_ids = Array.concat(
                  qual_output_profile_ids,
                  Array.fill(1)(in.getAs[String]("output_profile_id"))
                )
                op_dtls = Array.concat(
                  op_dtls,
                  Array.fill(1)(
                    Row(
                      in.getAs[String]("output_profile_name"),
                      in.getAs[String]("rxclaim_env_name"),
                      Array(in.getAs[String]("job_id")),
                      if (
                        (try Array(in.getAs[String]("alias_name"))
                        catch {
                          case error: Throwable => null
                        }) == null
                      )
                        Array[Any]()
                      else
                        Array(in.getAs[String]("alias_name")),
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
                index = (op_dtls.length - 1)
              } else {
                len = non_qual_output_profile_ids.length - 1
                if (
                  !non_qual_output_profile_ids(convertToInt(len))
                    .getAs[Seq[String]]("job_ids")
                    .toArray
                    .contains(in.getAs[String]("job_id"))
                )
                  non_qual_output_profile_ids(len) = Row(
                    (non_qual_output_profile_ids(convertToInt(len))
                      .getAs[String](0)),
                    non_qual_output_profile_ids(convertToInt(len)).getAs[String](1),
                    Array.concat(
                      non_qual_output_profile_ids(convertToInt(len))
                        .getAs[Seq[String]](2)
                        .toArray,
                      Array.fill(1)(in.getAs[String]("job_id"))
                    ),
                    non_qual_output_profile_ids(convertToInt(len))
                      .getAs[Seq[String]](3)
                      .toArray,
                    non_qual_output_profile_ids(convertToInt(len))
                      .getAs[Seq[String]](4)
                      .toArray,
                    non_qual_output_profile_ids(convertToInt(len))
                      .getAs[Seq[String]](5)
                      .toArray,
                    non_qual_output_profile_ids(convertToInt(len))
                      .getAs[Seq[String]](6)
                      .toArray
                  )
                if (
                  !non_qual_output_profile_ids(convertToInt(len))
                    .getAs[Seq[String]](5)
                    .toArray
                    .contains(
                      if (
                        (try in.getAs[String]("alias_name")
                        catch {
                          case error: Throwable => null
                        }) == null
                      )
                        ""
                      else
                        in.getAs[String]("alias_name")
                    )
                )
                  non_qual_output_profile_ids(len) = Row(
                    (non_qual_output_profile_ids(convertToInt(len))
                      .getAs[String](0)),
                    non_qual_output_profile_ids(convertToInt(len)).getAs[String](1),
                    non_qual_output_profile_ids(convertToInt(len))
                      .getAs[Seq[String]](2)
                      .toArray,
                    non_qual_output_profile_ids(convertToInt(len))
                      .getAs[Seq[String]](3)
                      .toArray,
                    non_qual_output_profile_ids(convertToInt(len))
                      .getAs[Seq[String]](4)
                      .toArray,
                    Array.concat(
                      non_qual_output_profile_ids(convertToInt(len))
                        .getAs[Seq[String]](5)
                        .toArray,
                      Array.fill(1)(in.getAs[String]("alias_name"))
                    ),
                    non_qual_output_profile_ids(convertToInt(len))
                      .getAs[Seq[String]](6)
                      .toArray
                  )
                if (
                  !non_qual_output_profile_ids(convertToInt(len))
                    .getAs[Seq[String]]("formulary_names")
                    .toArray
                    .contains(in.getAs[String]("formulary_name"))
                )
                  non_qual_output_profile_ids(len) = Row(
                    (non_qual_output_profile_ids(convertToInt(len))
                      .getAs[String](0)),
                    non_qual_output_profile_ids(convertToInt(len)).getAs[String](1),
                    non_qual_output_profile_ids(convertToInt(len))
                      .getAs[Seq[String]](2)
                      .toArray,
                    Array.concat(
                      non_qual_output_profile_ids(convertToInt(len))
                        .getAs[Seq[String]](3)
                        .toArray,
                      Array.fill(1)(in.getAs[String]("formulary_name"))
                    ),
                    Array.concat(
                      non_qual_output_profile_ids(convertToInt(len))
                        .getAs[Seq[String]](4)
                        .toArray,
                      Array.fill(1)(formulary_pseudonym)
                    ),
                    non_qual_output_profile_ids(convertToInt(len))
                      .getAs[Seq[String]](5)
                      .toArray,
                    non_qual_output_profile_ids(convertToInt(len))
                      .getAs[Seq[String]](6)
                      .toArray
                  )
                if (
                  !non_qual_output_profile_ids(convertToInt(len))
                    .getAs[Seq[String]]("job_names")
                    .toArray
                    .contains(in.getAs[String]("job_name"))
                )
                  non_qual_output_profile_ids(len) = Row(
                    (non_qual_output_profile_ids(convertToInt(len))
                      .getAs[String](0)),
                    non_qual_output_profile_ids(convertToInt(len)).getAs[String](1),
                    non_qual_output_profile_ids(convertToInt(len))
                      .getAs[Seq[String]](2)
                      .toArray,
                    non_qual_output_profile_ids(convertToInt(len))
                      .getAs[Seq[String]](3)
                      .toArray,
                    non_qual_output_profile_ids(convertToInt(len))
                      .getAs[Seq[String]](4)
                      .toArray,
                    non_qual_output_profile_ids(convertToInt(len))
                      .getAs[Seq[String]](5)
                      .toArray,
                    Array.concat(
                      non_qual_output_profile_ids(convertToInt(len))
                        .getAs[Seq[String]](6)
                        .toArray,
                      Array.fill(1)(in.getAs[String]("job_name"))
                    )
                  )
              }
            }
          } else if (!err_msgs.contains(error_msg)) {
            if (
              qual_output_profile_ids
                .contains(in.getAs[String]("output_profile_id"))
            ) {
              idx = qual_output_profile_ids.indexWhere(xx =>
                xx == in.getAs[String]("output_profile_id")
              )
              qual_output_profile_ids = (1 until convertToInt((idx - 1) + 1))
                .map(xslice => element_at(qual_output_profile_ids, xslice))
                .toArray
                .filter(yslice => !_isnull(yslice))
                .toArray
              op_dtls = (1 until convertToInt((idx - 1) + 1))
                .map(xslice => element_at(op_dtls, xslice))
                .toArray
                .filter(yslice => !_isnull(yslice))
                .toArray
              non_qual_output_profile_ids = Array.concat(
                non_qual_output_profile_ids,
                Array.fill(1)(
                  Row(
                    in.getAs[String]("output_profile_id"),
                    in.getAs[String]("rxclaim_env_name"),
                    Array(in.getAs[String]("job_id")),
                    Array(in.getAs[String]("formulary_name")),
                    Array(formulary_pseudonym),
                    Array(in.getAs[String]("alias_name")),
                    Array(in.getAs[String]("job_name"))
                  )
                )
              )
              err_msgs = Array.concat(err_msgs, Array.fill(1)(error_msg))
            } else {
              idx = non_qual_output_profile_ids.indexWhere(xx =>
                (xx.getAs[String](0)) == in.getAs[String]("output_profile_id")
              )
              if (idx != -1) {
                len = err_msgs.length - 1
                err_msgs(len) =
                  Array(err_msgs(convertToInt(len)), ".", error_msg).mkString("")
                if (
                  !non_qual_output_profile_ids(convertToInt(idx))
                    .getAs[Seq[String]](2)
                    .toArray
                    .contains(in.getAs[String]("job_id"))
                )
                  non_qual_output_profile_ids(idx) = Row(
                    (non_qual_output_profile_ids(convertToInt(idx))
                      .getAs[String](0)),
                    non_qual_output_profile_ids(convertToInt(idx)).getAs[String](1),
                    Array.concat(
                      non_qual_output_profile_ids(convertToInt(idx))
                        .getAs[Seq[String]](2)
                        .toArray,
                      Array.fill(1)(in.getAs[String]("job_id"))
                    ),
                    non_qual_output_profile_ids(convertToInt(idx))
                      .getAs[Seq[String]](3)
                      .toArray,
                    non_qual_output_profile_ids(convertToInt(idx))
                      .getAs[Seq[String]](4)
                      .toArray,
                    non_qual_output_profile_ids(convertToInt(idx))
                      .getAs[Seq[String]](5)
                      .toArray,
                    non_qual_output_profile_ids(convertToInt(idx))
                      .getAs[Seq[String]](6)
                      .toArray
                  )
                if (
                  !non_qual_output_profile_ids(convertToInt(idx))
                    .getAs[Seq[String]](5)
                    .toArray
                    .contains(
                      if (
                        (try in.getAs[String]("alias_name")
                        catch {
                          case error: Throwable => null
                        }) == null
                      )
                        ""
                      else
                        in.getAs[String]("alias_name")
                    )
                )
                  non_qual_output_profile_ids(idx) = Row(
                    (non_qual_output_profile_ids(convertToInt(idx))
                      .getAs[String](0)),
                    non_qual_output_profile_ids(convertToInt(idx)).getAs[String](1),
                    non_qual_output_profile_ids(convertToInt(idx))
                      .getAs[Seq[String]](2)
                      .toArray,
                    non_qual_output_profile_ids(convertToInt(idx))
                      .getAs[Seq[String]](3)
                      .toArray,
                    non_qual_output_profile_ids(convertToInt(idx))
                      .getAs[Seq[String]](4)
                      .toArray,
                    Array.concat(
                      non_qual_output_profile_ids(convertToInt(idx))
                        .getAs[Seq[String]](5)
                        .toArray,
                      Array.fill(1)(in.getAs[String]("alias_name"))
                    ),
                    non_qual_output_profile_ids(convertToInt(idx))
                      .getAs[Seq[String]](6)
                      .toArray
                  )
                if (
                  !non_qual_output_profile_ids(convertToInt(idx))
                    .getAs[Seq[String]]("formulary_names")
                    .toArray
                    .contains(in.getAs[String]("formulary_name"))
                )
                  non_qual_output_profile_ids(idx) = Row(
                    (non_qual_output_profile_ids(convertToInt(idx))
                      .getAs[String](0)),
                    non_qual_output_profile_ids(convertToInt(idx)).getAs[String](1),
                    non_qual_output_profile_ids(convertToInt(idx))
                      .getAs[Seq[String]](2)
                      .toArray,
                    Array.concat(
                      non_qual_output_profile_ids(convertToInt(idx))
                        .getAs[Seq[String]](3)
                        .toArray,
                      Array.fill(1)(in.getAs[String]("formulary_name"))
                    ),
                    Array.concat(
                      non_qual_output_profile_ids(convertToInt(idx))
                        .getAs[Seq[String]](4)
                        .toArray,
                      Array.fill(1)(formulary_pseudonym)
                    ),
                    non_qual_output_profile_ids(convertToInt(idx))
                      .getAs[Seq[String]](5)
                      .toArray,
                    non_qual_output_profile_ids(convertToInt(idx))
                      .getAs[Seq[String]](6)
                      .toArray
                  )
                if (
                  !non_qual_output_profile_ids(convertToInt(idx))
                    .getAs[Seq[String]]("job_names")
                    .toArray
                    .contains(in.getAs[String]("job_name"))
                )
                  non_qual_output_profile_ids(idx) = Row(
                    (non_qual_output_profile_ids(convertToInt(idx))
                      .getAs[String](0)),
                    non_qual_output_profile_ids(convertToInt(idx)).getAs[String](1),
                    non_qual_output_profile_ids(convertToInt(idx))
                      .getAs[Seq[String]](2)
                      .toArray,
                    non_qual_output_profile_ids(convertToInt(idx))
                      .getAs[Seq[String]](3)
                      .toArray,
                    non_qual_output_profile_ids(convertToInt(idx))
                      .getAs[Seq[String]](4)
                      .toArray,
                    non_qual_output_profile_ids(convertToInt(idx))
                      .getAs[Seq[String]](5)
                      .toArray,
                    Array.concat(
                      non_qual_output_profile_ids(convertToInt(idx))
                        .getAs[Seq[String]](3)
                        .toArray,
                      Array.fill(1)(in.getAs[String]("job_name"))
                    )
                  )
              } else {
                non_qual_output_profile_ids = Array.concat(
                  non_qual_output_profile_ids,
                  Array.fill(1)(
                    Row(
                      in.getAs[String]("output_profile_id"),
                      in.getAs[String]("rxclaim_env_name"),
                      Array(in.getAs[String]("job_id")),
                      Array(in.getAs[String]("formulary_name")),
                      Array(formulary_pseudonym),
                      Array(
                        if (
                          (try in.getAs[String]("alias_name")
                          catch {
                            case error: Throwable => null
                          }) == null
                        )
                          ""
                        else
                          in.getAs[String]("alias_name")
                      ),
                      Array(in.getAs[String]("job_name"))
                    )
                  )
                )
                err_msgs = Array.concat(err_msgs, Array.fill(1)(error_msg))
              }
            }
          } else {
            len = non_qual_output_profile_ids.length - 1
            if (
              !non_qual_output_profile_ids(convertToInt(len))
                .getAs[Seq[String]]("job_ids")
                .toArray
                .contains(in.getAs[String]("job_id"))
            )
              non_qual_output_profile_ids(len) = Row(
                (non_qual_output_profile_ids(convertToInt(len)).getAs[String](0)),
                non_qual_output_profile_ids(convertToInt(len)).getAs[String](1),
                Array.concat(
                  non_qual_output_profile_ids(convertToInt(len))
                    .getAs[Seq[String]](2)
                    .toArray,
                  Array.fill(1)(in.getAs[String]("job_id"))
                ),
                non_qual_output_profile_ids(convertToInt(len))
                  .getAs[Seq[String]](3)
                  .toArray,
                non_qual_output_profile_ids(convertToInt(len))
                  .getAs[Seq[String]](4)
                  .toArray,
                non_qual_output_profile_ids(convertToInt(len))
                  .getAs[Seq[String]](5)
                  .toArray,
                non_qual_output_profile_ids(convertToInt(len))
                  .getAs[Seq[String]](6)
                  .toArray
              )
            if (
              !non_qual_output_profile_ids(convertToInt(len))
                .getAs[String]("alias_names")
                .contains(
                  if (
                    (try in.getAs[String]("alias_name")
                    catch {
                      case error: Throwable => null
                    }) == null
                  )
                    ""
                  else
                    in.getAs[String]("alias_name")
                )
            )
              non_qual_output_profile_ids(len) = Row(
                (non_qual_output_profile_ids(convertToInt(len)).getAs[String](0)),
                non_qual_output_profile_ids(convertToInt(len)).getAs[String](1),
                non_qual_output_profile_ids(convertToInt(len))
                  .getAs[Seq[String]](2)
                  .toArray,
                non_qual_output_profile_ids(convertToInt(len))
                  .getAs[Seq[String]](3)
                  .toArray,
                non_qual_output_profile_ids(convertToInt(len))
                  .getAs[Seq[String]](4)
                  .toArray,
                Array.concat(
                  non_qual_output_profile_ids(convertToInt(len))
                    .getAs[Seq[String]](5)
                    .toArray,
                  Array.fill(1)(in.getAs[String]("alias_name"))
                ),
                non_qual_output_profile_ids(convertToInt(len))
                  .getAs[Seq[String]](6)
                  .toArray
              )
            if (
              !non_qual_output_profile_ids(convertToInt(len))
                .getAs[Seq[String]](3)
                .toArray
                .contains(in.getAs[String]("formulary_name"))
            )
              non_qual_output_profile_ids(len) = Row(
                (non_qual_output_profile_ids(convertToInt(len)).getAs[String](0)),
                non_qual_output_profile_ids(convertToInt(len)).getAs[String](1),
                non_qual_output_profile_ids(convertToInt(len))
                  .getAs[Seq[String]](2)
                  .toArray,
                Array.concat(
                  non_qual_output_profile_ids(convertToInt(len))
                    .getAs[Seq[String]](3)
                    .toArray,
                  Array.fill(1)(in.getAs[String]("formulary_name"))
                ),
                Array.concat(
                  non_qual_output_profile_ids(convertToInt(len))
                    .getAs[Seq[String]](4)
                    .toArray,
                  Array.fill(1)(formulary_pseudonym)
                ),
                non_qual_output_profile_ids(convertToInt(len))
                  .getAs[Seq[String]](5)
                  .toArray,
                non_qual_output_profile_ids(convertToInt(len))
                  .getAs[Seq[String]](6)
                  .toArray
              )
            if (
              !non_qual_output_profile_ids(convertToInt(len))
                .getAs[Seq[String]]("job_names")
                .toArray
                .contains(in.getAs[String]("job_name"))
            )
              non_qual_output_profile_ids(len) = Row(
                (non_qual_output_profile_ids(convertToInt(len)).getAs[String](0)),
                non_qual_output_profile_ids(convertToInt(len)).getAs[String](1),
                non_qual_output_profile_ids(convertToInt(len))
                  .getAs[Seq[String]](2)
                  .toArray,
                non_qual_output_profile_ids(convertToInt(len))
                  .getAs[Seq[String]](3)
                  .toArray,
                non_qual_output_profile_ids(convertToInt(len))
                  .getAs[Seq[String]](4)
                  .toArray,
                non_qual_output_profile_ids(convertToInt(len))
                  .getAs[Seq[String]](5)
                  .toArray,
                Array.concat(
                  non_qual_output_profile_ids(convertToInt(len))
                    .getAs[Seq[String]](6)
                    .toArray,
                  Array.fill(1)(in.getAs[String]("job_name"))
                )
              )
          }
        }
    
        Row(
          carrier,
          account,
          group,
          future_flg,
          as_of_dt,
          cag_override_data_path,
          convertToStringList(qual_output_profile_ids),
          op_dtls.map { x =>
            Row(
              (x.get(0)).toString,
              (x.get(1)).toString,
              convertToStringList(x.get(2)),
              x.get(3),
              x.get(4),
              x.get(5),
              x.get(6),
              x.get(7),
              (x.get(8)).toString,
              (x.get(9)).toString,
              (x.get(10)).toString,
              (x.get(11)).toString,
              (x.get(12)).toString
            )
          }.toArray,
          non_qual_output_profile_ids.map { x =>
            Row(
              (x.get(0)).toString,
              (x.get(1)).toString,
              convertToStringList(x.get(2)),
              x.get(3),
              x.get(4),
              x.get(5),
              x.get(6)
            )
          }.toArray,
          err_msgs,
          newline
        )
      },
      StructType(
        List(
          StructField("carrier", StringType, false),
          StructField("account", StringType, false),
          StructField("group", StringType, false),
          StructField("future_flg", StringType, false),
          StructField("as_of_dt", StringType, false),
          StructField("cag_override_data_path", StringType, false),
          StructField("qual_output_profile_ids", ArrayType(StringType), false),
          StructField(
            "op_dtls",
            ArrayType(
              StructType(
                List(
                  StructField("qual_output_profile_name", StringType, false), // 0
                  StructField("rxclaim_env_name", StringType, false), // 1
                  StructField("job_ids", ArrayType(StringType), false), // 1
                  StructField("alias_names", ArrayType(StringType), false), // 2
                  StructField("job_names", ArrayType(StringType), false), // 3
                  StructField("formulary_names", ArrayType(StringType), false), // 4
                  StructField(
                    "form_override_data_paths",
                    ArrayType(StringType),
                    false
                  ), // 5
                  StructField(
                    "formulary_pseudonyms",
                    ArrayType(StringType),
                    false
                  ), // 6
                  StructField("tal_name", StringType, false), // 7
                  StructField("tac_name", StringType, false), // 8
                  StructField("tar_name", StringType, false), // 9
                  StructField("tsd_name", StringType, false), // 10
                  StructField("st_tac_ind", StringType, false) // 11
                )
              )
            ),
            false
          ),
          StructField(
            "non_qual_output_profile_ids",
            ArrayType(
              StructType(
                List(
                  StructField("non_qual_op_id", StringType, false), // 0
                  StructField("rxclaim_env_name", StringType, false), // 1
                  StructField("job_ids", ArrayType(StringType), false), // 2
                  StructField("formulary_names", ArrayType(StringType), false), // 3
                  StructField(
                    "formulary_pseudonyms",
                    ArrayType(StringType),
                    false
                  ), // 4
                  StructField("alias_names", ArrayType(StringType), false), // 5
                  StructField("job_names", ArrayType(StringType), false) // 6
                )
              )
            ),
            false
          ),
          StructField("err_msgs", ArrayType(StringType), false),
          StructField("newline", StringType, false)
        )
      )
    )
    
    val origColumns = in0.columns.map(col)
    val out0 = in0
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
      .select((process_udf(col("inputRows"), col("cag_url"))).alias("output"))
      .select(col("output.*"))
    out0
  }

}
