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

object Apply_alias_rules {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    val temp54262_UDF = udf(
        (_gpi14: String, _label_nm: String, _alias_info_vec_idx: Seq[Row], _ndc11: String) => {
          var gpi14              = _gpi14
          var label_nm           = _label_nm
          var alias_info_vec_idx = _alias_info_vec_idx.toArray
          var k                  = 0
          var ndc11              = _ndc11
    
          while (compareTo(k, alias_info_vec_idx.size) < 0) {
            var alias = alias_info_vec_idx(convertToInt(k))
            alias
              .getAs[Row]("alias_info")
              .zipWithIndex
              .map {
                case (_rule, ruleIndex) =>
                  var rule = _rule
                  if (_string_lrtrim(label_nm) == _string_lrtrim((rule.getAs[String]("search_txt")))) {
                    if (rule.getAs[String]("qual_priority").toInt == 1) {
                      if (rule.getAs[String]("qual_id_value") == ndc11)
                        label_nm = rule.getAs[String]("replace_txt")
                    } else if (rule.getAs[String]("qual_priority").toInt == 2) {
                      if (rule.getAs[String]("qual_id_value") == substring_scala(ndc11, 1, 9))
                        label_nm = rule.getAs[String]("replace_txt")
                    } else if (rule.getAs[String]("qual_priority").toInt == 3) {
                      if (rule.getAs[String]("qual_id_value") == gpi14)
                        label_nm = rule.getAs[String]("replace_txt")
                    } else if (rule.getAs[String]("qual_priority").toInt == 4) {
                      if (rule.getAs[String]("qual_id_value") == substring_scala(gpi14, 1, 12))
                        label_nm = rule.getAs[String]("replace_txt")
                    } else if (rule.getAs[String]("qual_priority").toInt == 5) {
                      if (rule.getAs[String]("qual_id_value") == substring_scala(gpi14, 1, 10))
                        label_nm = rule.getAs[String]("replace_txt")
                    } else if (rule.getAs[String]("qual_priority").toInt == 6) {
                      if (rule.getAs[String]("qual_id_value") == substring_scala(gpi14, 1, 8))
                        label_nm = rule.getAs[String]("replace_txt")
                    } else if (rule.getAs[String]("qual_priority").toInt == 7) {
                      if (rule.getAs[String]("qual_id_value") == substring_scala(gpi14, 1, 6))
                        label_nm = rule.getAs[String]("replace_txt")
                    } else if (rule.getAs[String]("qual_priority").toInt == 8) {
                      if (rule.getAs[String]("qual_id_value") == substring_scala(gpi14, 1, 4))
                        label_nm = rule.getAs[String]("replace_txt")
                    } else if (rule.getAs[String]("qual_priority").toInt == 99)
                      label_nm = rule.getAs[String]("replace_txt")
                  }
              }
              .toArray
            k = k + 1
          }
    
          label_nm
        },
        StringType
      )
      
      
      
      val normalize_out_DF = in.normalize(
        lengthExpression = Some(size(typedLit(Config.ALIAS_NAME_VEC))),
        finishedExpression = None,
        finishedCondition = None,
        alias = "index",
        colsToSelect = List(
          (concat(lit("ALIAS_SET"), (col("index") + lit(1)).cast(StringType))).as("alias_set_name"),
          (temp54262_UDF(
            col("gpi14"),
            string_substring(col("prod_short_desc"), lit(1), lit(12)),
            element_at(
              filter(
                transform(
                  typedLit(Config.ALIAS_NAME_VEC),
                  alias_nm_vec =>
                    filter(
                      transform(
                        alias_nm_vec,
                        alias_nm =>
                          struct(
                            coalesce(lookup("Alias_Xwalk",         alias_nm),
                                     struct(lit(0).as("alias_id"), lit("").as("alias_name"), array().as("alias_info"))
                            ).getField("alias_id").as("alias_id"),
                            coalesce(lookup("Alias_Xwalk",         alias_nm),
                                     struct(lit(0).as("alias_id"), lit("").as("alias_name"), array().as("alias_info"))
                            ).getField("alias_name").as("alias_name"),
                            filter(
                              transform(
                                coalesce(lookup("Alias_Xwalk",         alias_nm),
                                         struct(lit(0).as("alias_id"), lit("").as("alias_name"), array().as("alias_info"))
                                ).getField("alias_info"),
                                alias_rule =>
                                  when((to_date(lit(Config.BUSINESS_DATE), "yyyyMMdd") >= to_date(alias_rule.getField("eff_dt"), "yyyyMMdd"))
                                         .and(to_date(lit(Config.BUSINESS_DATE), "yyyyMMdd") <= to_date(alias_rule.getField("term_dt"), "yyyyMMdd")),
                                       alias_rule
                                  )
                              ),
                              xx => !isnull(xx)
                            ).as("alias_info")
                          )
                      ),
                      xx => !isnull(xx)
                    )
                ),
                xx => !isnull(xx)
              ),
              col("index") + lit(1)
            ),
            col("ndc11")
          )).as("alias_label_nm"),
          col("ndc11").as("ndc11"),
          (col("newline").cast(StringType)).as("newline")
        ),
        lengthRelatedGlobalExpressions = Map(),
        tempWindowExpr = Map()
      )
    
      val simpleSelect_in_DF = normalize_out_DF.select(
        (col("alias_set_name")).as("alias_set_name"),
        (col("ndc11")).as("ndc11"),
        (col("alias_label_nm")).as("alias_label_nm"),
        (col("newline").cast(StringType)).as("newline")
      )
    
      val out = simpleSelect_in_DF
    out
  }

}
