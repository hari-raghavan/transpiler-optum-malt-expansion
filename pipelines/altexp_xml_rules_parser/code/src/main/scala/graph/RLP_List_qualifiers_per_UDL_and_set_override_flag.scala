package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object RLP_List_qualifiers_per_UDL_and_set_override_flag {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("udl_id"))
      .agg(
        last(col("udl_desc")).as("user_defined_list_desc"),
        array_distinct(
          array_sort(
            flatten(
              collect_list(
                transform(col("rule_def"),
                          rule => rule.getField("qualifier_cd")
                )
              )
            )
          )
        ).as("qual_list"),
        when(
          size(
            array_intersect(
              array_distinct(
                array_sort(
                  flatten(
                    collect_list(
                      transform(col("rule_def"),
                                rule => rule.getField("qualifier_cd")
                      )
                    )
                  )
                )
              ),
              array(lit("DESI_CD"),
                    lit("DOSAGE_FORM"),
                    lit("MSC"),
                    lit("ROA"),
                    lit("RXOTC")
              )
            )
          ) > lit(0),
          lit(1)
        ).otherwise(lit(0)).as("override_flg"),
        last(col("udl_nm")).as("udl_nm"),
        last(col("eff_dt")).as("eff_dt"),
        last(col("term_dt")).as("term_dt"),
        last(col("newline")).as("newline")
      )

}
