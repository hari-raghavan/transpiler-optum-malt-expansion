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

object Convert_Target_XMLTYPE_into_DML_described_format {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    val out = in.withColumn(
      "target_xml",
      explode(
        xml_split(
          col("target_rule"),
          StructType(
            StructField("seq_num", StringType, true),
            StructField(
              "Rule",
              StructType(
                StructField(
                  "Rule",
                  ArrayType(
                    StructType(
                      StructField(
                        "Qual",
                        ArrayType(
                          StructType(
                            StructField("type0", StringType, true),
                            StructField("op", StringType, true),
                            StructField("Qual", StringType, true)
                          ),
                          true
                        ),
                        true
                      ),
                      StructField("OR", ArrayType(StringType, true), true)
                    ),
                    true
                  ),
                  true
                ),
                StructField("AND", ArrayType(StringType, true), true),
                StructField(
                  "Qual",
                  ArrayType(
                    StructType(
                      StructField("type0", StringType, true),
                      StructField("op", StringType, true),
                      StructField("Qual", StringType, true)
                    ),
                    true
                  ),
                  true
                )
              ),
              true
            )
          )
        )
      )
    )
    out
  }

}
