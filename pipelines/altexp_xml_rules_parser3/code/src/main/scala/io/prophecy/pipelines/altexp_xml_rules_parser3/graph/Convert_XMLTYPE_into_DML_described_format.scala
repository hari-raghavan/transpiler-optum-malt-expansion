package io.prophecy.pipelines.altexp_xml_rules_parser3.graph

import io.prophecy.libs._
import io.prophecy.pipelines.altexp_xml_rules_parser3.config.Context
import io.prophecy.pipelines.altexp_xml_rules_parser3.udfs.UDFs._
import io.prophecy.pipelines.altexp_xml_rules_parser3.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Convert_XMLTYPE_into_DML_described_format {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    val out = in.withColumn(
      "xml",
      xml_split(col("rule"),
            """
type Qual_t = utf8 record
  string('\0') type0;
  string('\0') op;
  string('\0') Qual;
  string('\0',charset='x-ab-internal') XML_fields() = 'type0=@type,op=@op,Qual=text()';
end;

type Rule_t0 = utf8 record
  Qual_t[big endian integer(4)] Qual;
  string('\0')[big endian integer(4)] OR;
  string('\0',charset='x-ab-internal') XML_fields() = 'Qual=%e/Qual{1-n},OR=%e/OR{1-n}/text()';
end;

type Qual_t0 = utf8 record
  string('\0') type0;
  string('\0') op;
  string('\0') Qual;
  string('\0',charset='x-ab-internal') XML_fields() = 'type0=@type,op=@op,Qual=text()';
end;

type Rule_t = utf8 record
  Rule_t0[big endian integer(4)] Rule;
  string('\0')[big endian integer(4)] AND;
  Qual_t0[big endian integer(4)] Qual;
  string('\0',charset='x-ab-internal') XML_fields() = 'Rule=%e/Rule{1-n},AND=%e/AND{1-n}/text(),Qual=%e/Qual';
end;

type xml_doc_t = utf8 record
  decimal('\0') seq_num;
  Rule_t Rule = NULL;
  string('\0',charset='x-ab-internal') XML_fields() = 'seq_num=seqnum(),Rule=Rule';
  string('\0',charset='x-ab-internal') XML_generated_by_version() = '3.2.6.4';
  string('\0',charset='x-ab-internal') XML_generated_at_compatibility() = '';
end;
""".stripMargin
      )
    )
    out
  }

}
