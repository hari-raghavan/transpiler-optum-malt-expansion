package io.prophecy.pipelines.altexp_tar_tax_xml_rule_parser_2.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(EXTRACT_FILE: String = "", RULE_XWALK: String = "")
    extends ConfigBase
