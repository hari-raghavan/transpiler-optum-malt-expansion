package io.prophecy.pipelines.altexp_tac_rules_parser_2.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(EXTRACT_FILE: String = "", TAC_RULE_XWALK: String = "")
    extends ConfigBase
