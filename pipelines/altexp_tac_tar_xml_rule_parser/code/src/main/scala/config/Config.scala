package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(EXTRACT_FILE: String = "", RULE_XWALK: String = "")
    extends ConfigBase
