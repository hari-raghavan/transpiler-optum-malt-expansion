package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(ALIAS_EXTRACT: String = " ", ALIAS_XWALK_FILE: String = " ")
    extends ConfigBase
