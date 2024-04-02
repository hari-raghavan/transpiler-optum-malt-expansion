package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(TAD_XWALK: String = "", TAD_EXTRACT: String = "")
    extends ConfigBase
