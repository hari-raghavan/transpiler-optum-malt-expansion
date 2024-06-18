package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  FLAG:        String = "",
  OUTPUT_FILE: String = "",
  TEMP:        String = "",
  JAVA_HOME:   String = ""
) extends ConfigBase
