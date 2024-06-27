package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  TAL_OUTPUT:    String = "",
  PRODUCTS_FILE: String = "",
  JAVA_HOME:     String = "",
  FLAG:          String = "",
  INPUT_FILE:    String = ""
) extends ConfigBase
