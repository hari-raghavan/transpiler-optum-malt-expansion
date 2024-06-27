package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  TAL_OUTPUT:    String = "",
  PRODUCTS_FILE: String = "",
  JAVA_HOME:     String = "",
  FLAG:          String = "",
  INPUT_FILE: String =
    "/altexp_tal_container.RXCL3-CTR.NULL_BASE_LINE.20240627.dat"
) extends ConfigBase
