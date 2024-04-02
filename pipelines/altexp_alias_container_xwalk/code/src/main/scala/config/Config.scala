package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  ALIAS_NAME_VEC:       List[String] = List(" "),
  PRODUCTS_FILE:        String = "/altexp_prdcts.RXCL3.NULL_BASE_LINE.dat",
  BUSINESS_DATE:        String = "20240327",
  ALIAS_XWALK_LKP_FILE: String = "",
  ALIAS_FILE_NAMES:     String = "/altexp_alias_products.RXCL3.NULL_BASE_LINE.",
  ALIAS_XWALK_FILE:     String = " "
) extends ConfigBase
