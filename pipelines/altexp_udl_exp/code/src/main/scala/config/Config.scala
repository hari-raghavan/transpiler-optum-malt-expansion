package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  UDL_MSTR_XWALK:     String = "",
  CARRIER:            String = "NULL",
  RULE_PRODUCTS_LKP:  String = "",
  UDL_EXPANSION_FILE: String = "",
  BUSINESS_DATE:      String = "20240401",
  RULE_XWALK:         String = ""
) extends ConfigBase
