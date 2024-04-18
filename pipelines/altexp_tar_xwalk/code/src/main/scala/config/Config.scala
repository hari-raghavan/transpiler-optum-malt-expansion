package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  REBATE_ELIG_EXTRACT: String = "",
  TAR_RULE_XWALK:      String = "",
  TAR_ROA_DF_EXTRACT:  String = "",
  BUSINESS_DATE:       String = "20240401",
  TAR_EXP_XWALK:       String = " "
) extends ConfigBase
