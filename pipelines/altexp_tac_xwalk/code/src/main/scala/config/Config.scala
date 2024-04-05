package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  UDL_CAG_EXP_FILE:          String = "",
  TAC_RULE_XWALK:            String = "",
  CARRIER:                   String = "",
  TSD_EXP_XWALK:             String = "",
  BUSINESS_DATE:             String = "20240401",
  TAC_EXP_XWALK:             String = "",
  UDL_BASE_EXP_FILE:         String = "",
  FORMULARY_RULE_PRDCT_FILE: String = ""
) extends ConfigBase
