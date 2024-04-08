package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  UDL_CAG_EXP_FILE:       String = "",
  CAG_TAL_CONTAINER_FILE: String = "",
  UDL_BASELINE_EXP_FILE:  String = "",
  CARRIER:                String = "",
  RULE_PRODUCTS_LKP:      String = "",
  TAL_ASSOC_DTL:          String = "",
  BUSINESS_DATE:          String = "20240404",
  TAL_DTL:                String = "",
  SHARED_QUAL_FILE:       String = "",
  TAL_NAME:               String = "ORX_STANDARD"
) extends ConfigBase
