package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  TAC_RULE_XWALK:         String = "",
  CAG_TAL_CONTAINER_FILE: String = "",
  PRODUCT_FILE:           String = "",
  FORM_OVRRD_DATA_PATH:   String = "",
  TAC_NM:                 String = "TAC_NM",
  STEP_THERAPY_DTL_FILE:  String = "",
  TAC_CONTAINER_FILE:     String = "",
  TAC_CONDITION:          Int = 1
) extends ConfigBase
