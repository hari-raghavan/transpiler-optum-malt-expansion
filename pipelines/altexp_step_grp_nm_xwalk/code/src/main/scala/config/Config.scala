package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  TAC_RULE_XWALK:       String = "/altexp_tac_rule_xwalk.BAT.RXCL3-CTR.dat",
  FORM_OVRRD_DATA_PATH: String = "",
  TAC_NM:               String = "ORX_STANDARD_STTST1",
  STEP_GRP_NM_XWALK:    String = "",
  STEP_GRP_NUM_XWALK:   String = ""
) extends ConfigBase
