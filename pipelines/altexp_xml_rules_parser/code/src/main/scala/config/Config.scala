package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  EXTRACT_FILE:              String = "",
  RULE_XWALK:                String = "",
  RULE_XWALK_WT_RL_PRIORITY: String = "",
  UDL_MSTR_XWALK:            String = ""
) extends ConfigBase
