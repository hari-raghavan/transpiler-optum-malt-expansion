package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  REBATE_ELIG_EXTRACT:   String = "",
  REBATE_ELIGIBLE_XWALK: String = "",
  OUTPUT_PROF_ID:        String = "14288"
) extends ConfigBase
